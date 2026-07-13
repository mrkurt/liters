//! Manager integration tests over dir storage (HTTP-backed manager flows
//! are covered by the liters-ffi integration tests).
//!
//! Conventions (mirroring cancellation.rs): all DB load is bounded; every
//! wait is a deadline-bounded poll that fails the test instead of hanging
//! it; replica databases are only read after their worker has been joined
//! (shutdown/unregister), so in-place page application can never race the
//! row reads; Backoff durations are milliseconds so failure paths converge
//! fast. The Manager is always declared after the tempdir, so a panicking
//! test body drops (and joins) it before the directory disappears.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use liters::{
    Backoff, CancelToken, DbRole, DbState, DirReplicaClient, FollowConfig, FollowOptions,
    MaintenanceOptions, Manager, ManagerEvent, ManagerObserver, ManagerOptions, PushConfig,
    Replica, ReplicaClient, ReplicaOptions, StorageConfig, StorageError, Txid, WriterOptions,
};
use ltx::FileInfo;
use rusqlite::Connection;

const POLL: Duration = Duration::from_millis(10);

// ---------------------------------------------------------------------------
// Helpers

fn create_db(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(Duration::from_secs(5)).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn
}

/// Inserts a batch as ONE transaction: a batch is a single WAL commit, so a
/// concurrently running interval push can never split it across two L0
/// files — which would make every "converged at txid N" assertion
/// nondeterministic (txid N would hold only a prefix of the batch).
fn insert_rows(conn: &Connection, tag: &str, n: usize) {
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    for j in 0..n {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("{tag}-{j}")]).unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
}

fn rows_of(path: &Path) -> Vec<(i64, String)> {
    let conn =
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok");
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

/// Polls `cond` until it holds or `deadline` passes; returns the final
/// verdict so callers can shut down cleanly before asserting.
fn wait_for(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if cond() {
            return true;
        }
        std::thread::sleep(POLL);
    }
    cond()
}

/// Millisecond-scale backoff so transient-failure paths converge fast.
fn fast_backoff() -> Backoff {
    Backoff {
        initial: Duration::from_millis(10),
        max: Duration::from_millis(200),
        multiplier: 2.0,
        jitter: 0.0,
    }
}

fn manager() -> Manager {
    Manager::new(ManagerOptions { backoff: fast_backoff(), default_push_interval: None })
}

fn dir_push_cfg(bucket: &Path, interval: Option<Duration>) -> PushConfig {
    PushConfig {
        storage: StorageConfig::Dir { path: bucket.to_path_buf() },
        writer_options: WriterOptions::default(),
        push_interval: interval,
        maintenance: None,
        backoff: None,
    }
}

fn dir_follow_cfg(bucket: &Path) -> FollowConfig {
    FollowConfig {
        storage: StorageConfig::Dir { path: bucket.to_path_buf() },
        replica_options: ReplicaOptions::default(),
        follow_options: FollowOptions { poll_interval: Duration::from_millis(25), retry: None },
    }
}

/// Restores the bucket into `scratch` and asserts it matches `db` row-wise.
fn assert_bucket_matches(bucket: &Path, db: &Path, scratch: &Path) {
    let mut rep = Replica::open(
        scratch,
        Box::new(DirReplicaClient::new(bucket)),
        ReplicaOptions::default(),
    );
    rep.sync().unwrap();
    assert_eq!(rows_of(scratch), rows_of(db));
}

/// `PRAGMA wal_checkpoint(TRUNCATE)` from a fresh connection; returns the
/// busy flag (0 = checkpoint fully ran and the WAL was truncated, 1 = a
/// reader blocked it). A live Writer pins the WAL with its long-running
/// read transaction, so busy==0 is the observable for "the Writer was
/// dropped and its locks/fds released".
fn truncate_checkpoint_busy(db: &Path) -> i64 {
    let conn = Connection::open(db).unwrap();
    conn.busy_timeout(Duration::from_millis(100)).unwrap();
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get::<_, i64>(0)).unwrap()
}

// ---------------------------------------------------------------------------
// Instrumented ReplicaClient: counts every storage call (traffic meter),
// can block writes until the installed cancel token flips (stand-in for a
// stalled transfer on a token-aware backend), and can fail writes fatally a
// configured number of times. Shared knobs live in `ClientState` so a fresh
// client per StorageConfig::build() still reports to the same test.

#[derive(Clone, Default)]
struct ClientState {
    ops: Arc<AtomicU64>,
    block_writes: Arc<AtomicBool>,
    /// Remaining write_ltx_file calls to fail with a non-transient error.
    fail_fatal_writes: Arc<AtomicU64>,
    /// Remaining write_ltx_file calls to fail with a transient error.
    fail_transient_writes: Arc<AtomicU64>,
    /// While set, write_ltx_file at levels >= 1 fails with a non-transient
    /// error (level-0 pushes keep working) — a maintenance-only fault.
    fail_fatal_high_level_writes: Arc<AtomicBool>,
    /// Signalled when a write starts blocking.
    started: Arc<Mutex<Option<mpsc::Sender<()>>>>,
}

struct TestClient {
    inner: DirReplicaClient,
    st: ClientState,
    token: Mutex<CancelToken>,
}

impl TestClient {
    fn tick(&self) {
        self.st.ops.fetch_add(1, Ordering::SeqCst);
    }
}

impl ReplicaClient for TestClient {
    fn client_type(&self) -> &'static str {
        "test"
    }
    fn ltx_files(
        &self,
        level: u8,
        seek: Txid,
        use_metadata: bool,
    ) -> liters_storage::Result<Vec<FileInfo>> {
        self.tick();
        self.inner.ltx_files(level, seek, use_metadata)
    }
    fn open_ltx_file(
        &self,
        level: u8,
        min: Txid,
        max: Txid,
        offset: u64,
        size: u64,
    ) -> liters_storage::Result<Box<dyn std::io::Read + Send>> {
        self.tick();
        self.inner.open_ltx_file(level, min, max, offset, size)
    }
    fn write_ltx_file(
        &self,
        level: u8,
        min: Txid,
        max: Txid,
        rd: &mut dyn std::io::Read,
    ) -> liters_storage::Result<FileInfo> {
        self.tick();
        if level >= 1 && self.st.fail_fatal_high_level_writes.load(Ordering::SeqCst) {
            return Err(StorageError::Conflict("injected maintenance write failure".into()));
        }
        if self.st.fail_fatal_writes.load(Ordering::SeqCst) > 0 {
            self.st.fail_fatal_writes.fetch_sub(1, Ordering::SeqCst);
            return Err(StorageError::Conflict("injected fatal write failure".into()));
        }
        if self.st.fail_transient_writes.load(Ordering::SeqCst) > 0 {
            self.st.fail_transient_writes.fetch_sub(1, Ordering::SeqCst);
            return Err(StorageError::Unavailable("injected transient write failure".into()));
        }
        if self.st.block_writes.load(Ordering::SeqCst) {
            if let Some(tx) = self.st.started.lock().unwrap().as_ref() {
                let _ = tx.send(());
            }
            let token = self.token.lock().unwrap().clone();
            // Deadline-bounded so a broken cancellation path fails the test
            // instead of hanging it.
            let deadline = Instant::now() + Duration::from_secs(15);
            while !token.is_cancelled() {
                if Instant::now() >= deadline {
                    return Err(StorageError::Other("blocked write was never cancelled".into()));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            return Err(StorageError::Cancelled);
        }
        self.inner.write_ltx_file(level, min, max, rd)
    }
    fn delete_ltx_files(&self, infos: &[FileInfo]) -> liters_storage::Result<()> {
        self.tick();
        self.inner.delete_ltx_files(infos)
    }
    fn delete_all(&self) -> liters_storage::Result<()> {
        self.tick();
        self.inner.delete_all()
    }
    fn set_cancel(&self, token: CancelToken) {
        *self.token.lock().unwrap() = token;
    }
}

fn custom_storage(bucket: &Path, st: ClientState) -> StorageConfig {
    let bucket = bucket.to_path_buf();
    StorageConfig::Custom(Arc::new(move || {
        Ok(Box::new(TestClient {
            inner: DirReplicaClient::new(&bucket),
            st: st.clone(),
            token: Mutex::new(CancelToken::new()),
        }))
    }))
}

// ---------------------------------------------------------------------------
// Observer recording

#[derive(Default)]
struct RecordingObserver {
    events: Mutex<Vec<ManagerEvent>>,
}

impl ManagerObserver for RecordingObserver {
    fn on_event(&self, event: ManagerEvent) {
        self.events.lock().unwrap().push(event);
    }
}

impl RecordingObserver {
    fn snapshot(&self) -> Vec<ManagerEvent> {
        self.events.lock().unwrap().clone()
    }
}

/// Compact event fingerprint for order assertions.
fn kind(e: &ManagerEvent) -> String {
    match e {
        ManagerEvent::StateChanged { state, .. } => format!("state:{state:?}"),
        ManagerEvent::PushCompleted { .. } => "push".into(),
        ManagerEvent::SyncCompleted { .. } => "sync".into(),
        ManagerEvent::Error { .. } => "error".into(),
    }
}

// ---------------------------------------------------------------------------
// Tests

/// M1: two push registrations and one follower over dir storage move data
/// end to end — writer app commits rows, buckets fill, the follower
/// materializes them — and statuses report ids, roles, and positions.
#[test]
fn multi_db_end_to_end_with_statuses() {
    let tmp = tempfile::tempdir().unwrap();
    let db_a = tmp.path().join("a.db");
    let db_b = tmp.path().join("b.db");
    let replica_a = tmp.path().join("ra.db");
    let bucket_a = tmp.path().join("bucket-a");
    let bucket_b = tmp.path().join("bucket-b");
    let conn_a = create_db(&db_a);
    let conn_b = create_db(&db_b);
    insert_rows(&conn_a, "a0", 10);
    insert_rows(&conn_b, "b0", 10);

    let mgr = manager();
    let interval = Some(Duration::from_millis(25));
    mgr.register_push("a", &db_a, dir_push_cfg(&bucket_a, interval)).unwrap();
    mgr.register_push("b", &db_b, dir_push_cfg(&bucket_b, interval)).unwrap();
    mgr.register_follow("fa", &replica_a, dir_follow_cfg(&bucket_a)).unwrap();

    // First pushes run immediately; the follower restores from bucket A.
    let converged = wait_for(Duration::from_secs(30), || {
        mgr.status("a").is_some_and(|s| s.position == Some(Txid(1)))
            && mgr.status("b").is_some_and(|s| s.position == Some(Txid(1)))
            && mgr.status("fa").is_some_and(|s| s.position == Some(Txid(1)))
    });
    assert!(converged, "initial replication never converged: {:?}", mgr.statuses());

    // New commits flow through the running workers.
    insert_rows(&conn_a, "a1", 10);
    let converged = wait_for(Duration::from_secs(30), || {
        mgr.status("fa").is_some_and(|s| s.position == Some(Txid(2)))
    });
    assert!(converged, "follower never caught up to txid 2: {:?}", mgr.statuses());

    let statuses = mgr.statuses();
    assert_eq!(
        statuses.iter().map(|s| (s.id.as_str(), s.role)).collect::<Vec<_>>(),
        vec![("a", DbRole::Push), ("b", DbRole::Push), ("fa", DbRole::Follow)]
    );
    for s in &statuses {
        assert!(s.last_error.is_none(), "unexpected error on {}: {:?}", s.id, s.last_error);
        assert!(s.last_activity.is_some(), "no activity recorded on {}", s.id);
    }

    // Join everything before touching replica files (no in-flight applies).
    mgr.shutdown();
    assert_eq!(rows_of(&replica_a), rows_of(&db_a));
    assert_bucket_matches(&bucket_b, &db_b, &tmp.path().join("rb.db"));
}

/// M2: sleep() cancels an in-flight (blocked) push, the worker drops its
/// Writer — observable as a successful `PRAGMA wal_checkpoint(TRUNCATE)`
/// from a separate connection — and a sleeping entry generates zero storage
/// traffic, ignoring nudges. Resume rebuilds a fresh session and uploads
/// the backlog, including rows committed while asleep.
#[test]
fn sleep_cancels_inflight_releases_wal_lock_and_goes_quiet() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m2", 10);

    let st = ClientState::default();
    st.block_writes.store(true, Ordering::SeqCst);
    let (started_tx, started_rx) = mpsc::channel();
    *st.started.lock().unwrap() = Some(started_tx);

    let mgr = manager();
    let cfg = PushConfig {
        storage: custom_storage(&bucket, st.clone()),
        writer_options: WriterOptions::default(),
        push_interval: None, // rounds only on push_now: deterministic
        maintenance: None,
        backoff: None,
    };
    mgr.register_push("p", &db, cfg).unwrap();

    mgr.push_now("p").unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("push never reached the blocked upload");

    // While the push is in flight the Writer's read transaction pins the
    // WAL: a TRUNCATE checkpoint from a second connection cannot complete.
    assert_eq!(truncate_checkpoint_busy(&db), 1, "expected the WAL to be pinned mid-push");

    mgr.sleep("p").unwrap();

    // The worker observes the cancelled token, regains control, and drops
    // the Writer: now the TRUNCATE checkpoint succeeds.
    let released =
        wait_for(Duration::from_secs(10), || truncate_checkpoint_busy(&db) == 0);
    assert!(released, "WAL read lock never released after sleep()");
    assert!(matches!(mgr.status("p").unwrap().state, DbState::Sleeping));

    // Zero traffic while sleeping; nudges are ignored.
    let ops_before = st.ops.load(Ordering::SeqCst);
    insert_rows(&conn, "asleep", 10);
    mgr.push_now("p").unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(st.ops.load(Ordering::SeqCst), ops_before, "storage traffic while sleeping");
    assert!(matches!(mgr.status("p").unwrap().state, DbState::Sleeping));

    // Resume: fresh token + fresh client; the backlog (txid 1) and the
    // rows committed while asleep (txid 2) both land.
    st.block_writes.store(false, Ordering::SeqCst);
    mgr.resume("p").unwrap();
    let caught_up = wait_for(Duration::from_secs(30), || {
        mgr.status("p").is_some_and(|s| s.position == Some(Txid(2)))
    });
    assert!(caught_up, "resume never caught up: {:?}", mgr.status("p"));

    mgr.shutdown();
    assert_bucket_matches(&bucket, &db, &tmp.path().join("check.db"));
}

/// M3: pure-dir resume catch-up — rows written while an interval-driven
/// push entry sleeps arrive after resume.
#[test]
fn resume_catches_up_after_sleep() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m3", 10);

    let mgr = manager();
    mgr.register_push("p", &db, dir_push_cfg(&bucket, Some(Duration::from_millis(20))))
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p").is_some_and(|s| s.position == Some(Txid(1)))
        }),
        "initial push never landed"
    );

    mgr.sleep("p").unwrap();
    insert_rows(&conn, "while-asleep", 10);

    mgr.resume("p").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p").is_some_and(|s| s.position >= Some(Txid(2)))
        }),
        "post-resume push never landed: {:?}",
        mgr.status("p")
    );

    mgr.shutdown();
    assert_bucket_matches(&bucket, &db, &tmp.path().join("check.db"));
}

/// M4: sleep_all/resume_all across mixed roles — both entries go quiet,
/// then both catch up after resume.
#[test]
fn sleep_all_resume_all_mixed_roles() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let replica = tmp.path().join("replica.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m4", 10);

    let mgr = manager();
    mgr.register_push("p", &db, dir_push_cfg(&bucket, Some(Duration::from_millis(20))))
        .unwrap();
    mgr.register_follow("f", &replica, dir_follow_cfg(&bucket)).unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p").is_some_and(|s| s.position == Some(Txid(1)))
                && mgr.status("f").is_some_and(|s| s.position == Some(Txid(1)))
        }),
        "initial replication never converged: {:?}",
        mgr.statuses()
    );

    mgr.sleep_all();
    assert!(
        wait_for(Duration::from_secs(10), || {
            mgr.statuses().iter().all(|s| s.state == DbState::Sleeping)
        }),
        "not everything went to sleep: {:?}",
        mgr.statuses()
    );

    insert_rows(&conn, "while-asleep", 10);
    mgr.resume_all();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p").is_some_and(|s| s.position >= Some(Txid(2)))
                && mgr.status("f").is_some_and(|s| s.position >= Some(Txid(2)))
        }),
        "resume_all never caught up: {:?}",
        mgr.statuses()
    );

    mgr.shutdown();
    assert_eq!(rows_of(&replica), rows_of(&db));
}

/// M5: unregister joins the worker and releases the Writer (WAL lock
/// observable), frees the id and the db path, and a re-registration under
/// the same id works.
#[test]
fn unregister_joins_and_frees_id() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m5", 10);

    let mgr = manager();
    mgr.register_push("u", &db, dir_push_cfg(&bucket, None)).unwrap();
    mgr.push_now("u").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("u").is_some_and(|s| s.position == Some(Txid(1)))
        }),
        "first push never landed"
    );

    mgr.unregister("u").unwrap();
    // The join is synchronous: the Writer is gone the moment we return.
    assert_eq!(truncate_checkpoint_busy(&db), 0, "unregister left the WAL pinned");
    assert!(mgr.status("u").is_none());
    assert!(mgr.unregister("u").is_err(), "double unregister must error");

    // Same id, same db path: both freed by the unregister.
    insert_rows(&conn, "again", 10);
    mgr.register_push("u", &db, dir_push_cfg(&bucket, None)).unwrap();
    mgr.push_now("u").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("u").is_some_and(|s| s.position == Some(Txid(2)))
        }),
        "post-re-register push never landed: {:?}",
        mgr.status("u")
    );

    mgr.shutdown();
    assert_bucket_matches(&bucket, &db, &tmp.path().join("check.db"));
}

/// M6: the observer sees the full event sequence, in order, for a
/// nudge-push → sleep → resume cycle. Rounds run only on push_now (no
/// interval), so the sequence is deterministic.
#[test]
fn observer_sees_events_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m6", 5);

    let mgr = manager();
    let obs = Arc::new(RecordingObserver::default());
    mgr.set_observer(Some(obs.clone()));

    mgr.register_push("o", &db, dir_push_cfg(&bucket, None)).unwrap();
    mgr.push_now("o").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || obs.snapshot().len() >= 3),
        "first push round produced no events: {:?}",
        obs.snapshot()
    );

    mgr.sleep("o").unwrap();
    assert!(
        wait_for(Duration::from_secs(10), || obs.snapshot().len() >= 4),
        "sleep produced no event"
    );

    mgr.resume("o").unwrap(); // schedules an immediate (no-op) push round
    assert!(
        wait_for(Duration::from_secs(30), || obs.snapshot().len() >= 7),
        "resume round incomplete: {:?}",
        obs.snapshot().iter().map(kind).collect::<Vec<_>>()
    );

    let kinds: Vec<String> = obs.snapshot().iter().map(kind).collect();
    assert_eq!(
        kinds,
        vec![
            "state:Working", // push_now
            "push",
            "state:Idle",
            "state:Sleeping", // sleep()
            "state:Working",  // resume() emits nothing; its round starts here
            "push",
            "state:Idle",
        ],
        "unexpected event sequence"
    );
    // Every event carries the right id.
    for e in obs.snapshot() {
        let id = match e {
            ManagerEvent::StateChanged { id, .. }
            | ManagerEvent::PushCompleted { id, .. }
            | ManagerEvent::SyncCompleted { id, .. }
            | ManagerEvent::Error { id, .. } => id,
        };
        assert_eq!(id, "o");
    }

    mgr.shutdown();
}

/// M7: a fatal (non-transient) error parks the entry as Failed with the
/// error message in status; a push_now nudge retries once, and with the
/// fault cleared the entry recovers.
#[test]
fn failed_entry_retries_on_nudge() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m7", 10);

    let st = ClientState::default();
    st.fail_fatal_writes.store(1, Ordering::SeqCst);

    let mgr = manager();
    let cfg = PushConfig {
        storage: custom_storage(&bucket, st.clone()),
        writer_options: WriterOptions::default(),
        push_interval: None,
        maintenance: None,
        backoff: None,
    };
    mgr.register_push("p", &db, cfg).unwrap();

    mgr.push_now("p").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p").is_some_and(|s| s.state == DbState::Failed)
        }),
        "entry never reached Failed: {:?}",
        mgr.status("p")
    );
    let status = mgr.status("p").unwrap();
    assert!(
        status.last_error.as_deref().unwrap_or("").contains("conflict"),
        "expected the injected conflict in last_error: {status:?}"
    );

    // The fault was one-shot; the nudge retries once and succeeds.
    mgr.push_now("p").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p")
                .is_some_and(|s| s.position == Some(Txid(1)) && s.state == DbState::Idle)
        }),
        "nudge retry never recovered: {:?}",
        mgr.status("p")
    );

    mgr.shutdown();
    assert_bucket_matches(&bucket, &db, &tmp.path().join("check.db"));
}

/// M8: dropping the Manager with active workers — one push blocked
/// mid-upload, one live follower — cancels and joins everything well under
/// the 10s bound.
#[test]
fn drop_with_active_workers_exits_under_10s() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let replica = tmp.path().join("replica.db");
    let bucket = tmp.path().join("bucket");
    let blocked_bucket = tmp.path().join("bucket-blocked");
    let conn = create_db(&db);
    insert_rows(&conn, "m8", 10);

    // Seed the follower's bucket so it is mid-follow, not waiting on an
    // empty bucket (either way it must stop promptly; this covers the
    // busier path).
    {
        let mut w = liters::Writer::open(
            &db,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        w.push().unwrap();
    }

    // The blocked push gets its own database: `db`'s meta dir has already
    // verified positions against `bucket`, so pointing it at the fresh
    // `blocked_bucket` would (correctly) reseed instead of uploading — and
    // this test needs the first push to reach a blocked upload.
    let db2 = tmp.path().join("app2.db");
    let conn2 = create_db(&db2);
    insert_rows(&conn2, "m8-blocked", 10);

    let st = ClientState::default();
    st.block_writes.store(true, Ordering::SeqCst);
    let (started_tx, started_rx) = mpsc::channel();
    *st.started.lock().unwrap() = Some(started_tx);

    let mgr = manager();
    let cfg = PushConfig {
        storage: custom_storage(&blocked_bucket, st),
        writer_options: WriterOptions::default(),
        push_interval: None,
        maintenance: None,
        backoff: None,
    };
    mgr.register_push("blocked", &db2, cfg).unwrap();
    mgr.register_follow("f", &replica, dir_follow_cfg(&bucket)).unwrap();

    mgr.push_now("blocked").unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("push never reached the blocked upload");
    // Make sure the follower has actually started a session too.
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("f").is_some_and(|s| s.position == Some(Txid(1)))
        }),
        "follower never started"
    );

    let start = Instant::now();
    drop(mgr); // shutdown(): cancel all sessions, join all workers
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(10), "shutdown took {elapsed:?}");
}

/// M9: duplicate ids and duplicate db paths are rejected at registration;
/// unregister frees both.
#[test]
fn duplicate_registrations_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let _conn = create_db(&db);

    let mgr = manager();
    mgr.register_push("x", &db, dir_push_cfg(&bucket, None)).unwrap();

    // Duplicate id (any role).
    assert!(
        mgr.register_follow("x", tmp.path().join("other.db"), dir_follow_cfg(&bucket)).is_err(),
        "duplicate id must be rejected"
    );
    // Same file under a different id and spelling.
    let alias = tmp.path().join(".").join("app.db");
    assert!(
        mgr.register_push("y", &alias, dir_push_cfg(&bucket, None)).is_err(),
        "duplicate db path must be rejected"
    );
    // Role mismatches on the nudge entry points.
    assert!(mgr.sync_now("x").is_err(), "sync_now on a push entry must error");
    assert!(mgr.push_now("nope").is_err(), "unknown id must error");

    mgr.unregister("x").unwrap();
    mgr.register_push("y", &db, dir_push_cfg(&bucket, None)).unwrap();

    mgr.shutdown();
}

/// M11: transient failures back off and auto-retry — a single nudge rides
/// out two injected transient failures (BackingOff state + transient Error
/// events observed) and recovers without further nudging.
#[test]
fn transient_failure_backs_off_and_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m11", 10);

    let st = ClientState::default();
    st.fail_transient_writes.store(2, Ordering::SeqCst);

    let mgr = manager(); // fast_backoff: 10ms initial, so retries are quick
    let obs = Arc::new(RecordingObserver::default());
    mgr.set_observer(Some(obs.clone()));
    let cfg = PushConfig {
        storage: custom_storage(&bucket, st),
        writer_options: WriterOptions::default(),
        push_interval: None,
        maintenance: None,
        backoff: None,
    };
    mgr.register_push("p", &db, cfg).unwrap();

    // One nudge only: the backoff schedule must drive both retries itself.
    mgr.push_now("p").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p")
                .is_some_and(|s| s.position == Some(Txid(1)) && s.state == DbState::Idle)
        }),
        "backoff never recovered: {:?}",
        mgr.status("p")
    );

    let status = mgr.status("p").unwrap();
    assert!(status.last_error.is_none(), "success must clear last_error: {status:?}");
    let events = obs.snapshot();
    assert!(
        events.iter().any(|e| matches!(e, ManagerEvent::Error { transient: true, .. })),
        "expected a transient Error event: {:?}",
        events.iter().map(kind).collect::<Vec<_>>()
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            ManagerEvent::StateChanged { state: DbState::BackingOff { .. }, .. }
        )),
        "expected a BackingOff state event: {:?}",
        events.iter().map(kind).collect::<Vec<_>>()
    );

    mgr.shutdown();
    assert_bucket_matches(&bucket, &db, &tmp.path().join("check.db"));
}

/// M10: sync_now on a follower forces an immediate session restart (the
/// restart begins with a full sync round), picking up data pushed while
/// the follower was between polls.
#[test]
fn sync_now_forces_follower_round() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let replica = tmp.path().join("replica.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m10", 10);

    // Seed the bucket directly (no manager push entry needed).
    let mut w = liters::Writer::open(
        &db,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    w.push().unwrap();

    let mgr = manager();
    // A long poll interval: without a nudge the follower would idle for
    // minutes between rounds, so convergence below proves sync_now works.
    let cfg = FollowConfig {
        storage: StorageConfig::Dir { path: bucket.clone() },
        replica_options: ReplicaOptions::default(),
        follow_options: FollowOptions { poll_interval: Duration::from_secs(120), retry: None },
    };
    mgr.register_follow("f", &replica, cfg).unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("f").is_some_and(|s| s.position == Some(Txid(1)))
        }),
        "initial restore never happened: {:?}",
        mgr.status("f")
    );

    insert_rows(&conn, "late", 10);
    w.push().unwrap();
    mgr.sync_now("f").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("f").is_some_and(|s| s.position == Some(Txid(2)))
        }),
        "sync_now never forced a round: {:?}",
        mgr.status("f")
    );

    mgr.shutdown();
    assert_eq!(rows_of(&replica), rows_of(&db));
}

// ---------------------------------------------------------------------------
// Manager-scheduled maintenance (PushConfig::maintenance)

/// Compact L1 immediately, prune covered L0s with no grace; the periodic
/// snapshot stays far away (an initial L9 is still written because an empty
/// snapshot level is always due).
fn aggressive_maintenance() -> MaintenanceOptions {
    MaintenanceOptions {
        level_intervals: vec![Duration::ZERO],
        snapshot_interval: Duration::from_secs(100_000),
        snapshot_retention: Duration::from_secs(24 * 60 * 60),
        l0_retention: Duration::ZERO,
        retention_enabled: true,
    }
}

/// M12: with maintenance configured, a successful push is followed by a
/// maintain round — observable as an L1 compaction landing in the bucket —
/// and the entry returns to Idle with no error.
#[test]
fn maintenance_runs_after_push_and_compacts() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m12", 10);

    let mgr = manager();
    let cfg = PushConfig {
        storage: StorageConfig::Dir { path: bucket.clone() },
        writer_options: WriterOptions::default(),
        push_interval: None, // rounds only on push_now: deterministic
        maintenance: Some((aggressive_maintenance(), Duration::ZERO)),
        backoff: None,
    };
    mgr.register_push("p", &db, cfg).unwrap();

    mgr.push_now("p").unwrap();
    let compacted = wait_for(Duration::from_secs(30), || {
        !DirReplicaClient::new(&bucket).ltx_files(1, Txid(0), false).unwrap_or_default().is_empty()
    });
    assert!(compacted, "maintain never landed an L1 compaction: {:?}", mgr.status("p"));
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p")
                .is_some_and(|s| s.position == Some(Txid(1)) && s.state == DbState::Idle)
        }),
        "entry never settled after push+maintain: {:?}",
        mgr.status("p")
    );
    assert!(mgr.status("p").unwrap().last_error.is_none());

    // A second round keeps working (interval ZERO: maintain re-attempts).
    insert_rows(&conn, "m12-b", 10);
    mgr.push_now("p").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p")
                .is_some_and(|s| s.position == Some(Txid(2)) && s.state == DbState::Idle)
        }),
        "second push round never settled: {:?}",
        mgr.status("p")
    );

    mgr.shutdown();
    assert_bucket_matches(&bucket, &db, &tmp.path().join("check.db"));
}

/// M13: a failing maintain is reported (Error event, last_error) but NEVER
/// parks the entry — the state returns to Idle, and subsequent pushes keep
/// landing.
#[test]
fn maintain_failure_reports_but_never_fails_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m13", 10);

    let st = ClientState::default();
    st.fail_fatal_high_level_writes.store(true, Ordering::SeqCst);

    let mgr = manager();
    let obs = Arc::new(RecordingObserver::default());
    mgr.set_observer(Some(obs.clone()));
    let cfg = PushConfig {
        storage: custom_storage(&bucket, st.clone()),
        writer_options: WriterOptions::default(),
        push_interval: None,
        maintenance: Some((aggressive_maintenance(), Duration::ZERO)),
        backoff: None,
    };
    mgr.register_push("p", &db, cfg).unwrap();

    mgr.push_now("p").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p")
                .is_some_and(|s| s.position == Some(Txid(1)) && s.state == DbState::Idle)
        }),
        "first push+failed-maintain never settled at Idle: {:?}",
        mgr.status("p")
    );
    let status = mgr.status("p").unwrap();
    assert!(
        status.last_error.as_deref().unwrap_or("").contains("maintenance"),
        "the maintain failure must be recorded: {status:?}"
    );

    // The failure must not block the next push (which attempts maintain
    // again — the cadence measures attempts).
    insert_rows(&conn, "m13-b", 10);
    mgr.push_now("p").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p")
                .is_some_and(|s| s.position == Some(Txid(2)) && s.state == DbState::Idle)
        }),
        "push after a failed maintain never landed: {:?}",
        mgr.status("p")
    );

    let events = obs.snapshot();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, ManagerEvent::StateChanged { state: DbState::Failed, .. })),
        "a maintain failure must never transition the entry to Failed: {:?}",
        events.iter().map(kind).collect::<Vec<_>>()
    );
    let errors = events.iter().filter(|e| matches!(e, ManagerEvent::Error { .. })).count();
    assert_eq!(errors, 2, "one maintain attempt (and Error) per successful push");
    let pushes =
        events.iter().filter(|e| matches!(e, ManagerEvent::PushCompleted { .. })).count();
    assert_eq!(pushes, 2);

    mgr.shutdown();
    // The L0 chain (level-0 writes were never failed) still restores.
    assert_bucket_matches(&bucket, &db, &tmp.path().join("check.db"));
}

/// M14: the maintenance cadence gates attempts — with a long interval, the
/// first successful push attempts maintenance and the second one does NOT.
/// (Attempts are observable one-to-one as Error events via the injected
/// maintenance-only fault.)
#[test]
fn maintenance_cadence_gates_attempts() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "m14", 10);

    let st = ClientState::default();
    st.fail_fatal_high_level_writes.store(true, Ordering::SeqCst);

    let mgr = manager();
    let obs = Arc::new(RecordingObserver::default());
    mgr.set_observer(Some(obs.clone()));
    let cfg = PushConfig {
        storage: custom_storage(&bucket, st.clone()),
        writer_options: WriterOptions::default(),
        push_interval: None,
        maintenance: Some((aggressive_maintenance(), Duration::from_secs(3600))),
        backoff: None,
    };
    mgr.register_push("p", &db, cfg).unwrap();

    mgr.push_now("p").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p")
                .is_some_and(|s| s.position == Some(Txid(1)) && s.state == DbState::Idle)
        }),
        "first round never settled: {:?}",
        mgr.status("p")
    );
    let errors = |obs: &RecordingObserver| {
        obs.snapshot().iter().filter(|e| matches!(e, ManagerEvent::Error { .. })).count()
    };
    assert_eq!(errors(&obs), 1, "the first push must attempt maintenance immediately");

    insert_rows(&conn, "m14-b", 10);
    mgr.push_now("p").unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || {
            mgr.status("p")
                .is_some_and(|s| s.position == Some(Txid(2)) && s.state == DbState::Idle)
        }),
        "second round never settled: {:?}",
        mgr.status("p")
    );
    assert_eq!(
        errors(&obs),
        1,
        "the second push must NOT re-attempt maintenance inside the interval"
    );

    mgr.shutdown();
    assert_bucket_matches(&bucket, &db, &tmp.path().join("check.db"));
}
