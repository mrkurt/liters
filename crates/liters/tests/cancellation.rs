//! Cancellation tests for the `_with` entry points: a blocked push/maintain
//! returns `Error::Cancelled` promptly and leaves state retryable, follow
//! treats cancellation as a clean stop, and follow's backoff attempt counter
//! resets on progress.
//!
//! Conventions (mirroring http_follow.rs): all DB load is bounded; every
//! blocking wait carries a deadline that fails the test instead of hanging
//! it; follower threads run under std::thread::scope with a StopGuard so a
//! panicking test body can never leave a follow() thread blocking the scope
//! join; replica rows are only read after the follower has been cancelled
//! and joined.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{mpsc, Mutex};
use std::thread::ScopedJoinHandle;
use std::time::{Duration, Instant};

use liters::{
    Backoff, CancelToken, DirReplicaClient, Error, FollowOptions, MaintenanceOptions, Replica,
    ReplicaClient, ReplicaOptions, StorageError, Writer, WriterOptions,
};
use ltx::{FileInfo, Txid};
use rusqlite::Connection;

const POLL: Duration = Duration::from_millis(50);

fn create_db(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn
}

fn setup(tmp: &Path) -> (Connection, PathBuf, PathBuf) {
    let db_path = tmp.join("app.db");
    let bucket = tmp.join("bucket");
    let conn = create_db(&db_path);
    (conn, db_path, bucket)
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

fn insert_rows(conn: &Connection, tag: &str, n: usize) {
    for j in 0..n {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("{tag}-{j}")]).unwrap();
    }
}

fn sidecar_txid(db_path: &Path) -> u64 {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-txid");
    match std::fs::read_to_string(PathBuf::from(p)) {
        Ok(s) => u64::from_str_radix(s.trim(), 16).unwrap_or(0),
        Err(_) => 0,
    }
}

/// Polls `cond` every 50ms until it holds, `deadline` passes, or the
/// follower thread exits early (one final check then). Returns whether the
/// condition held; never panics, so callers can cancel and join before
/// asserting.
fn poll_until<T>(
    deadline: Duration,
    follower: &ScopedJoinHandle<'_, T>,
    mut cond: impl FnMut() -> bool,
) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if cond() {
            return true;
        }
        if follower.is_finished() {
            return cond();
        }
        std::thread::sleep(POLL);
    }
    false
}

/// Cancels the follow() token on drop, so a panic anywhere in a test's
/// scope body can never leave the follower thread running forever (which
/// would hang the scope join, and CI).
struct StopGuard<'a>(&'a CancelToken);

impl Drop for StopGuard<'_> {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Wraps a dir client; writes at `block_level` signal `started` and then
/// park until the installed cancel token flips — the minimal stand-in for a
/// stalled transfer on a token-aware backend (the real one is the HTTP
/// client). The park is deadline-bounded so a broken cancellation path
/// fails the test instead of hanging it.
struct BlockingClient {
    inner: DirReplicaClient,
    block_level: u8,
    started: mpsc::Sender<()>,
    token: Mutex<CancelToken>,
}

impl BlockingClient {
    fn new(bucket: &Path, block_level: u8, started: mpsc::Sender<()>) -> BlockingClient {
        BlockingClient {
            inner: DirReplicaClient::new(bucket),
            block_level,
            started,
            token: Mutex::new(CancelToken::new()),
        }
    }
}

impl ReplicaClient for BlockingClient {
    fn client_type(&self) -> &'static str {
        "blocking"
    }
    fn ltx_files(
        &self,
        level: u8,
        seek: Txid,
        use_metadata: bool,
    ) -> liters_storage::Result<Vec<FileInfo>> {
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
        self.inner.open_ltx_file(level, min, max, offset, size)
    }
    fn write_ltx_file(
        &self,
        level: u8,
        min: Txid,
        max: Txid,
        rd: &mut dyn std::io::Read,
    ) -> liters_storage::Result<FileInfo> {
        if level != self.block_level {
            return self.inner.write_ltx_file(level, min, max, rd);
        }
        let _ = self.started.send(());
        let token = self.token.lock().unwrap().clone();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !token.is_cancelled() {
            if Instant::now() >= deadline {
                return Err(StorageError::Other("blocked write was never cancelled".into()));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Err(StorageError::Cancelled)
    }
    fn delete_ltx_files(&self, infos: &[FileInfo]) -> liters_storage::Result<()> {
        self.inner.delete_ltx_files(infos)
    }
    fn delete_all(&self) -> liters_storage::Result<()> {
        self.inner.delete_all()
    }
    fn set_cancel(&self, token: CancelToken) {
        *self.token.lock().unwrap() = token;
    }
}

/// Wraps a dir client; every operation first checks the token installed via
/// `set_cancel`, mimicking a token-aware backend (the real one is the HTTP
/// client, whose every request begins with a `check_cancel`). This is what
/// makes a *stale* cancelled token observable: an entry point that forgets
/// to install its own token inherits the previous operation's.
struct TokenCheckingClient {
    inner: DirReplicaClient,
    token: Mutex<CancelToken>,
}

impl TokenCheckingClient {
    fn new(bucket: &Path) -> TokenCheckingClient {
        TokenCheckingClient {
            inner: DirReplicaClient::new(bucket),
            token: Mutex::new(CancelToken::new()),
        }
    }
    fn check(&self) -> liters_storage::Result<()> {
        self.token.lock().unwrap().check()
    }
}

impl ReplicaClient for TokenCheckingClient {
    fn client_type(&self) -> &'static str {
        "token-checking"
    }
    fn ltx_files(
        &self,
        level: u8,
        seek: Txid,
        use_metadata: bool,
    ) -> liters_storage::Result<Vec<FileInfo>> {
        self.check()?;
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
        self.check()?;
        self.inner.open_ltx_file(level, min, max, offset, size)
    }
    fn write_ltx_file(
        &self,
        level: u8,
        min: Txid,
        max: Txid,
        rd: &mut dyn std::io::Read,
    ) -> liters_storage::Result<FileInfo> {
        self.check()?;
        self.inner.write_ltx_file(level, min, max, rd)
    }
    fn delete_ltx_files(&self, infos: &[FileInfo]) -> liters_storage::Result<()> {
        self.check()?;
        self.inner.delete_ltx_files(infos)
    }
    fn delete_all(&self) -> liters_storage::Result<()> {
        self.check()?;
        self.inner.delete_all()
    }
    fn set_cancel(&self, token: CancelToken) {
        *self.token.lock().unwrap() = token;
    }
}

// ---------------------------------------------------------------------------

/// C1: cancelling a push blocked mid-upload returns `Error::Cancelled`
/// promptly; the stranded L0 persists locally and a fresh writer with a
/// working client uploads it, leaving the bucket fully restorable.
#[test]
fn cancel_mid_push_then_retry_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());
    insert_rows(&conn, "c1", 20);

    let (started_tx, started_rx) = mpsc::channel();
    let mut w = Writer::open(
        &db_path,
        Box::new(BlockingClient::new(&bucket, 0, started_tx)),
        WriterOptions::default(),
    )
    .unwrap();

    let cancel = CancelToken::new();
    let cancelled_at: Mutex<Option<Instant>> = Mutex::new(None);
    let (err, latency) = std::thread::scope(|s| {
        let canceller = s.spawn({
            let cancel = cancel.clone();
            let cancelled_at = &cancelled_at;
            move || {
                started_rx
                    .recv_timeout(Duration::from_secs(15))
                    .expect("push never reached the blocked upload");
                *cancelled_at.lock().unwrap() = Some(Instant::now());
                cancel.cancel();
            }
        });
        let err = w.push_with(&cancel).expect_err("blocked push must be cancelled");
        let latency =
            cancelled_at.lock().unwrap().expect("cancel signal arrived out of order").elapsed();
        canceller.join().expect("canceller thread panicked");
        (err, latency)
    });
    assert!(matches!(err, Error::Cancelled), "expected Error::Cancelled, got {err:?}");
    assert!(latency < Duration::from_secs(2), "cancellation took {latency:?}");

    // The converted L0 survived the cancelled upload.
    assert_eq!(w.pos().unwrap().txid, Txid(1));
    drop(w);

    // Retry with a working client: the backlog uploads and the bucket is
    // intact end to end.
    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    let r = w.push().unwrap();
    assert_eq!(r.remote_txid, Txid(1));

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    let sr = rep.sync().unwrap();
    assert_eq!(sr.to_txid, Txid(1));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

/// C2: cancelling a maintain blocked mid-compaction-upload returns
/// `Error::Cancelled` promptly; the abandoned run left the bucket valid and
/// a later maintain over a working client completes it.
#[test]
fn cancel_mid_maintain_then_retry_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    // Seed L0 over a working client so an L1 compaction is due.
    {
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        for i in 0..5 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("c2-{i}")]).unwrap();
            w.push().unwrap();
        }
    }

    let opts = MaintenanceOptions {
        level_intervals: vec![Duration::ZERO],
        snapshot_interval: Duration::from_secs(100_000),
        snapshot_retention: Duration::from_secs(24 * 60 * 60),
        l0_retention: Duration::ZERO,
        retention_enabled: true,
    };

    let (started_tx, started_rx) = mpsc::channel();
    let mut w = Writer::open(
        &db_path,
        Box::new(BlockingClient::new(&bucket, 1, started_tx)),
        WriterOptions::default(),
    )
    .unwrap();

    let cancel = CancelToken::new();
    let cancelled_at: Mutex<Option<Instant>> = Mutex::new(None);
    let (err, latency) = std::thread::scope(|s| {
        let canceller = s.spawn({
            let cancel = cancel.clone();
            let cancelled_at = &cancelled_at;
            move || {
                started_rx
                    .recv_timeout(Duration::from_secs(15))
                    .expect("maintain never reached the blocked L1 upload");
                *cancelled_at.lock().unwrap() = Some(Instant::now());
                cancel.cancel();
            }
        });
        let err = w.maintain_with(&cancel, &opts).expect_err("blocked maintain must be cancelled");
        let latency =
            cancelled_at.lock().unwrap().expect("cancel signal arrived out of order").elapsed();
        canceller.join().expect("canceller thread panicked");
        (err, latency)
    });
    assert!(matches!(err, Error::Cancelled), "expected Error::Cancelled, got {err:?}");
    assert!(latency < Duration::from_secs(2), "cancellation took {latency:?}");
    drop(w);

    // The write-then-delete ordering means nothing was lost: a working
    // maintain finishes the compaction and the bucket stays restorable.
    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    let report = w.maintain(&opts).unwrap();
    assert!(report.compacted_levels.contains(&1), "L1 compaction must complete: {report:?}");

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    let sr = rep.sync().unwrap();
    assert_eq!(sr.to_txid, Txid(5));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

/// C3: cancelling a running follow makes it return `Ok(())` (a clean stop,
/// not an error) promptly.
#[test]
fn cancel_mid_follow_returns_ok() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for i in 0..3 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("c3-{i}")]).unwrap();
        w.push().unwrap();
    }

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );

    let cancel = CancelToken::new();
    let fo = FollowOptions { poll_interval: Duration::from_millis(100), retry: None };
    let (converged, latency, res) = std::thread::scope(|s| {
        let follower = s.spawn(|| rep.follow(&cancel, &fo));
        let _stop_guard = StopGuard(&cancel);
        let converged =
            poll_until(Duration::from_secs(30), &follower, || sidecar_txid(&replica_path) == 3);
        let at = Instant::now();
        cancel.cancel();
        let res = follower.join().expect("follower thread panicked");
        (converged, at.elapsed(), res)
    });

    assert!(converged, "follower never restored to txid 3 within 30s");
    res.expect("follow must return Ok on cancellation");
    assert!(latency < Duration::from_secs(2), "follow took {latency:?} to observe the cancel");
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

/// Dir wrapper for C4: level-0 listings fail `fails_per_cycle` times, then
/// succeed truncated to the single next file, repeatedly — every success
/// makes exactly one transaction of progress and is followed by a fresh
/// failure burst. A follow loop whose attempt counter resets on progress
/// pays only delay(0..fails_per_cycle) per burst; one that never resets
/// compounds attempts across bursts into multi-minute sleeps and blows the
/// test deadline.
struct BurstFailClient {
    inner: DirReplicaClient,
    fails_per_cycle: u32,
    cycle_pos: Mutex<u32>,
    fails_injected: AtomicU32,
}

impl ReplicaClient for BurstFailClient {
    fn client_type(&self) -> &'static str {
        "burstfail"
    }
    fn ltx_files(
        &self,
        level: u8,
        seek: Txid,
        use_metadata: bool,
    ) -> liters_storage::Result<Vec<FileInfo>> {
        if level != 0 {
            return self.inner.ltx_files(level, seek, use_metadata);
        }
        let mut pos = self.cycle_pos.lock().unwrap();
        if *pos < self.fails_per_cycle {
            *pos += 1;
            self.fails_injected.fetch_add(1, Ordering::SeqCst);
            return Err(StorageError::Unavailable("injected burst failure".into()));
        }
        *pos = 0;
        let mut files = self.inner.ltx_files(0, seek, use_metadata)?;
        files.truncate(1);
        Ok(files)
    }
    fn open_ltx_file(
        &self,
        level: u8,
        min: Txid,
        max: Txid,
        offset: u64,
        size: u64,
    ) -> liters_storage::Result<Box<dyn std::io::Read + Send>> {
        self.inner.open_ltx_file(level, min, max, offset, size)
    }
    fn write_ltx_file(
        &self,
        level: u8,
        min: Txid,
        max: Txid,
        rd: &mut dyn std::io::Read,
    ) -> liters_storage::Result<FileInfo> {
        self.inner.write_ltx_file(level, min, max, rd)
    }
    fn delete_ltx_files(&self, infos: &[FileInfo]) -> liters_storage::Result<()> {
        self.inner.delete_ltx_files(infos)
    }
    fn delete_all(&self) -> liters_storage::Result<()> {
        self.inner.delete_all()
    }
}

/// C4: follow's backoff attempt counter resets on progress. Twenty
/// single-txid progress steps each gated behind a burst of transient
/// failures converge quickly under a genuinely exponential Backoff (10ms
/// doubling to a 60s cap) — only possible if every burst restarts at
/// attempt 0.
#[test]
fn follow_backoff_attempt_resets_on_progress() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for i in 0..20 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("c4-{i}")]).unwrap();
        w.push().unwrap();
    }
    drop(w);

    let client = Box::new(BurstFailClient {
        inner: DirReplicaClient::new(&bucket),
        fails_per_cycle: 4,
        cycle_pos: Mutex::new(0),
        fails_injected: AtomicU32::new(0),
    });
    let fails_injected: *const AtomicU32 = &client.fails_injected;

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(&replica_path, client, ReplicaOptions::default());

    let cancel = CancelToken::new();
    let fo = FollowOptions {
        poll_interval: Duration::from_millis(100),
        retry: Some(Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_secs(60),
            multiplier: 2.0,
            jitter: 0.0,
        }),
    };
    let (converged, res) = std::thread::scope(|s| {
        let follower = s.spawn(|| rep.follow(&cancel, &fo));
        let _stop_guard = StopGuard(&cancel);
        let converged =
            poll_until(Duration::from_secs(60), &follower, || sidecar_txid(&replica_path) == 20);
        cancel.cancel();
        (converged, follower.join().expect("follower thread panicked"))
    });

    res.expect("follow must survive transient bursts and stop cleanly");
    assert!(
        converged,
        "follower never reached txid 20 within 60s — attempt counter likely not resetting"
    );
    // Prove the schedule was actually exercised: at least ten full bursts
    // interleaved with the twenty progress steps.
    let injected = unsafe { (*fails_injected).load(Ordering::SeqCst) };
    assert!(injected >= 40, "only {injected} transient failures injected");
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

/// C5 (regression): a cancelled operation's token must never poison the next
/// one. Every public entry point installs its own token on the storage
/// client at entry; `compact_level` used to skip that, so on a token-aware
/// backend the client cell still held the cancelled token from a previous
/// push and every direct `compact_level` call failed with `Cancelled`
/// forever — deterministically, with no cancellation requested.
#[test]
fn stale_cancelled_token_does_not_poison_next_operation() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(TokenCheckingClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for i in 0..3 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("c5-{i}")]).unwrap();
        w.push().unwrap();
    }

    // A cancelled push leaves the client cell holding a cancelled token
    // (tokens are sticky by design).
    let cancelled = CancelToken::new();
    cancelled.cancel();
    let err = w.push_with(&cancelled).expect_err("pre-cancelled push must fail");
    assert!(matches!(err, Error::Cancelled), "expected Cancelled, got {err:?}");

    // Direct compact_level must run on its own fresh token, not the stale
    // cancelled one.
    let compacted = w
        .compact_level(1)
        .expect("compact_level after a cancelled push must not inherit the stale token");
    assert!(compacted, "three uncompacted L0s were available");

    // maintain() likewise (it always installed its own token; assert the
    // contract for the whole entry-point family).
    let opts = MaintenanceOptions {
        level_intervals: vec![Duration::ZERO],
        snapshot_interval: Duration::from_secs(100_000),
        snapshot_retention: Duration::from_secs(24 * 60 * 60),
        l0_retention: Duration::ZERO,
        retention_enabled: true,
    };
    match w.maintain(&opts) {
        Ok(_) => {}
        Err(e) => panic!("maintain after a cancelled push must not be Cancelled: {e:?}"),
    }
    // And snapshot() (one more txid so the L9 target is fresh).
    conn.execute("INSERT INTO t (v) VALUES ('c5-post')", []).unwrap();
    w.push().unwrap();
    assert_eq!(w.snapshot().expect("snapshot must not inherit the stale token"), Some(Txid(4)));

    // The bucket stayed coherent through all of it.
    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    assert_eq!(rep.sync().unwrap().to_txid, Txid(4));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}
