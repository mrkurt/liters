//! Exercises the FFI Manager and Writer objects from Rust — the same types
//! UniFFI exports — over dir storage. Conventions match the core manager
//! tests: all DB load is bounded, every wait is a deadline-bounded poll,
//! and replica files are only read after their worker is unregistered.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use liters_ffi::{
    DbRole, DbState, FollowOptions, LitersManager, LitersWriter, ManagerEvent, ManagerListener,
    PushOptions, Storage,
};
use rusqlite::Connection;

const POLL: Duration = Duration::from_millis(10);
const DEADLINE: Duration = Duration::from_secs(10);

fn create_db(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(Duration::from_secs(5)).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn
}

/// One transaction per batch so a concurrent push can never split it.
fn insert_rows(conn: &Connection, tag: &str, n: usize) {
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    for j in 0..n {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("{tag}-{j}")]).unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
}

fn rows_of(path: &Path) -> Vec<String> {
    let conn =
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut stmt = conn.prepare("SELECT v FROM t ORDER BY id").unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn wait_for(mut cond: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + DEADLINE;
    while Instant::now() < end {
        if cond() {
            return true;
        }
        std::thread::sleep(POLL);
    }
    cond()
}

fn dir_storage(path: &Path) -> Storage {
    Storage::Dir { path: path.to_string_lossy().into_owned() }
}

fn push_defaults() -> PushOptions {
    PushOptions { push_interval_ms: None, maintenance_interval_ms: None, backoff: None }
}

struct Recorder(Mutex<Vec<ManagerEvent>>);

impl ManagerListener for Recorder {
    fn on_event(&self, event: ManagerEvent) {
        self.0.lock().unwrap().push(event);
    }
}

/// register_push → push_now → status → sleep → resume → follow catch-up →
/// unregister, all through the FFI surface.
#[test]
fn manager_lifecycle_over_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let replica = tmp.path().join("replica.db");
    let conn = create_db(&db);
    insert_rows(&conn, "first", 5);

    let mgr = LitersManager::new();
    let rec = Arc::new(Recorder(Mutex::new(Vec::new())));
    mgr.set_listener(Some(rec.clone()));

    mgr.register_push(
        "app".into(),
        db.to_string_lossy().into_owned(),
        dir_storage(&bucket),
        push_defaults(),
    )
    .unwrap();
    // Duplicate id and duplicate path both refuse.
    assert!(mgr
        .register_push(
            "app".into(),
            db.to_string_lossy().into_owned(),
            dir_storage(&bucket),
            push_defaults(),
        )
        .is_err());

    // No interval configured: nothing pushes until the nudge.
    mgr.push_now("app".into()).unwrap();
    assert!(wait_for(|| mgr.status("app".into()).is_some_and(|s| s.position.is_some())));
    let st = mgr.status("app".into()).unwrap();
    assert!(matches!(st.role, DbRole::Push));
    assert!(st.last_error.is_none());
    let pos1 = st.position.unwrap();
    assert!(pos1 >= 1);
    assert!(bucket.join("ltx").is_dir(), "push landed nothing in the bucket");

    // Sleep is immediate and observable; rows written while asleep arrive
    // only after resume (which schedules an immediate round).
    mgr.sleep("app".into()).unwrap();
    assert!(matches!(mgr.status("app".into()).unwrap().state, DbState::Sleeping));
    insert_rows(&conn, "while-asleep", 5);
    mgr.push_now("app".into()).unwrap(); // ignored while sleeping
    mgr.resume("app".into()).unwrap();
    assert!(wait_for(|| mgr
        .status("app".into())
        .is_some_and(|s| s.position.is_some_and(|p| p > pos1))));

    // A follower registered on the same bucket materializes everything.
    mgr.register_follow(
        "replica".into(),
        replica.to_string_lossy().into_owned(),
        dir_storage(&bucket),
        FollowOptions { auto_reset: false, poll_interval_ms: Some(20), retry: None },
    )
    .unwrap();
    let target = mgr.status("app".into()).unwrap().position.unwrap();
    assert!(wait_for(|| mgr
        .status("replica".into())
        .is_some_and(|s| s.position.is_some_and(|p| p >= target))));

    // Read the replica only after its worker is joined.
    mgr.unregister("replica".into()).unwrap();
    assert!(mgr.status("replica".into()).is_none());
    assert_eq!(rows_of(&replica).len(), 10);

    mgr.unregister("app".into()).unwrap();
    assert!(mgr.statuses().is_empty());

    let events = rec.0.lock().unwrap();
    assert!(events.iter().any(|e| matches!(e, ManagerEvent::PushCompleted { id, .. } if id == "app")));
    assert!(events.iter().any(
        |e| matches!(e, ManagerEvent::StateChanged { id, state: DbState::Sleeping } if id == "app")
    ));
    drop(events);

    mgr.shutdown(); // idempotent with the unregisters above
}

/// close() releases the database and later calls error clearly; cancel()
/// after close is a no-op.
#[test]
fn writer_close_semantics() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "x", 3);

    let w = LitersWriter::new(db.to_string_lossy().into_owned(), dir_storage(&bucket)).unwrap();
    let s = w.push().unwrap();
    assert!(s.txid >= 1);

    w.close();
    w.close(); // idempotent
    w.cancel(); // no-op after close
    let err = w.push().unwrap_err();
    assert!(err.to_string().contains("closed"), "unexpected error: {err}");
    assert!(w.position().is_err());

    // The WAL read lock is gone: a full checkpoint now succeeds.
    let busy: i64 =
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0)).unwrap();
    assert_eq!(busy, 0, "checkpoint still blocked after close()");
}

/// A listener that throws on every event must never take down a replication
/// worker. (Over the real FFI a thrown Kotlin/Swift exception surfaces as a
/// Rust panic in the generated glue; the adapter contains it with
/// catch_unwind — a panicking Rust impl exercises the same containment.)
#[test]
fn panicking_listener_does_not_kill_worker() {
    struct PanickingListener;
    impl ManagerListener for PanickingListener {
        fn on_event(&self, _event: ManagerEvent) {
            panic!("listener blew up");
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db);
    insert_rows(&conn, "boom", 5);

    let mgr = LitersManager::new();
    mgr.set_listener(Some(Arc::new(PanickingListener)));
    mgr.register_push(
        "app".into(),
        db.to_string_lossy().into_owned(),
        dir_storage(&bucket),
        push_defaults(),
    )
    .unwrap();

    // Every event panics in the listener; the worker must survive and the
    // push must land regardless.
    mgr.push_now("app".into()).unwrap();
    assert!(
        wait_for(|| mgr.status("app".into()).is_some_and(|s| s.position == Some(1))),
        "push never landed with a panicking listener: {:?}",
        mgr.status("app".into())
    );

    // The worker is still alive: sleep (whose event also panics), resume,
    // and a second round all work.
    mgr.sleep("app".into()).unwrap();
    assert!(matches!(mgr.status("app".into()).unwrap().state, DbState::Sleeping));
    insert_rows(&conn, "boom2", 5);
    mgr.resume("app".into()).unwrap();
    assert!(
        wait_for(|| mgr.status("app".into()).is_some_and(|s| s.position == Some(2))),
        "second push never landed after sleep/resume: {:?}",
        mgr.status("app".into())
    );

    mgr.unregister("app".into()).unwrap();
    assert!(rows_of(&db).len() == 10);
}
