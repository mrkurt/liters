//! Push-replication tests: a `Writer` whose destination client is an
//! `HttpReplicaClient` pushes LTX files to a listening liters `HttpServer`
//! running with `writable: true` (reversed roles — the writer dials out, the
//! receiver materializes the bucket). Covers plain pushes, maintenance
//! (compaction/retention/snapshot) carried over HTTP, the relay shape
//! (push in, /stream out), loopback self-follow, writer reopen/resume, and
//! the read-only rejection path. P3 additionally gates on the Go oracle:
//! a bucket written *through* liters HTTP must stay litestream-exact.
//!
//! Conventions (mirroring http_follow.rs): all DB load is bounded (fixed row
//! counts); servers bind 127.0.0.1:0; no latency assertions, only eventual
//! convergence within generous deadlines polled at 50ms; every wait is
//! deadline-bounded. Follower threads run under std::thread::scope with a
//! StopGuard so a panicking test body can never leave a follow() thread
//! blocking the scope join. Replica row contents are only read after the
//! follower thread has been stopped and joined; mid-run progress is observed
//! through the atomically-renamed `{db}-txid` sidecar only. Servers are shut
//! down before their tempdirs drop.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::ScopedJoinHandle;
use std::time::{Duration, Instant};

use liters::{
    DirReplicaClient, FollowOptions, MaintenanceOptions, Replica, ReplicaClient, ReplicaOptions,
    Writer, WriterOptions, SNAPSHOT_LEVEL,
};
use liters_storage::{HttpReplicaClient, HttpServer, HttpServerOptions};
use ltx::Txid;
use rusqlite::Connection;

const POLL: Duration = Duration::from_millis(50);

fn fast_server(writable: bool) -> HttpServerOptions {
    HttpServerOptions {
        poll_interval: Duration::from_millis(100),
        ping_interval: Duration::from_millis(300),
        writable,
    }
}

fn follow_opts(retry: Option<Duration>) -> FollowOptions {
    FollowOptions { poll_interval: Duration::from_millis(100), retry }
}

/// Aggressive maintenance: compact L1 immediately, prune covered L0s with no
/// grace, keep the periodic snapshot far away (an initial L9 is still written
/// because an empty snapshot level is always "due").
fn aggressive_maintenance() -> MaintenanceOptions {
    MaintenanceOptions {
        level_intervals: vec![Duration::ZERO],
        snapshot_interval: Duration::from_secs(100_000),
        snapshot_retention: Duration::from_secs(24 * 60 * 60),
        l0_retention: Duration::ZERO,
        retention_enabled: true,
    }
}

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

fn url_of(srv: &HttpServer) -> String {
    format!("http://127.0.0.1:{}", srv.local_addr().port())
}

fn http_client(srv: &HttpServer) -> Box<dyn ReplicaClient> {
    Box::new(HttpReplicaClient::new(url_of(srv)).unwrap())
}

fn sidecar_path(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-txid");
    PathBuf::from(p)
}

/// Current follower position from the `{db}-txid` sidecar (0 if absent or
/// unparseable). Safe to read while a follower runs: writes are atomic
/// renames.
fn sidecar_txid(db_path: &Path) -> u64 {
    match std::fs::read_to_string(sidecar_path(db_path)) {
        Ok(s) => u64::from_str_radix(s.trim(), 16).unwrap_or(0),
        Err(_) => 0,
    }
}

/// Polls `cond` every 50ms until it holds, `deadline` passes, or the
/// follower thread exits early (one final check then). Returns whether the
/// condition held; never panics, so callers can flip stop flags and join
/// before asserting.
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

/// Sets the follow() stop flag on drop, so a panic anywhere in a test's
/// scope body can never leave the follower thread running forever (which
/// would hang the scope join, and CI).
struct StopGuard<'a>(&'a AtomicBool);

impl Drop for StopGuard<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn oracle_dir() -> Option<PathBuf> {
    let dir = std::env::var_os("LITERS_ORACLE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/oracle"));
    if dir.join("litestream").exists() {
        Some(dir)
    } else {
        eprintln!("SKIP: oracle binaries not found in {dir:?}; run `make oracle`");
        None
    }
}

fn litestream_restore(oracle: &Path, bucket: &Path, out_db: &Path) {
    let _ = std::fs::remove_file(out_db);
    let output = Command::new(oracle.join("litestream"))
        .args([
            "restore",
            "-o",
            out_db.to_str().unwrap(),
            &format!("file://{}", bucket.display()),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "litestream restore failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

// ---------------------------------------------------------------------------

/// P1: pushes travel over HTTP into the receiver's dir bucket; the result is
/// readable both straight off the bucket and back through HTTP.
#[test]
fn push_over_http_then_local_restore() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(true),
    )
    .unwrap();

    let mut w = Writer::open(&db_path, http_client(&srv), WriterOptions::default()).unwrap();
    let mut last = Txid(0);
    for batch in 0..3 {
        insert_rows(&conn, &format!("p1-{batch}"), 40);
        last = w.push().unwrap().remote_txid;
    }
    assert_eq!(last, Txid(3), "3 pushes must land 3 L0s in the receiver's bucket");

    // Straight off the bucket the server wrote into.
    let a_path = tmp.path().join("replica_a.db");
    let mut a = Replica::open(
        &a_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    let ra = a.sync().unwrap();
    assert!(ra.restored);
    assert_eq!(ra.to_txid, Txid(3));

    // And back out through a second HTTP client.
    let b_path = tmp.path().join("replica_b.db");
    let mut b = Replica::open(&b_path, http_client(&srv), ReplicaOptions::default());
    let rb = b.sync().unwrap();
    assert!(rb.restored);
    assert_eq!(rb.to_txid, Txid(3));

    srv.shutdown();

    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 120);
    assert_eq!(rows_of(&a_path), expect);
    assert_eq!(rows_of(&b_path), expect);
}

/// P2: maintenance runs entirely over HTTP — the L1 compaction is written
/// via PUT, covered L0s are pruned via DELETE, an explicit snapshot lands on
/// level 9 — and the bucket stays restorable throughout.
#[test]
fn maintain_over_http_keeps_bucket_valid() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(true),
    )
    .unwrap();

    let mut w = Writer::open(&db_path, http_client(&srv), WriterOptions::default()).unwrap();
    for i in 0..10 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("p2-{i}")]).unwrap();
        w.push().unwrap();
    }

    let report = w.maintain(&aggressive_maintenance()).unwrap();
    assert!(report.compacted_levels.contains(&1), "expected an L1 compaction, got {report:?}");

    let dir = DirReplicaClient::new(&bucket);
    let l1 = dir.ltx_files(1, Txid(0), false).unwrap();
    assert!(!l1.is_empty(), "maintain must have PUT an L1 file over HTTP");
    let l0 = dir.ltx_files(0, Txid(0), false).unwrap();
    assert!(
        l0.len() < 10,
        "maintain must have DELETEd covered L0s over HTTP, still have {l0:?}"
    );

    // A fresh replica over HTTP restores the maintained bucket.
    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(&replica_path, http_client(&srv), ReplicaOptions::default());
    let r = rep.sync().unwrap();
    assert!(r.restored);
    assert_eq!(r.to_txid, Txid(10));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));

    // An explicit snapshot also pushes over HTTP: one more txid so the new
    // L9 is distinguishable from the one maintain wrote at txid 10.
    conn.execute("INSERT INTO t (v) VALUES ('p2-extra')", []).unwrap();
    w.push().unwrap();
    assert_eq!(w.snapshot().unwrap(), Some(Txid(11)));
    let l9 = dir.ltx_files(SNAPSHOT_LEVEL, Txid(0), false).unwrap();
    assert!(
        l9.iter().any(|f| f.max_txid == Txid(11)),
        "explicit snapshot must have PUT an L9 file at txid 11, got {l9:?}"
    );

    let r = rep.sync().unwrap();
    assert_eq!(r.to_txid, Txid(11));
    srv.shutdown();
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

/// P3 (oracle gate): a bucket written entirely THROUGH liters HTTP —
/// pushes, compaction, retention, snapshot — must remain litestream-exact:
/// stock Go `litestream restore` reproduces the source rows.
#[test]
fn pushed_bucket_restores_with_stock_litestream() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(true),
    )
    .unwrap();

    let mut w = Writer::open(&db_path, http_client(&srv), WriterOptions::default()).unwrap();
    for batch in 0..3 {
        insert_rows(&conn, &format!("p3-{batch}"), 30);
        w.push().unwrap();
    }
    w.maintain(&aggressive_maintenance()).unwrap();
    // One more push after maintenance so the restore spans L1 + fresh L0.
    insert_rows(&conn, "p3-post", 30);
    w.push().unwrap();

    srv.shutdown(); // the bucket at rest is what stock litestream sees

    let restored = tmp.path().join("restored.db");
    litestream_restore(&oracle, &bucket, &restored);
    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 120);
    assert_eq!(rows_of(&restored), expect);
}

/// P4: the writable server is a relay — the pusher PUTs L0s in over HTTP
/// while a downstream follower streams them out of /stream, converging on
/// the pusher's final remote txid.
#[test]
fn relay_push_in_stream_out() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(true),
    )
    .unwrap();
    let mut w = Writer::open(&db_path, http_client(&srv), WriterOptions::default()).unwrap();

    let follower_path = tmp.path().join("follower.db");
    let mut rep = Replica::open(&follower_path, http_client(&srv), ReplicaOptions::default());

    let stop = AtomicBool::new(false);
    let fo = follow_opts(None);
    let (final_txid, converged, res) = std::thread::scope(|s| {
        let follower = s.spawn(|| rep.follow(&stop, &fo));
        let _stop_guard = StopGuard(&stop);

        // Exactly 20 bounded pushes, one insert each.
        let mut last = Txid(0);
        for i in 0..20 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("p4-{i}")]).unwrap();
            last = w.push().unwrap().remote_txid;
            std::thread::sleep(Duration::from_millis(10));
        }

        let converged = poll_until(Duration::from_secs(60), &follower, || {
            sidecar_txid(&follower_path) == last.0
        });
        stop.store(true, Ordering::SeqCst);
        (last, converged, follower.join().expect("follower thread panicked"))
    });
    srv.shutdown();

    res.expect("follow returned an error");
    assert_eq!(final_txid, Txid(20), "20 pushes must end at remote txid 20");
    assert!(converged, "follower never reached the pusher's final txid 20 within 60s");
    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 20);
    assert_eq!(rows_of(&follower_path), expect);
}

/// P5: loopback self-follow — the receiving process points a Replica at its
/// OWN server URL for a live local materialization of whatever the remote
/// pusher sends in.
#[test]
fn receiver_self_follow_loopback() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(true),
    )
    .unwrap();
    let mut w = Writer::open(&db_path, http_client(&srv), WriterOptions::default()).unwrap();

    // The receiver's own materialized copy, fed through its own server.
    let local_copy = tmp.path().join("local_copy.db");
    let mut rep = Replica::open(
        &local_copy,
        Box::new(HttpReplicaClient::new(url_of(&srv)).unwrap()),
        ReplicaOptions::default(),
    );

    let stop = AtomicBool::new(false);
    let fo = follow_opts(None);
    let (final_txid, converged, res) = std::thread::scope(|s| {
        let follower = s.spawn(|| rep.follow(&stop, &fo));
        let _stop_guard = StopGuard(&stop);

        let mut last = Txid(0);
        for i in 0..15 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("p5-{i}")]).unwrap();
            last = w.push().unwrap().remote_txid;
            std::thread::sleep(Duration::from_millis(10));
        }

        let converged = poll_until(Duration::from_secs(60), &follower, || {
            sidecar_txid(&local_copy) == last.0
        });
        stop.store(true, Ordering::SeqCst);
        (last, converged, follower.join().expect("follower thread panicked"))
    });
    srv.shutdown();

    res.expect("self-follow returned an error");
    assert_eq!(final_txid, Txid(15), "15 pushes must end at remote txid 15");
    assert!(converged, "loopback follower never converged within 60s");
    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 15);
    assert_eq!(rows_of(&local_copy), expect);
}

/// P6: dropping the pusher and reopening on the same source db with a FRESH
/// HttpReplicaClient resumes cleanly — Writer::open's check_behind_remote
/// lists the bucket over HTTP and subsequent pushes continue the chain.
#[test]
fn writer_reopen_resumes_over_http() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(true),
    )
    .unwrap();

    {
        let mut w = Writer::open(&db_path, http_client(&srv), WriterOptions::default()).unwrap();
        let mut last = Txid(0);
        for i in 0..5 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("p6-a-{i}")]).unwrap();
            last = w.push().unwrap().remote_txid;
        }
        assert_eq!(last, Txid(5));
    } // writer dropped entirely

    let mut w = Writer::open(&db_path, http_client(&srv), WriterOptions::default()).unwrap();
    let mut last = Txid(0);
    for i in 0..5 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("p6-b-{i}")]).unwrap();
        last = w.push().unwrap().remote_txid;
    }
    assert_eq!(last, Txid(10), "reopened writer must continue the chain at txid 6..10");

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(&replica_path, http_client(&srv), ReplicaOptions::default());
    let r = rep.sync().unwrap();
    assert!(r.restored);
    assert_eq!(r.to_txid, Txid(10));

    srv.shutdown();
    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 10);
    assert_eq!(rows_of(&replica_path), expect);
}

/// P7: pushing to a read-only server fails cleanly — the error names the
/// cause, the bucket stays empty, the source db is unharmed — and reopening
/// the writer (same db, hence same meta dir) against a writable server later
/// uploads the stranded L0 along with new data.
#[test]
fn push_to_read_only_server_fails_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut ro = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(false), // read-only (the default posture)
    )
    .unwrap();

    // Opening works: Writer::open only reads (listings) over HTTP.
    let mut w = Writer::open(&db_path, http_client(&ro), WriterOptions::default()).unwrap();
    insert_rows(&conn, "p7-a", 8);
    let err = w.push().expect_err("push to a read-only server must fail");
    let msg = err.to_string();
    assert!(msg.contains("read-only"), "error must name the cause, got: {msg}");

    // The bucket stayed empty at every level.
    let dir = DirReplicaClient::new(&bucket);
    for level in 0..=SNAPSHOT_LEVEL {
        let files = dir.ltx_files(level, Txid(0), false).unwrap();
        assert!(files.is_empty(), "level {level} must stay empty, got {files:?}");
    }

    // The source database is unharmed.
    assert_eq!(rows_of(&db_path).len(), 8);

    drop(w);
    ro.shutdown();

    // Reopen against a writable server over the same bucket. The meta dir is
    // derived from the db path, so the L0 the failed push stranded locally
    // (txid 1) uploads together with the new batch (txid 2).
    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(true),
    )
    .unwrap();
    let mut w = Writer::open(&db_path, http_client(&srv), WriterOptions::default()).unwrap();
    insert_rows(&conn, "p7-b", 4);
    let r = w.push().unwrap();
    assert_eq!(r.remote_txid, Txid(2), "stranded L0 (txid 1) + new batch (txid 2)");

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(&replica_path, http_client(&srv), ReplicaOptions::default());
    let r = rep.sync().unwrap();
    assert!(r.restored);
    assert_eq!(r.to_txid, Txid(2));

    srv.shutdown();
    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 12);
    assert_eq!(rows_of(&replica_path), expect);
}
