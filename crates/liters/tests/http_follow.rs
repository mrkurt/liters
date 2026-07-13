//! End-to-end tests for liters-native HTTP replication: Writer -> HttpServer
//! -> Replica (restore, incremental sync, live streaming follow, gap/reset
//! handling, server restarts). The HTTP protocol is liters-proprietary
//! (docs/http-protocol.md), so unlike the oracle suites nothing here gates
//! on the Go binaries.
//!
//! Conventions: all DB load is bounded (fixed row counts, never
//! time-conditioned inserts); servers bind 127.0.0.1:0; no latency
//! assertions, only eventual convergence within generous deadlines polled at
//! 50ms; every wait has a deadline that fails the test instead of hanging.
//! Follower threads run under std::thread::scope with a StopGuard so a
//! panicking test body can never leave a follow() thread blocking the scope
//! join. Replica row contents are only read after the follower thread has
//! been stopped and joined (fcntl locks do not exclude same-process
//! readers); mid-run progress is observed through the atomically-renamed
//! `{db}-txid` sidecar only.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::ScopedJoinHandle;
use std::time::{Duration, Instant};

use liters::{
    Backoff, CancelToken, DirReplicaClient, FollowOptions, LtxStream, MaintenanceOptions, Replica,
    ReplicaClient, ReplicaOptions, StreamEvent, Writer, WriterOptions,
};
use liters_storage::{HttpReplicaClient, HttpServer, HttpServerOptions};
use ltx::Txid;
use rusqlite::Connection;

const POLL: Duration = Duration::from_millis(50);

fn fast_server() -> HttpServerOptions {
    HttpServerOptions {
        poll_interval: Duration::from_millis(100),
        ping_interval: Duration::from_millis(300),
        ..HttpServerOptions::default()
    }
}

/// Fixed-delay backoff (no growth, no jitter) so retrying tests keep a
/// deterministic cadence.
fn follow_opts(retry: Option<Duration>) -> FollowOptions {
    FollowOptions {
        poll_interval: Duration::from_millis(100),
        retry: retry.map(|d| Backoff { initial: d, max: d, multiplier: 1.0, jitter: 0.0 }),
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

/// `(id, length(v))` per row — shape comparison that keeps assertion output
/// small when a row holds megabytes of text.
fn shape_of(path: &Path) -> Vec<(i64, i64)> {
    let conn =
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok");
    let mut stmt = conn.prepare("SELECT id, length(v) FROM t ORDER BY id").unwrap();
    stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

/// FNV-1a over one row's text: content equality without huge assert output.
fn text_sig(path: &Path, id: i64) -> (usize, u64) {
    let conn =
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let v: String = conn.query_row("SELECT v FROM t WHERE id = ?1", [id], |r| r.get(0)).unwrap();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in v.as_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    (v.len(), h)
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

/// Cancels the follow() token on drop, so a panic anywhere in a test's
/// scope body can never leave the follower thread running forever (which
/// would hang the scope join, and CI).
struct StopGuard<'a>(&'a CancelToken);

impl Drop for StopGuard<'_> {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Pumps a stream until a non-Idle event arrives; panics after `deadline`.
/// (`LtxStream::next` itself ticks at ~1s granularity, so the loop is
/// bounded.)
fn next_non_idle(stream: &mut dyn LtxStream, deadline: Duration) -> StreamEvent {
    let end = Instant::now() + deadline;
    let mut sink = std::io::sink();
    while Instant::now() < end {
        match stream.next(&mut sink).unwrap() {
            StreamEvent::Idle { .. } => continue,
            ev => return ev,
        }
    }
    panic!("no non-idle stream event within {deadline:?}");
}

// ---------------------------------------------------------------------------

/// T1: a full restore over HTTP produces byte-identical rows to a restore
/// straight off the dir bucket, and to the source database.
#[test]
fn restore_over_http_matches_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for batch in 0..3 {
        insert_rows(&conn, &format!("t1-{batch}"), 50);
        w.push().unwrap();
    }

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(),
    )
    .unwrap();

    let a_path = tmp.path().join("replica_a.db");
    let mut a = Replica::open(
        &a_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    let ra = a.sync().unwrap();
    assert!(ra.restored);
    assert_eq!(ra.to_txid, Txid(3));

    let b_path = tmp.path().join("replica_b.db");
    let mut b = Replica::open(&b_path, http_client(&srv), ReplicaOptions::default());
    let rb = b.sync().unwrap();
    assert!(rb.restored);
    assert_eq!(rb.to_txid, Txid(3));

    srv.shutdown();

    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 150);
    assert_eq!(rows_of(&a_path), expect);
    assert_eq!(rows_of(&b_path), expect);
}

/// T2: after the initial restore, new pushes are applied incrementally over
/// HTTP (no re-restore), advancing the position.
#[test]
fn incremental_sync_over_http() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    insert_rows(&conn, "t2-0", 30);
    w.push().unwrap();

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(),
    )
    .unwrap();

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(&replica_path, http_client(&srv), ReplicaOptions::default());
    let r = rep.sync().unwrap();
    assert!(r.restored);
    assert_eq!(r.to_txid, Txid(1));

    for batch in 1..3 {
        insert_rows(&conn, &format!("t2-{batch}"), 20);
        w.push().unwrap();
    }

    let r = rep.sync().unwrap();
    assert!(!r.restored, "incremental sync over http must not re-restore");
    assert_eq!(r.from_txid, Txid(1));
    assert_eq!(r.to_txid, Txid(3));

    srv.shutdown();
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

/// T3: live streaming follow — writer pushes through the notifying tee, the
/// follower applies frames from /stream as they arrive, and the position
/// sidecar advances (well-formed, monotone) throughout.
#[test]
fn streaming_follow_live() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(),
    )
    .unwrap();
    let mut w = Writer::open(
        &db_path,
        srv.notifying_client(Box::new(DirReplicaClient::new(&bucket))),
        WriterOptions::default(),
    )
    .unwrap();

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(&replica_path, http_client(&srv), ReplicaOptions::default());

    let stop = CancelToken::new();
    let final_txid = AtomicU64::new(0);
    let writer_done = AtomicBool::new(false);
    let mut sidecar_seen: Vec<String> = Vec::new();

    let fo = follow_opts(None);
    let (converged, follow_res) = std::thread::scope(|s| {
        let follower = s.spawn(|| rep.follow(&stop, &fo));
        let _stop_guard = StopGuard(&stop);

        let writer = s.spawn({
            let final_txid = &final_txid;
            let writer_done = &writer_done;
            move || {
                // Exactly 30 bounded pushes, one insert each.
                let mut last = Txid(0);
                for i in 0..30 {
                    conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("live-{i}")])
                        .unwrap();
                    last = w.push().unwrap().remote_txid;
                    std::thread::sleep(Duration::from_millis(10));
                }
                final_txid.store(last.0, Ordering::SeqCst);
                writer_done.store(true, Ordering::SeqCst);
            }
        });

        // Observe the raw sidecar mid-run (validated after the joins so a
        // malformed read can't panic while threads are live).
        let converged = poll_until(Duration::from_secs(60), &follower, || {
            if let Ok(raw) = std::fs::read_to_string(sidecar_path(&replica_path)) {
                if sidecar_seen.last() != Some(&raw) {
                    sidecar_seen.push(raw);
                }
            }
            let f = final_txid.load(Ordering::SeqCst);
            writer_done.load(Ordering::SeqCst) && f != 0 && sidecar_txid(&replica_path) == f
        });

        stop.cancel();
        let follow_res = follower.join().expect("follower thread panicked");
        writer.join().expect("writer thread panicked");
        (converged, follow_res)
    });

    srv.shutdown();

    follow_res.expect("follow returned an error");
    assert_eq!(final_txid.load(Ordering::SeqCst), 30, "30 pushes must end at txid 30");
    assert!(converged, "follower never reached the writer's final txid 30 within 60s");

    // Record the settled sidecar too, then validate every observation:
    // 16 lowercase hex digits + newline, monotonically non-decreasing.
    if let Ok(raw) = std::fs::read_to_string(sidecar_path(&replica_path)) {
        if sidecar_seen.last() != Some(&raw) {
            sidecar_seen.push(raw);
        }
    }
    assert!(!sidecar_seen.is_empty(), "position sidecar was never observed during streaming");
    let mut prev = 0u64;
    for raw in &sidecar_seen {
        assert_eq!(raw.len(), 17, "sidecar must be 16 hex digits + newline, got {raw:?}");
        assert!(raw.ends_with('\n'), "sidecar must end in a newline: {raw:?}");
        let hex = &raw[..16];
        assert!(
            hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "sidecar is not 16 lowercase hex digits: {raw:?}"
        );
        let v = u64::from_str_radix(hex, 16).unwrap();
        assert!(v >= prev, "sidecar position went backwards: {prev} -> {v}");
        prev = v;
    }
    assert_eq!(prev, 30, "final sidecar position");

    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 30);
    assert_eq!(rows_of(&replica_path), expect);
}

/// T4: after L1 compaction prunes L0, /stream announces the gap to a
/// below-range seek; a fresh replica still restores from scratch, and a
/// follower positioned inside the pruned range bridges the gap via sync()'s
/// L1 logic under follow().
#[test]
fn gap_bridged_after_l0_prune() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for i in 0..5 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("t4-a-{i}")]).unwrap();
        w.push().unwrap();
    }

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(),
    )
    .unwrap();

    // A follower synced mid-history; the L0 files covering its position are
    // about to be pruned.
    let bridge_path = tmp.path().join("bridge.db");
    let mut bridge = Replica::open(&bridge_path, http_client(&srv), ReplicaOptions::default());
    assert_eq!(bridge.sync().unwrap().to_txid, Txid(5));

    for i in 0..5 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("t4-b-{i}")]).unwrap();
        w.push().unwrap();
    }

    // Compact L0 into L1 now and prune L0 immediately (zero grace).
    let report = w
        .maintain(&MaintenanceOptions {
            level_intervals: vec![Duration::ZERO],
            snapshot_interval: Duration::from_secs(100_000),
            snapshot_retention: Duration::from_secs(24 * 60 * 60),
            l0_retention: Duration::ZERO,
            retention_enabled: true,
        })
        .unwrap();
    assert!(report.compacted_levels.contains(&1), "expected an L1 compaction, got {report:?}");

    let dir = DirReplicaClient::new(&bucket);
    let l0 = dir.ltx_files(0, Txid(0), false).unwrap();
    assert_eq!(l0.len(), 1, "L0 should be pruned to the newest file, got {l0:?}");
    let oldest_l0_min = l0[0].min_txid;
    assert!(oldest_l0_min > Txid(1), "pruning left the chain intact, got {l0:?}");

    // Direct stream contract: a seek below the pruned range yields Gap with
    // the oldest surviving L0 min.
    let http = HttpReplicaClient::new(url_of(&srv)).unwrap();
    let mut stream = http
        .open_ltx_stream(Txid(1))
        .unwrap()
        .expect("http backend must support streaming");
    match next_non_idle(stream.as_mut(), Duration::from_secs(15)) {
        StreamEvent::Gap { next } => assert_eq!(next, oldest_l0_min),
        ev => panic!("expected Gap, got {ev:?}"),
    }
    drop(stream);

    // A fresh replica restores from scratch across the pruned range.
    let fresh_path = tmp.path().join("fresh.db");
    let mut fresh = Replica::open(&fresh_path, http_client(&srv), ReplicaOptions::default());
    let r = fresh.sync().unwrap();
    assert!(r.restored);
    assert_eq!(r.to_txid, Txid(10));
    assert_eq!(rows_of(&fresh_path), rows_of(&db_path));

    // The mid-history follower converges via follow(): its first stream open
    // hits the gap, which routes through sync()'s L1 bridge.
    let stop = CancelToken::new();
    let fo = follow_opts(None);
    let (converged, res) = std::thread::scope(|s| {
        let follower = s.spawn(|| bridge.follow(&stop, &fo));
        let _stop_guard = StopGuard(&stop);
        let converged =
            poll_until(Duration::from_secs(60), &follower, || sidecar_txid(&bridge_path) == 10);
        stop.cancel();
        (converged, follower.join().expect("follower thread panicked"))
    });
    srv.shutdown();

    res.expect("follow returned an error");
    assert!(converged, "follower never bridged the pruned range within 60s");
    assert_eq!(rows_of(&bridge_path), rows_of(&db_path));
}

/// T5: a wiped-and-reseeded bucket is detected — /stream answers an
/// above-range seek with Reset, and an auto_reset follower re-restores onto
/// the new history under follow().
#[test]
fn reseed_detected_and_auto_reset() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn_a, db_a, bucket) = setup(tmp.path());

    let mut wa = Writer::open(
        &db_a,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for i in 0..20 {
        conn_a.execute("INSERT INTO t (v) VALUES (?1)", [format!("a-{i}")]).unwrap();
        wa.push().unwrap();
    }
    drop(wa);

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(),
    )
    .unwrap();

    // Phase 1: follow to position 20, then stop.
    let replica_path = tmp.path().join("replica.db");
    {
        let mut rep = Replica::open(&replica_path, http_client(&srv), ReplicaOptions::default());
        let stop = CancelToken::new();
        let fo = follow_opts(None);
        let (converged, res) = std::thread::scope(|s| {
            let follower = s.spawn(|| rep.follow(&stop, &fo));
            let _stop_guard = StopGuard(&stop);
            let converged = poll_until(Duration::from_secs(60), &follower, || {
                sidecar_txid(&replica_path) == 20
            });
            stop.cancel();
            (converged, follower.join().expect("phase-1 follower panicked"))
        });
        res.expect("phase-1 follow errored");
        assert!(converged, "phase-1 follower never reached txid 20");
    }
    assert_eq!(rows_of(&replica_path).len(), 20);

    // Reseed: wipe the bucket, then a brand-new database pushes 3 rows.
    DirReplicaClient::new(&bucket).delete_all().unwrap();
    let db_b = tmp.path().join("app_b.db");
    let conn_b = create_db(&db_b);
    let mut wb = Writer::open(
        &db_b,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for i in 0..3 {
        conn_b.execute("INSERT INTO t (v) VALUES (?1)", [format!("b-{i}")]).unwrap();
        wb.push().unwrap();
    }

    // Direct stream contract: seek above the reseeded bucket yields Reset
    // carrying the new bucket max.
    let http = HttpReplicaClient::new(url_of(&srv)).unwrap();
    let mut stream = http
        .open_ltx_stream(Txid(21))
        .unwrap()
        .expect("http backend must support streaming");
    match next_non_idle(stream.as_mut(), Duration::from_secs(15)) {
        StreamEvent::Reset { bucket_max } => assert_eq!(bucket_max, Txid(3)),
        ev => panic!("expected Reset, got {ev:?}"),
    }
    drop(stream);

    // Phase 2: the stale follower (position 20) with auto_reset converges
    // onto writer B's history.
    let mut rep = Replica::open(
        &replica_path,
        http_client(&srv),
        ReplicaOptions { auto_reset: true, ..Default::default() },
    );
    let stop = CancelToken::new();
    let fo = follow_opts(Some(Duration::from_millis(100)));
    let (converged, res) = std::thread::scope(|s| {
        let follower = s.spawn(|| rep.follow(&stop, &fo));
        let _stop_guard = StopGuard(&stop);
        let converged =
            poll_until(Duration::from_secs(60), &follower, || sidecar_txid(&replica_path) == 3);
        stop.cancel();
        (converged, follower.join().expect("phase-2 follower panicked"))
    });
    srv.shutdown();

    res.expect("phase-2 follow errored");
    assert!(converged, "auto_reset follower never converged onto the reseeded bucket");
    let rows = rows_of(&replica_path);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows, rows_of(&db_b));
}

/// T6: the follower (with retry) survives a full server restart on the same
/// port, catching up on rows pushed while the server was down.
#[test]
fn server_restart_mid_follow() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();

    let mut srv1 = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(),
    )
    .unwrap();
    let port = srv1.local_addr().port();

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(HttpReplicaClient::new(format!("http://127.0.0.1:{port}")).unwrap()),
        ReplicaOptions::default(),
    );

    let stop = CancelToken::new();
    let fo = follow_opts(Some(Duration::from_millis(200)));
    let outcome = std::thread::scope(|s| {
        let follower = s.spawn(|| rep.follow(&stop, &fo));
        let _stop_guard = StopGuard(&stop);

        for i in 0..10 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("t6-a-{i}")]).unwrap();
            w.push().unwrap();
        }
        let caught_up =
            poll_until(Duration::from_secs(30), &follower, || sidecar_txid(&replica_path) == 10);

        srv1.shutdown();

        // The bucket keeps advancing while the server is down.
        for i in 0..10 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("t6-b-{i}")]).unwrap();
            w.push().unwrap();
        }

        // Rebind the same port, retrying (AddrInUse etc.) for up to 10s.
        let mut srv2 = None;
        let bind_deadline = Instant::now() + Duration::from_secs(10);
        while srv2.is_none() && Instant::now() < bind_deadline {
            match HttpServer::bind(
                ("127.0.0.1", port),
                Arc::new(DirReplicaClient::new(&bucket)),
                fast_server(),
            ) {
                Ok(srv) => srv2 = Some(srv),
                Err(_) => std::thread::sleep(Duration::from_millis(100)),
            }
        }
        let Some(mut srv2) = srv2 else {
            eprintln!("SKIP: port {port} stolen; cannot rebind for the restart test");
            stop.cancel();
            let _ = follower.join();
            return None;
        };

        let converged =
            poll_until(Duration::from_secs(60), &follower, || sidecar_txid(&replica_path) == 20);
        stop.cancel();
        let res = follower.join().expect("follower thread panicked");
        srv2.shutdown();
        Some((caught_up, converged, res))
    });

    let Some((caught_up, converged, res)) = outcome else {
        return; // skipped: could not reclaim the port
    };
    res.expect("follow errored despite retry across the restart");
    assert!(caught_up, "follower never caught up before the restart");
    assert!(converged, "follower never converged after the server restart");
    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 20);
    assert_eq!(rows_of(&replica_path), expect);
}

/// T7: four concurrent full restores over HTTP against a bucket with a
/// snapshot all succeed with identical rows.
#[test]
fn concurrent_full_restores() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for batch in 0..4 {
        insert_rows(&conn, &format!("t7-{batch}"), 50);
        w.push().unwrap();
    }
    assert_eq!(w.snapshot().unwrap(), Some(Txid(4)));

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(),
    )
    .unwrap();
    let url = url_of(&srv);

    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 200);

    std::thread::scope(|s| {
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let url = url.clone();
                let path = tmp.path().join(format!("replica-{i}.db"));
                s.spawn(move || {
                    let mut rep = Replica::open(
                        &path,
                        Box::new(HttpReplicaClient::new(&url).unwrap()),
                        ReplicaOptions::default(),
                    );
                    let r = rep.sync().unwrap();
                    assert!(r.restored);
                    assert_eq!(r.to_txid, Txid(4));
                    rows_of(&path)
                })
            })
            .collect();
        for h in handles {
            let rows = h.join().expect("restore thread panicked");
            assert_eq!(rows, expect);
        }
    });
    srv.shutdown();
}

/// T8: no notifying tee — an "external" writer pushes straight to the dir
/// bucket while the server serves the same directory; streamers pick the
/// changes up via the server's poll re-listing.
#[test]
fn external_writer_poll_path() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(),
    )
    .unwrap();

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(&replica_path, http_client(&srv), ReplicaOptions::default());

    let stop = CancelToken::new();
    let fo = follow_opts(None);
    let (converged, res) = std::thread::scope(|s| {
        let follower = s.spawn(|| rep.follow(&stop, &fo));
        let _stop_guard = StopGuard(&stop);

        for i in 0..10 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("t8-{i}")]).unwrap();
            w.push().unwrap();
            std::thread::sleep(Duration::from_millis(10));
        }

        let converged =
            poll_until(Duration::from_secs(30), &follower, || sidecar_txid(&replica_path) == 10);
        stop.cancel();
        (converged, follower.join().expect("follower thread panicked"))
    });
    srv.shutdown();

    res.expect("follow returned an error");
    assert!(converged, "follower never converged via the poll path within 30s");
    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 10);
    assert_eq!(rows_of(&replica_path), expect);
}

/// T9: a single commit carrying a 2 MiB value streams as one large frame to
/// an already-following replica; contents compared by length + checksum.
#[test]
fn big_single_commit_frame() {
    let tmp = tempfile::tempdir().unwrap();
    let (conn, db_path, bucket) = setup(tmp.path());

    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(&bucket)),
        fast_server(),
    )
    .unwrap();
    let mut w = Writer::open(
        &db_path,
        srv.notifying_client(Box::new(DirReplicaClient::new(&bucket))),
        WriterOptions::default(),
    )
    .unwrap();

    conn.execute("INSERT INTO t (id, v) VALUES (1, 'seed')", []).unwrap();
    w.push().unwrap();

    // Exactly 2 MiB of varied text, built before any thread runs.
    let big: String = (0..(2 * 1024 * 1024 / 16)).map(|i| format!("{i:015x}\n")).collect();
    assert_eq!(big.len(), 2 * 1024 * 1024);

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(&replica_path, http_client(&srv), ReplicaOptions::default());

    let stop = CancelToken::new();
    let fo = follow_opts(None);
    let (streamed, converged, res) = std::thread::scope(|s| {
        let follower = s.spawn(|| rep.follow(&stop, &fo));
        let _stop_guard = StopGuard(&stop);

        // Follower is live at txid 1 before the big commit lands.
        let streamed =
            poll_until(Duration::from_secs(30), &follower, || sidecar_txid(&replica_path) == 1);

        conn.execute("INSERT INTO t (id, v) VALUES (2, ?1)", [big.as_str()]).unwrap();
        w.push().unwrap();

        let converged =
            poll_until(Duration::from_secs(60), &follower, || sidecar_txid(&replica_path) == 2);
        stop.cancel();
        (streamed, converged, follower.join().expect("follower thread panicked"))
    });
    srv.shutdown();

    res.expect("follow returned an error");
    assert!(streamed, "follower never reached txid 1 before the big commit");
    assert!(converged, "follower never applied the 2 MiB frame within 60s");

    assert_eq!(shape_of(&replica_path), shape_of(&db_path));
    let src = text_sig(&db_path, 2);
    assert_eq!(src.0, 2 * 1024 * 1024);
    assert_eq!(text_sig(&replica_path, 2), src, "2 MiB row content diverged (len, fnv1a)");
}
