//! Conformance suite: ports of litestream's integration-test scenarios
//! (tests/integration/{ltx_behavior,boundary,concurrent,compatibility}) plus
//! mobile-lifecycle cases the Go daemon never faces (writer reopened every
//! app launch).

use std::path::{Path, PathBuf};
use std::process::Command;

use liters::{DirReplicaClient, Replica, ReplicaClient, ReplicaOptions, Writer, WriterOptions};
use ltx::Txid;
use rusqlite::Connection;

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

fn litestream_restore(oracle: &Path, bucket: &Path, out: &Path, extra: &[&str]) {
    let _ = std::fs::remove_file(out);
    let mut args =
        vec!["restore".to_string(), "-o".into(), out.to_str().unwrap().into()];
    args.extend(extra.iter().map(|s| s.to_string()));
    args.push(format!("file://{}", bucket.display()));
    let output = Command::new(oracle.join("litestream")).args(&args).output().unwrap();
    assert!(
        output.status.success(),
        "litestream restore {args:?} failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Counts L0 files that are full images (page count == commit): only the
/// very first push should produce one. (TestLTXBehavior_NoExcessiveSnapshots)
fn count_full_image_l0s(bucket: &Path) -> usize {
    let client = DirReplicaClient::new(bucket);
    let mut n = 0;
    for info in client.ltx_files(0, Txid(0), false).unwrap() {
        let mut rd = client.open_ltx_file(0, info.min_txid, info.max_txid, 0, 0).unwrap();
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut rd, &mut buf).unwrap();
        let dec = ltx::Decoder::new(std::io::Cursor::new(&buf));
        let (hdr, _, index) = dec.verify().unwrap();
        // Full image: covers every page up to commit (lock page excepted).
        if index.len() as u32 >= hdr.commit.saturating_sub(1) && hdr.commit > 1 {
            n += 1;
        }
    }
    n
}

/// Page-size matrix: the full write→restore→follow cycle must work for every
/// SQLite page size. (TestLockPageWithDifferentPageSizes, scaled down)
#[test]
fn page_size_matrix() {
    let Some(oracle) = oracle_dir() else { return };
    for page_size in [512u32, 8192, 32768] {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("app.db");
        let bucket = tmp.path().join("bucket");

        let app = Connection::open(&db_path).unwrap();
        app.pragma_update(None, "page_size", page_size).unwrap();
        app.pragma_update(None, "journal_mode", "WAL").unwrap();
        app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();

        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        assert_eq!(w.page_size(), page_size, "writer picked up page size");

        for i in 0..4 {
            for j in 0..25 {
                app.execute(
                    "INSERT INTO t (v) VALUES (?1)",
                    [format!("ps{page_size}-{i}-{j}-{}", "y".repeat(100))],
                )
                .unwrap();
            }
            w.push().unwrap();
        }

        // Oracle restore.
        let restored = tmp.path().join("restored.db");
        litestream_restore(&oracle, &bucket, &restored, &[]);
        assert_eq!(rows_of(&restored), rows_of(&db_path), "page_size={page_size}");

        // Rust replica follow.
        let replica_path = tmp.path().join("replica.db");
        let mut rep = Replica::open(
            &replica_path,
            Box::new(DirReplicaClient::new(&bucket)),
            ReplicaOptions::default(),
        );
        rep.sync().unwrap();
        app.execute("INSERT INTO t (v) VALUES ('tail')", []).unwrap();
        w.push().unwrap();
        rep.sync().unwrap();
        assert_eq!(rows_of(&replica_path), rows_of(&db_path), "page_size={page_size}");
    }
}

/// Point-in-time restore: `litestream restore -txid N` against a liters
/// bucket must reproduce the exact state at push N.
/// (TestRestore_PointInTimeAccuracy)
#[test]
fn point_in_time_restore_accuracy() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let app = Connection::open(&db_path).unwrap();
    app.pragma_update(None, "journal_mode", "WAL").unwrap();
    app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();

    let mut states: Vec<(Txid, Vec<(i64, String)>)> = Vec::new();
    for i in 0..5 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("pit-{i}")]).unwrap();
        let r = w.push().unwrap();
        states.push((r.txid, rows_of(&db_path)));
    }

    for (txid, expect) in &states {
        let out = tmp.path().join(format!("pit-{txid}.db"));
        litestream_restore(&oracle, &bucket, &out, &["-txid", &txid.to_string()]);
        assert_eq!(&rows_of(&out), expect, "state at txid {txid}");
    }
}

/// Only the first push may be a full image; heavy checkpointing must never
/// degrade subsequent pushes into snapshots.
/// (TestLTXBehavior_NoExcessiveSnapshots + TestRapidCheckpoints)
#[test]
fn no_excessive_snapshots_under_rapid_checkpoints() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let app = Connection::open(&db_path).unwrap();
    app.pragma_update(None, "journal_mode", "WAL").unwrap();
    app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for i in 0..200 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("base-{i}-{}", "z".repeat(64))])
            .unwrap();
    }

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    w.push().unwrap();

    let mut first_cycle_max_txid = Txid(0);
    for i in 0..10 {
        w.checkpoint(if i % 2 == 0 {
            liters::CheckpointMode::Passive
        } else {
            liters::CheckpointMode::Truncate
        })
        .unwrap();
        app.execute("UPDATE t SET v = ?1 WHERE id = ?2", [format!("u{i}"), (i + 1).to_string()])
            .unwrap();
        let r = w.push().unwrap();
        if i == 1 {
            first_cycle_max_txid = r.txid;
        }
    }

    // Faithful-to-Go bound: besides the initial snapshot, ONE extra
    // full-image L0 is permitted on the first TRUNCATE after a PASSIVE
    // restart. The stale WAL tail left by the restart makes
    // synced_to_wal_end false (a file-size comparison, db.go:2044), so the
    // TRUNCATE's post-copy takes the "truncated by another process" branch —
    // Go behaves identically (safe: extra snapshot, never loss). Steady-state
    // cycles must stay incremental.
    let full_images = count_full_image_l0s(&bucket);
    assert!(
        full_images <= 2,
        "rapid checkpoints degraded incremental pushes into snapshots ({full_images} full images)"
    );

    // Every L0 after the first PASSIVE+TRUNCATE cycle must be a small delta.
    let client = DirReplicaClient::new(&bucket);
    for info in client.ltx_files(0, Txid(0), false).unwrap() {
        if info.min_txid <= first_cycle_max_txid {
            continue;
        }
        let mut rd = client.open_ltx_file(0, info.min_txid, info.max_txid, 0, 0).unwrap();
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut rd, &mut buf).unwrap();
        let dec = ltx::Decoder::new(std::io::Cursor::new(&buf));
        let (hdr, _, index) = dec.verify().unwrap();
        assert!(
            (index.len() as u32) < hdr.commit / 2,
            "steady-state push {} snapshotted: {} pages of commit {}",
            info.min_txid,
            index.len(),
            hdr.commit
        );
    }
}

/// Mobile lifecycle: the writer is dropped and reopened between pushes (as on
/// every app launch). Content must stay correct and pushes incremental.
#[test]
fn writer_reopen_across_launches() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let app = Connection::open(&db_path).unwrap();
    app.pragma_update(None, "journal_mode", "WAL").unwrap();
    app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for i in 0..100 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("base-{i}")]).unwrap();
    }

    for launch in 0..5 {
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("launch-{launch}")]).unwrap();
        w.push().unwrap();
        drop(w); // read lock released, as on app termination
    }

    // Reopen cost check: after the first push, relaunches must not have
    // produced full-image L0s (nobody checkpointed while closed).
    assert_eq!(count_full_image_l0s(&bucket), 1, "relaunches caused snapshot pushes");

    let restored = tmp.path().join("restored.db");
    litestream_restore(&oracle, &bucket, &restored, &[]);
    assert_eq!(rows_of(&restored), rows_of(&db_path));
}

/// Mobile lifecycle, hostile variant: while the writer is closed, the app
/// itself checkpoints (TRUNCATE) — exactly what the persisted-state shortcut
/// must NOT paper over. The next push must detect the truncation, snapshot,
/// and lose nothing.
#[test]
fn app_checkpoint_while_writer_closed_forces_snapshot_not_loss() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let app = Connection::open(&db_path).unwrap();
    app.pragma_update(None, "journal_mode", "WAL").unwrap();
    app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    app.execute("INSERT INTO t (v) VALUES ('a')", []).unwrap();

    {
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        w.push().unwrap();
    } // writer closed; read lock gone

    // App writes AND checkpoints while liters is not running: these commits
    // exist only in the main db file afterwards.
    app.execute("INSERT INTO t (v) VALUES ('written-while-closed')", []).unwrap();
    let _: (i64, i64, i64) = app
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    app.execute("INSERT INTO t (v) VALUES ('after-reopen')", []).unwrap();
    let r = w.push().unwrap();
    assert!(r.synced);

    let restored = tmp.path().join("restored.db");
    litestream_restore(&oracle, &bucket, &restored, &[]);
    assert_eq!(
        rows_of(&restored),
        rows_of(&db_path),
        "commits checkpointed while the writer was closed were lost"
    );
}

/// WAL growth without checkpoints (long-lived writer, many pushes below the
/// threshold), then a threshold crossing. (TestWALGrowth)
#[test]
fn wal_growth_and_recovery() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let app = Connection::open(&db_path).unwrap();
    app.pragma_update(None, "journal_mode", "WAL").unwrap();
    app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions { min_checkpoint_page_n: 50, ..Default::default() },
    )
    .unwrap();

    let mut checkpointed = false;
    for i in 0..15 {
        for j in 0..30 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("g{i}-{j}")]).unwrap();
        }
        let r = w.push().unwrap();
        checkpointed |= r.checkpointed;
    }
    assert!(checkpointed, "WAL never crossed the checkpoint threshold");

    let restored = tmp.path().join("restored.db");
    litestream_restore(&oracle, &bucket, &restored, &[]);
    assert_eq!(rows_of(&restored), rows_of(&db_path));
}

/// A snapshot taken right after an app relaunch must contain every committed
/// transaction even when the persisted sync-state file is missing or stale:
/// the WAL scan bound must come from the newest L0 header, never from the
/// advisory sync-state value. (A stale bound silently drops un-checkpointed
/// commits from a snapshot that claims to contain them, and retention then
/// makes the loss permanent.)
#[test]
fn snapshot_after_reopen_survives_missing_or_stale_sync_state() {
    let Some(oracle) = oracle_dir() else { return };

    // Trigger A: sync-state file deleted between launches.
    {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("app.db");
        let bucket = tmp.path().join("bucket");
        let app = Connection::open(&db_path).unwrap();
        app.pragma_update(None, "journal_mode", "WAL").unwrap();
        app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();

        {
            let mut w = Writer::open(
                &db_path,
                Box::new(DirReplicaClient::new(&bucket)),
                WriterOptions::default(),
            )
            .unwrap();
            for i in 0..5 {
                app.execute("INSERT INTO t (v) VALUES (?1)", [format!("a-{i}")]).unwrap();
                w.push().unwrap();
            }
        }

        let meta = tmp.path().join(".app.db-litestream");
        std::fs::remove_file(meta.join("sync-state")).unwrap();

        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        assert!(w.snapshot().unwrap().is_some());

        let restored = tmp.path().join("restored.db");
        litestream_restore(&oracle, &bucket, &restored, &[]);
        assert_eq!(rows_of(&restored), rows_of(&db_path), "sync-state deleted");
    }

    // Trigger B: sync-state one push behind (the crash window between the L0
    // rename and the sync-state write).
    {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("app.db");
        let bucket = tmp.path().join("bucket");
        let app = Connection::open(&db_path).unwrap();
        app.pragma_update(None, "journal_mode", "WAL").unwrap();
        app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();

        let meta = tmp.path().join(".app.db-litestream");
        let stale = {
            let mut w = Writer::open(
                &db_path,
                Box::new(DirReplicaClient::new(&bucket)),
                WriterOptions::default(),
            )
            .unwrap();
            app.execute("INSERT INTO t (v) VALUES ('first')", []).unwrap();
            w.push().unwrap();
            let stale = std::fs::read(meta.join("sync-state")).unwrap();
            app.execute("INSERT INTO t (v) VALUES ('second')", []).unwrap();
            w.push().unwrap();
            stale
        };
        std::fs::write(meta.join("sync-state"), &stale).unwrap();

        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        assert!(w.snapshot().unwrap().is_some());

        let restored = tmp.path().join("restored.db");
        litestream_restore(&oracle, &bucket, &restored, &[]);
        assert_eq!(rows_of(&restored), rows_of(&db_path), "sync-state one push behind");
    }
}

/// An out-of-band page-size change between writer lifetimes (journal_mode=
/// DELETE + PRAGMA page_size + VACUUM) leaves the newest L0's resume offset
/// smaller than one new-size frame. Go returns a controlled error
/// (db.go:1594); liters must do the same, not underflow. No oracle needed.
#[test]
fn page_size_change_between_launches_errors_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");

    let app = Connection::open(&db_path).unwrap();
    app.pragma_update(None, "page_size", 512).unwrap();
    app.pragma_update(None, "journal_mode", "WAL").unwrap();
    app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();

    {
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        app.execute("INSERT INTO t (v) VALUES ('small-pages')", []).unwrap();
        w.push().unwrap();
    }

    // The only legal way to change page size; deletes the old WAL.
    app.pragma_update(None, "journal_mode", "delete").unwrap();
    app.pragma_update(None, "page_size", 4096).unwrap();
    app.execute_batch("VACUUM").unwrap();
    app.pragma_update(None, "journal_mode", "WAL").unwrap();

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    app.execute("INSERT INTO t (v) VALUES ('big-pages')", []).unwrap();
    match w.push() {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("prev WAL offset is less than the header size"),
                "expected Go's controlled verify error, got: {msg}"
            );
        }
        Ok(r) => panic!("push unexpectedly succeeded after page-size change: {r:?}"),
    }
}
