//! Phase 6 oracle tests: device-side compaction, snapshots, and retention
//! must preserve every invariant stock litestream readers rely on.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use liters::{
    DirReplicaClient, MaintenanceOptions, Replica, ReplicaClient, ReplicaOptions, Writer,
    WriterOptions,
};
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

fn litestream_restore_rows(oracle: &Path, bucket: &Path, out: &Path) -> Vec<(i64, String)> {
    let _ = std::fs::remove_file(out);
    let output = Command::new(oracle.join("litestream"))
        .args(["restore", "-o", out.to_str().unwrap(), &format!("file://{}", bucket.display())])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "litestream restore failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    rows_of(out)
}

fn setup(tmp: &Path) -> (Connection, PathBuf, PathBuf) {
    let db_path = tmp.join("app.db");
    let bucket = tmp.join("bucket");
    let conn = Connection::open(&db_path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    (conn, db_path, bucket)
}

/// Every compaction level (1..8) must hold sorted, contiguous,
/// non-overlapping TXID ranges. (compactor.go VerifyLevelConsistency)
fn assert_levels_consistent(bucket: &Path) {
    let client = DirReplicaClient::new(bucket);
    for level in 1..9u8 {
        let files = client.ltx_files(level, Txid(0), false).unwrap();
        for pair in files.windows(2) {
            assert_eq!(
                pair[0].max_txid.0 + 1,
                pair[1].min_txid.0,
                "level {level} not contiguous: {}-{} then {}-{}",
                pair[0].min_txid,
                pair[0].max_txid,
                pair[1].min_txid,
                pair[1].max_txid
            );
        }
    }
}

/// Force-everything maintenance: all intervals zero, zero grace.
fn eager() -> MaintenanceOptions {
    MaintenanceOptions {
        level_intervals: vec![Duration::ZERO, Duration::ZERO, Duration::ZERO],
        snapshot_interval: Duration::ZERO,
        snapshot_retention: Duration::ZERO,
        l0_retention: Duration::ZERO,
        retention_enabled: true,
    }
}

#[test]
fn compaction_and_snapshot_keep_bucket_restorable() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();

    // Rounds of pushes + full maintenance; the bucket must stay restorable
    // by stock litestream at every step.
    for round in 0..4 {
        for i in 0..8 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("r{round}-{i}")]).unwrap();
            w.push().unwrap();
        }
        let report = w.maintain(&eager()).unwrap();
        if round == 0 {
            assert!(report.compacted_levels.contains(&1), "L1 not compacted: {report:?}");
            assert!(report.snapshot.is_some(), "no snapshot written: {report:?}");
        }
        assert_levels_consistent(&bucket);

        let expect = rows_of(&db_path);
        let out = tmp.path().join(format!("restored-{round}.db"));
        assert_eq!(
            litestream_restore_rows(&oracle, &bucket, &out),
            expect,
            "restore mismatch after maintenance round {round}"
        );
    }

    // Retention has actually pruned L0: only covered-and-grace-expired files
    // remain (at minimum the newest).
    let client = DirReplicaClient::new(&bucket);
    let l0 = client.ltx_files(0, Txid(0), false).unwrap();
    let max_l1 = client
        .ltx_files(1, Txid(0), false)
        .unwrap()
        .iter()
        .map(|f| f.max_txid)
        .max()
        .unwrap();
    assert!(
        l0.iter().all(|f| f.max_txid.0 >= max_l1.0.min(f.max_txid.0)),
        "unexpected L0 state"
    );
    assert!(!l0.is_empty(), "newest L0 must always be kept");

    // Snapshot retention: exactly one snapshot survives (retention zero).
    let snaps = client.ltx_files(9, Txid(0), false).unwrap();
    assert_eq!(snaps.len(), 1, "expected only the newest snapshot: {snaps:?}");
}

#[test]
fn reader_follows_through_maintenance() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    app.execute("INSERT INTO t (v) VALUES ('seed')", []).unwrap();
    w.push().unwrap();

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    rep.sync().unwrap();

    for round in 0..3 {
        for i in 0..6 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("m{round}-{i}")]).unwrap();
            w.push().unwrap();
        }
        // Maintenance compacts L0s into L1 and prunes them (grace zero) —
        // the reader must bridge through L1/snapshots without re-restoring.
        w.maintain(&eager()).unwrap();

        let r = rep.sync().unwrap();
        assert!(!r.restored, "round {round}: reader re-restored instead of following");
        assert_eq!(rows_of(&replica_path), rows_of(&db_path), "round {round} content mismatch");
    }
}

#[test]
fn maintenance_is_incremental_across_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();

    for i in 0..3 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("a-{i}")]).unwrap();
        w.push().unwrap();
    }
    w.maintain(&eager()).unwrap();
    let client = DirReplicaClient::new(&bucket);
    let l1_before = client.ltx_files(1, Txid(0), false).unwrap();
    assert_eq!(l1_before.len(), 1);
    assert_eq!(l1_before[0].max_txid, Txid(3));

    // No new pushes: compaction must be a no-op (no zero-source compaction,
    // no duplicate files).
    let report = w.maintain(&eager()).unwrap();
    assert!(report.compacted_levels.is_empty(), "no-op maintain compacted: {report:?}");

    // More pushes: the next L1 file continues exactly at max+1.
    for i in 0..2 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("b-{i}")]).unwrap();
        w.push().unwrap();
    }
    w.maintain(&eager()).unwrap();
    let l1_after = client.ltx_files(1, Txid(0), false).unwrap();
    assert_eq!(l1_after.len(), 2);
    assert_eq!(l1_after[1].min_txid, Txid(4));
    assert_eq!(l1_after[1].max_txid, Txid(5));
    assert_levels_consistent(&bucket);
}
