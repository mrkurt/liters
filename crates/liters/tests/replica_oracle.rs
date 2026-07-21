//! Reader oracle tests: the Replica must correctly consume buckets written by
//! the Rust writer AND by stock Go litestream, survive compaction/GC races,
//! and detect divergence.

use std::path::{Path, PathBuf};
use std::process::Command;

use liters::{
    DirReplicaClient, Error, Replica, ReplicaClient, ReplicaOptions, Writer, WriterOptions,
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
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok");
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

fn setup(tmp: &Path) -> (Connection, PathBuf, PathBuf) {
    let db_path = tmp.join("app.db");
    let bucket = tmp.join("bucket");
    let conn = Connection::open(&db_path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    (conn, db_path, bucket)
}

#[test]
fn restore_then_incremental_follow() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();

    app.execute("INSERT INTO t (v) VALUES ('one')", []).unwrap();
    w.push().unwrap();

    // Initial sync = full restore.
    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    let r = rep.sync().unwrap();
    assert!(r.restored);
    assert_eq!(r.to_txid, Txid(1));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));

    // Incremental follows, one per push.
    for i in 0..6 {
        for j in 0..30 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("r{i}-{j}")]).unwrap();
        }
        if i % 2 == 1 {
            app.execute("DELETE FROM t WHERE id % 5 = 0", []).unwrap();
        }
        w.push().unwrap();

        let r = rep.sync().unwrap();
        assert!(!r.restored, "sync {i} unexpectedly re-restored");
        assert_eq!(r.to_txid, Txid(2 + i as u64));
        assert_eq!(rows_of(&replica_path), rows_of(&db_path), "content diverged at sync {i}");
    }

    // No new data: position unchanged.
    let before = rep.position().unwrap();
    let r = rep.sync().unwrap();
    assert_eq!(r.to_txid, before);
    assert_eq!(r.from_txid, before);

    // Crash-recovery: the sidecar is authoritative; a re-opened replica
    // resumes cleanly.
    drop(rep);
    let mut rep2 = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    app.execute("INSERT INTO t (v) VALUES ('after-reopen')", []).unwrap();
    w.push().unwrap();
    let r = rep2.sync().unwrap();
    assert!(!r.restored);
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

#[test]
fn follows_live_litestream_bucket() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());
    app.execute("INSERT INTO t (v) VALUES ('seed')", []).unwrap();

    // Run litestream for ~4s while rows are written, syncing the replica
    // concurrently.
    let mut child = Command::new(oracle.join("litestream"))
        .args([
            "replicate",
            "-exec",
            "sleep 4",
            db_path.to_str().unwrap(),
            &format!("file://{}", bucket.display()),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );

    // Write all rows within the first ~2s — strictly before litestream's
    // shutdown sync — while syncing the replica against the moving bucket.
    let mut synced_once = false;
    for i in 0..200u64 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("live-{i}")]).unwrap();
        if i % 20 == 0 {
            match rep.sync() {
                Ok(_) => synced_once = true,
                // Bucket may be empty for the first second.
                Err(e) => assert!(
                    e.to_string().contains("transaction not available"),
                    "unexpected sync error: {e}"
                ),
            }
            std::thread::sleep(std::time::Duration::from_millis(150));
        }
    }

    // All rows are committed; litestream's shutdown sync captures them.
    let status = child.wait().unwrap();
    assert!(status.success());
    rep.sync().unwrap();
    assert!(synced_once || rep.position().unwrap() > Txid(0));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

/// Compacts L0 files [1..=k] of a bucket into a single L1 file using the ltx
/// compactor, mirroring litestream's store compaction.
fn compact_l0_into_l1(bucket: &Path, upto: Txid) {
    let client = DirReplicaClient::new(bucket);
    let files = client.ltx_files(0, Txid(0), false).unwrap();
    let inputs: Vec<_> = files.iter().filter(|f| f.max_txid <= upto).collect();
    assert!(!inputs.is_empty());
    let readers: Vec<_> = inputs
        .iter()
        .map(|f| client.open_ltx_file(0, f.min_txid, f.max_txid, 0, 0).unwrap())
        .collect();
    let mut compactor = ltx::Compactor::new(readers);
    compactor.header_flags = ltx::HEADER_FLAG_NO_CHECKSUM;
    let mut out = Vec::new();
    let (hdr, _) = compactor.compact(&mut out).unwrap();
    client
        .write_ltx_file(1, hdr.min_txid, hdr.max_txid, &mut std::io::Cursor::new(&out))
        .unwrap();
}

#[test]
fn gap_bridging_via_l1_after_l0_pruned() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();

    // Reader syncs at TXID 2.
    for i in 0..2 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("a-{i}")]).unwrap();
        w.push().unwrap();
    }
    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    assert_eq!(rep.sync().unwrap().to_txid, Txid(2));

    // More pushes; then compact 1..=5 into L1 and prune those L0s entirely.
    for i in 0..3 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("b-{i}")]).unwrap();
        w.push().unwrap();
    }
    compact_l0_into_l1(&bucket, Txid(5));
    let client = DirReplicaClient::new(&bucket);
    let l0 = client.ltx_files(0, Txid(0), false).unwrap();
    client
        .delete_ltx_files(&l0.iter().filter(|f| f.max_txid <= Txid(5)).cloned().collect::<Vec<_>>())
        .unwrap();

    // Continue pushing new L0s after the pruned range.
    for i in 0..2 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("c-{i}")]).unwrap();
        w.push().unwrap();
    }

    // The reader at TXID 2 must bridge 3..=5 via the overlapping L1 file
    // (min=1 <= 2+1), then continue with L0 6..=7.
    let r = rep.sync().unwrap();
    assert!(!r.restored, "gap bridging must not re-restore");
    assert_eq!(r.to_txid, Txid(7));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

#[test]
fn snapshot_fallback_when_all_levels_pruned() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    app.execute("INSERT INTO t (v) VALUES ('x')", []).unwrap();
    w.push().unwrap();

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    assert_eq!(rep.sync().unwrap().to_txid, Txid(1));

    // Advance the writer, then simulate aggressive retention: everything in
    // levels 0..8 replaced by a single L9 snapshot.
    for i in 0..4 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("s-{i}")]).unwrap();
        w.push().unwrap();
    }
    let client = DirReplicaClient::new(&bucket);
    let l0 = client.ltx_files(0, Txid(0), false).unwrap();
    // Merge ALL L0s (which start at the TXID-1 snapshot) into a level-9
    // snapshot file, then delete every L0.
    let readers: Vec<_> = l0
        .iter()
        .map(|f| client.open_ltx_file(0, f.min_txid, f.max_txid, 0, 0).unwrap())
        .collect();
    let mut compactor = ltx::Compactor::new(readers);
    compactor.header_flags = ltx::HEADER_FLAG_NO_CHECKSUM;
    let mut out = Vec::new();
    let (hdr, _) = compactor.compact(&mut out).unwrap();
    assert!(hdr.is_snapshot());
    client
        .write_ltx_file(9, hdr.min_txid, hdr.max_txid, &mut std::io::Cursor::new(&out))
        .unwrap();
    client.delete_ltx_files(&l0).unwrap();

    // The reader at TXID 1 has no L0/L1-8 chain; it must apply the snapshot
    // in place (litestream's follow mode would stall here).
    let r = rep.sync().unwrap();
    assert!(!r.restored);
    assert_eq!(r.to_txid, Txid(5));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

#[test]
fn divergence_detected_and_reset() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    {
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        for i in 0..5 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("d-{i}")]).unwrap();
            w.push().unwrap();
        }
    }

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    assert_eq!(rep.sync().unwrap().to_txid, Txid(5));

    // Wipe the bucket and reseed from scratch (a different history).
    DirReplicaClient::new(&bucket).delete_all().unwrap();
    let meta = db_path.parent().unwrap().join(format!(
        ".{}-litestream",
        db_path.file_name().unwrap().to_string_lossy()
    ));
    std::fs::remove_dir_all(&meta).unwrap();
    app.execute("INSERT INTO t (v) VALUES ('reseed')", []).unwrap();
    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    w.push().unwrap(); // fresh snapshot at TXID 1

    // Default: divergence is an error.
    match rep.sync() {
        Err(Error::Diverged { local, remote }) => {
            assert_eq!(local, Txid(5));
            assert_eq!(remote, Txid(1));
        }
        other => panic!("expected Diverged, got {other:?}"),
    }

    // auto_reset: re-restore from the new history.
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions { auto_reset: true, ..Default::default() },
    );
    let r = rep.sync().unwrap();
    assert!(r.restored);
    assert_eq!(r.to_txid, Txid(1));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

#[test]
fn restore_survives_missing_compacted_file() {
    // Port of restore_fuzz_test.go: delete one file whose coverage is
    // duplicated at another level; a fresh restore must still succeed.
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for i in 0..6 {
        for j in 0..10 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("f-{i}-{j}")]).unwrap();
        }
        w.push().unwrap();
    }

    // Duplicate coverage: L1 covers 1..=4 while the L0s stay in place.
    compact_l0_into_l1(&bucket, Txid(4));
    let expect = rows_of(&db_path);

    let client = DirReplicaClient::new(&bucket);
    let l0: Vec<_> = client.ltx_files(0, Txid(0), false).unwrap();

    // Case A: delete one covered L0 file — restore must route through L1.
    for victim_idx in [0usize, 2] {
        let scenario = tmp.path().join(format!("bucket-a{victim_idx}"));
        copy_dir(&bucket, &scenario);
        let sc = DirReplicaClient::new(&scenario);
        sc.delete_ltx_files(std::slice::from_ref(&l0[victim_idx])).unwrap();

        let replica_path = tmp.path().join(format!("replica-a{victim_idx}.db"));
        let mut rep =
            Replica::open(&replica_path, Box::new(sc), ReplicaOptions::default());
        rep.sync().unwrap_or_else(|e| panic!("restore with missing L0 {victim_idx}: {e}"));
        assert_eq!(rows_of(&replica_path), expect);
    }

    // Case B: delete the L1 file — restore must use the raw L0 chain.
    {
        let scenario = tmp.path().join("bucket-b");
        copy_dir(&bucket, &scenario);
        let sc = DirReplicaClient::new(&scenario);
        let l1 = sc.ltx_files(1, Txid(0), false).unwrap();
        sc.delete_ltx_files(&l1).unwrap();

        let replica_path = tmp.path().join("replica-b.db");
        let mut rep = Replica::open(&replica_path, Box::new(sc), ReplicaOptions::default());
        rep.sync().unwrap();
        assert_eq!(rows_of(&replica_path), expect);
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in walk(src) {
        let rel = entry.strip_prefix(src).unwrap();
        let to = dst.join(rel);
        std::fs::create_dir_all(to.parent().unwrap()).unwrap();
        std::fs::copy(&entry, &to).unwrap();
    }
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p);
            }
        }
    }
    out
}

/// A wiped-but-not-yet-reseeded bucket is a transient window, not divergence:
/// sync must no-op (Go's follow loop no-ops on empty listings) and must never
/// destroy the healthy local replica — even with auto_reset. Divergence fires
/// only once the reseeded history actually appears.
#[test]
fn wiped_bucket_sync_is_nondestructive() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    {
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        for i in 0..3 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("w-{i}")]).unwrap();
            w.push().unwrap();
        }
    }

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions { auto_reset: true, ..Default::default() },
    );
    assert_eq!(rep.sync().unwrap().to_txid, Txid(3));
    let healthy_rows = rows_of(&replica_path);

    // Wipe the bucket; do not reseed yet.
    DirReplicaClient::new(&bucket).delete_all().unwrap();

    // Mid-window sync: a no-op, with the local replica left fully readable.
    let r = rep.sync().unwrap();
    assert!(!r.restored);
    assert_eq!(r.to_txid, Txid(3));
    assert!(replica_path.exists(), "local replica must survive the wipe window");
    assert_eq!(rows_of(&replica_path), healthy_rows);

    // Same holds without auto_reset.
    let mut plain = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    assert_eq!(plain.sync().unwrap().to_txid, Txid(3));
    assert_eq!(rows_of(&replica_path), healthy_rows);

    // Reseed with a fresh history; now divergence is real and auto_reset
    // replaces the replica atomically.
    let meta = tmp.path().join(".app.db-litestream");
    std::fs::remove_dir_all(&meta).unwrap();
    app.execute("INSERT INTO t (v) VALUES ('reseeded')", []).unwrap();
    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    w.push().unwrap();

    let r = rep.sync().unwrap();
    assert!(r.restored);
    assert_eq!(r.to_txid, Txid(1));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

type StorageResult<T> = std::result::Result<T, liters::StorageError>;

/// Wraps a real client, 404ing the first L0 open (a list-then-GC race) and
/// counting snapshot-level opens.
struct Fail404Once {
    inner: DirReplicaClient,
    fail_next_l0: std::sync::atomic::AtomicBool,
    snapshot_opens: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl ReplicaClient for Fail404Once {
    fn client_type(&self) -> &'static str {
        self.inner.client_type()
    }
    fn ltx_files(
        &self,
        level: u8,
        seek: Txid,
        use_metadata: bool,
    ) -> StorageResult<Vec<ltx::FileInfo>> {
        self.inner.ltx_files(level, seek, use_metadata)
    }
    fn open_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        offset: u64,
        size: u64,
    ) -> StorageResult<Box<dyn std::io::Read + Send>> {
        use std::sync::atomic::Ordering;
        if level == 9 {
            self.snapshot_opens.fetch_add(1, Ordering::SeqCst);
        }
        if level == 0 && self.fail_next_l0.swap(false, Ordering::SeqCst) {
            return Err(liters::StorageError::NotFound { level, min_txid, max_txid });
        }
        self.inner.open_ltx_file(level, min_txid, max_txid, offset, size)
    }
    fn write_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        rd: &mut (dyn std::io::Read + Send),
    ) -> StorageResult<ltx::FileInfo> {
        self.inner.write_ltx_file(level, min_txid, max_txid, rd)
    }
    fn delete_ltx_files(&self, infos: &[ltx::FileInfo]) -> StorageResult<()> {
        self.inner.delete_ltx_files(infos)
    }
    fn delete_all(&self) -> StorageResult<()> {
        self.inner.delete_all()
    }
}

/// A 404 on a file the sync just listed is a compaction/GC race: the replica
/// must retry next sync (Go's behavior) and must NOT fall back to
/// downloading and applying a full snapshot in the same sync.
#[test]
fn transient_404_retries_without_snapshot_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for i in 0..3 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("t-{i}")]).unwrap();
        w.push().unwrap();
    }

    let replica_path = tmp.path().join("replica.db");
    let snapshot_opens = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut rep = Replica::open(
        &replica_path,
        Box::new(Fail404Once {
            inner: DirReplicaClient::new(&bucket),
            fail_next_l0: std::sync::atomic::AtomicBool::new(false),
            snapshot_opens: snapshot_opens.clone(),
        }),
        ReplicaOptions::default(),
    );
    assert_eq!(rep.sync().unwrap().to_txid, Txid(3));

    // New L0 (txid 4) plus an L9 snapshot covering 1..=4.
    app.execute("INSERT INTO t (v) VALUES ('t-3')", []).unwrap();
    w.push().unwrap();
    assert_eq!(w.snapshot().unwrap(), Some(Txid(4)));
    snapshot_opens.store(0, std::sync::atomic::Ordering::SeqCst);

    // Recreate the replica with the 404 armed for the next L0 open.
    let mut rep = Replica::open(
        &replica_path,
        Box::new(Fail404Once {
            inner: DirReplicaClient::new(&bucket),
            fail_next_l0: std::sync::atomic::AtomicBool::new(true),
            snapshot_opens: snapshot_opens.clone(),
        }),
        ReplicaOptions::default(),
    );

    // Raced sync: no progress, and crucially no snapshot download.
    let r = rep.sync().unwrap();
    assert!(!r.restored);
    assert_eq!(r.to_txid, Txid(3), "raced sync must not advance");
    assert_eq!(
        snapshot_opens.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "transient 404 must not trigger a full snapshot apply"
    );

    // Next sync sees the truth and applies the small L0 delta.
    let r = rep.sync().unwrap();
    assert_eq!(r.to_txid, Txid(4));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}
