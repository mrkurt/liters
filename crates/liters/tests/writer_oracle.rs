//! Writer oracle tests: everything the Rust writer pushes must be restorable
//! by stock Go `litestream restore`, at every intermediate position, and the
//! writer must interoperate with litestream's own local state (same meta dir,
//! same bucket).

use std::path::{Path, PathBuf};
use std::process::Command;

use liters::{DirReplicaClient, ReplicaClient, Writer, WriterOptions};
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

/// All rows of `t`, plus the integrity check.
fn snapshot_rows(conn: &Connection) -> Vec<(i64, String)> {
    let ok: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ok, "ok");
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    rows
}

fn restored_rows(oracle: &Path, bucket: &Path, tmp: &Path, tag: &str) -> Vec<(i64, String)> {
    let out_db = tmp.join(format!("restored-{tag}.db"));
    litestream_restore(oracle, bucket, &out_db);
    let conn = Connection::open(&out_db).unwrap();
    snapshot_rows(&conn)
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
fn every_push_is_restorable() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    let client = Box::new(DirReplicaClient::new(&bucket));
    let mut w = Writer::open(&db_path, client, WriterOptions::default()).unwrap();

    // First push: full snapshot at TXID 1.
    app.execute("INSERT INTO t (v) VALUES ('genesis')", []).unwrap();
    let r = w.push().unwrap();
    assert!(r.synced);
    assert_eq!(r.txid, Txid(1));
    assert_eq!(r.remote_txid, Txid(1));
    assert_eq!(
        restored_rows(&oracle, &bucket, tmp.path(), "t1"),
        snapshot_rows(&app)
    );

    // A series of pushes; after each, litestream restore must reproduce the
    // exact committed content.
    for i in 0..8 {
        for j in 0..25 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("row-{i}-{j}")]).unwrap();
        }
        if i % 3 == 1 {
            app.execute("DELETE FROM t WHERE id % 7 = 0", []).unwrap();
        }
        let expect = snapshot_rows(&app);
        let r = w.push().unwrap();
        assert!(r.synced, "push {i} created no L0");
        assert_eq!(r.txid, Txid(2 + i as u64));
        assert_eq!(
            restored_rows(&oracle, &bucket, tmp.path(), &format!("i{i}")),
            expect,
            "restore mismatch after push {i}"
        );
    }

    // Idle push: no new committed content, no new L0.
    let before = w.pos().unwrap();
    let r = w.push().unwrap();
    assert!(!r.synced, "idle push must not create an L0 file");
    assert_eq!(w.pos().unwrap(), before);
}

#[test]
fn checkpoints_stay_incremental() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    // Grow the database so incremental pushes are clearly smaller than
    // snapshots.
    for i in 0..500 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("bulk-{i:05}-{}", "x".repeat(64))])
            .unwrap();
    }

    let client = Box::new(DirReplicaClient::new(&bucket));
    let mut w = Writer::open(&db_path, client, WriterOptions::default()).unwrap();
    w.push().unwrap(); // snapshot at TXID 1

    // Force checkpoints between small writes; pushes afterwards must remain
    // incremental (a snapshot would carry ~all pages). Port of the invariant
    // in db_internal_test.go:931/1043.
    for (i, mode) in [liters::CheckpointMode::Passive, liters::CheckpointMode::Truncate]
        .iter()
        .cycle()
        .take(6)
        .enumerate()
    {
        w.checkpoint(*mode).unwrap();
        app.execute("UPDATE t SET v = ?1 WHERE id = ?2", [format!("upd-{i}"), (i + 1).to_string()])
            .unwrap();
        let expect = snapshot_rows(&app);
        let r = w.push().unwrap();
        assert!(r.synced);

        // Inspect the L0 file just written: page count must be far below the
        // database page count (i.e. not a snapshot).
        let bucket_client = DirReplicaClient::new(&bucket);
        let mut rd = bucket_client.open_ltx_file(0, r.txid, r.txid, 0, 0).unwrap();
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut rd, &mut buf).unwrap();
        let dec = ltx::Decoder::new(std::io::Cursor::new(&buf));
        let (hdr, _, index) = dec.verify().unwrap();
        assert!(
            (index.len() as u32) < hdr.commit / 2,
            "push {i} after {mode:?} checkpoint looks like a snapshot: {} pages of commit {}",
            index.len(),
            hdr.commit,
        );

        assert_eq!(
            restored_rows(&oracle, &bucket, tmp.path(), &format!("ckpt{i}")),
            expect
        );
    }
}

#[test]
fn wal_threshold_triggers_checkpoint() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());
    app.execute("INSERT INTO t (v) VALUES ('x')", []).unwrap();

    let client = Box::new(DirReplicaClient::new(&bucket));
    let mut w = Writer::open(
        &db_path,
        client,
        WriterOptions { min_checkpoint_page_n: 4, ..Default::default() },
    )
    .unwrap();
    w.push().unwrap();

    // Enough writes to cross the 4-page WAL threshold.
    let mut checkpointed = false;
    for i in 0..10 {
        for j in 0..20 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("w-{i}-{j}")]).unwrap();
        }
        let r = w.push().unwrap();
        checkpointed |= r.checkpointed;
    }
    assert!(checkpointed, "no checkpoint ran despite tiny threshold");

    let expect = snapshot_rows(&app);
    assert_eq!(restored_rows(&oracle, &bucket, tmp.path(), "final"), expect);
}

#[test]
fn litestream_writes_then_liters_continues() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());
    for i in 0..50 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("go-{i}")]).unwrap();
    }

    // Litestream replicates first (creating its own meta dir + bucket state).
    let output = Command::new(oracle.join("litestream"))
        .args([
            "replicate",
            "-exec",
            "sleep 3",
            db_path.to_str().unwrap(),
            &format!("file://{}", bucket.display()),
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));

    // liters continues from litestream's local meta state (same dir layout).
    let client = Box::new(DirReplicaClient::new(&bucket));
    let mut w = Writer::open(&db_path, client, WriterOptions::default()).unwrap();
    let start_pos = w.pos().unwrap();
    assert!(!start_pos.txid.is_zero(), "expected position derived from litestream's meta dir");

    for i in 0..30 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("rust-{i}")]).unwrap();
    }
    let expect = snapshot_rows(&app);
    let r = w.push().unwrap();
    assert!(r.synced);
    assert!(r.txid > start_pos.txid);

    assert_eq!(restored_rows(&oracle, &bucket, tmp.path(), "handoff"), expect);
}

#[test]
fn upload_failure_preserves_state_and_recovers() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());
    app.execute("INSERT INTO t (v) VALUES ('a')", []).unwrap();

    /// A client whose writes fail while poisoned.
    struct Flaky {
        inner: DirReplicaClient,
        poisoned: std::sync::atomic::AtomicBool,
    }
    impl ReplicaClient for Flaky {
        fn client_type(&self) -> &'static str {
            "flaky"
        }
        fn ltx_files(
            &self,
            level: u8,
            seek: Txid,
            use_metadata: bool,
        ) -> liters_storage::Result<Vec<ltx::FileInfo>> {
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
            rd: &mut (dyn std::io::Read + Send),
        ) -> liters_storage::Result<ltx::FileInfo> {
            if self.poisoned.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(liters_storage::StorageError::Other("injected failure".into()));
            }
            self.inner.write_ltx_file(level, min, max, rd)
        }
        fn delete_ltx_files(&self, infos: &[ltx::FileInfo]) -> liters_storage::Result<()> {
            self.inner.delete_ltx_files(infos)
        }
        fn delete_all(&self) -> liters_storage::Result<()> {
            self.inner.delete_all()
        }
    }

    let flaky = Box::new(Flaky {
        inner: DirReplicaClient::new(&bucket),
        poisoned: std::sync::atomic::AtomicBool::new(false),
    });
    let poisoned: *const std::sync::atomic::AtomicBool = &flaky.poisoned;
    let mut w = Writer::open(&db_path, flaky, WriterOptions::default()).unwrap();

    // Poison uploads; the push must fail but keep local state consistent.
    unsafe { (*poisoned).store(true, std::sync::atomic::Ordering::SeqCst) };
    assert!(w.push().is_err());
    let pos_after_failure = w.pos().unwrap();
    assert_eq!(pos_after_failure.txid, Txid(1), "L0 must exist locally despite upload failure");

    // More writes while the bucket is unreachable.
    app.execute("INSERT INTO t (v) VALUES ('b')", []).unwrap();
    unsafe { (*poisoned).store(true, std::sync::atomic::Ordering::SeqCst) };
    assert!(w.push().is_err());

    // Heal: the next push uploads the whole backlog.
    unsafe { (*poisoned).store(false, std::sync::atomic::Ordering::SeqCst) };
    let expect = snapshot_rows(&app);
    let r = w.push().unwrap();
    assert_eq!(r.remote_txid, w.pos().unwrap().txid);

    assert_eq!(restored_rows(&oracle, &bucket, tmp.path(), "healed"), expect);
}

#[test]
fn device_restore_rebaselines_from_bucket() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());

    {
        let client = Box::new(DirReplicaClient::new(&bucket));
        let mut w = Writer::open(&db_path, client, WriterOptions::default()).unwrap();
        for i in 0..5 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("pre-{i}")]).unwrap();
            w.push().unwrap();
        }
        assert_eq!(w.pos().unwrap().txid, Txid(5));
    }

    // Simulate a device restore from an old backup: wipe the local meta dir
    // (local position resets to zero while the bucket sits at TXID 5).
    let meta_dir = db_path.parent().unwrap().join(format!(
        ".{}-litestream",
        db_path.file_name().unwrap().to_string_lossy()
    ));
    std::fs::remove_dir_all(&meta_dir).unwrap();

    app.execute("INSERT INTO t (v) VALUES ('post-restore')", []).unwrap();

    let client = Box::new(DirReplicaClient::new(&bucket));
    let mut w = Writer::open(&db_path, client, WriterOptions::default()).unwrap();
    // open() no longer touches the bucket; the lineage check on the first
    // push detects the bucket ahead of the (wiped) local lineage and adopts
    // the bucket position.
    assert_eq!(w.pos().unwrap().txid, Txid(0));
    let r = w.push().unwrap();
    assert_eq!(r.txid, Txid(5), "first push must rebaseline to the bucket position");
    assert_eq!(r.uploaded, 0, "the rebaselining push uploads nothing");

    let expect = snapshot_rows(&app);
    let r = w.push().unwrap();
    assert!(r.synced);
    assert_eq!(r.txid, Txid(6), "push after rebaseline must continue at remote+1");

    assert_eq!(restored_rows(&oracle, &bucket, tmp.path(), "rebase"), expect);
}

#[test]
fn concurrent_writes_during_pushes() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let (app, db_path, bucket) = setup(tmp.path());
    drop(app);

    // Open the writer before the write storm: open() performs first-run
    // schema writes (meta tables, WAL bootstrap) and is documented as
    // failing retryably with SQLITE_BUSY when the app writes non-stop past
    // the busy timeout.
    let client = Box::new(DirReplicaClient::new(&bucket));
    let mut w = Writer::open(
        &db_path,
        client,
        WriterOptions { min_checkpoint_page_n: 8, ..Default::default() },
    )
    .unwrap();

    // A bounded concurrent writer: fixed row count so the database (and this
    // test's disk usage) cannot grow without bound if pushes run slow.
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_done = done.clone();
    let writer_db = db_path.clone();
    let writer_thread = std::thread::spawn(move || {
        let conn = Connection::open(&writer_db).unwrap();
        conn.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
        for i in 0..2000u64 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("c-{i}")]).unwrap();
            if i % 100 == 0 {
                std::thread::yield_now();
            }
        }
        writer_done.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    while !done.load(std::sync::atomic::Ordering::Relaxed) {
        w.push().unwrap();
    }
    writer_thread.join().unwrap();

    // Final push captures the last committed state.
    let app = Connection::open(&db_path).unwrap();
    let expect = snapshot_rows(&app);
    w.push().unwrap();

    assert_eq!(restored_rows(&oracle, &bucket, tmp.path(), "concurrent"), expect);
}
