//! Offline-tolerant Writer tests: construction with an unreachable bucket,
//! offline L0 accumulation + backlog catch-up, and the lazy lineage check's
//! device-restore rebaseline with offline L0s in play. The rebaseline test
//! additionally gates on the Go oracle when available (writer_oracle.rs
//! style): the post-rebaseline bucket must restore with stock litestream.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use liters::{DirReplicaClient, Replica, ReplicaClient, ReplicaOptions, Writer, WriterOptions};
use ltx::{FileInfo, Txid};
use rusqlite::Connection;

fn create_db(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn
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

fn meta_dir_of(db_path: &Path) -> PathBuf {
    db_path.parent().unwrap().join(format!(
        ".{}-litestream",
        db_path.file_name().unwrap().to_string_lossy()
    ))
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap().flatten() {
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

fn remove_if_exists(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("remove {path:?}: {e}"),
    }
}

fn sibling(db_path: &Path, suffix: &str) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push(suffix);
    PathBuf::from(p)
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

/// The "device is offline" backend: every operation fails with a transient
/// transport error.
struct OfflineClient;

impl ReplicaClient for OfflineClient {
    fn client_type(&self) -> &'static str {
        "offline"
    }
    fn ltx_files(
        &self,
        _level: u8,
        _seek: Txid,
        _use_metadata: bool,
    ) -> liters_storage::Result<Vec<FileInfo>> {
        Err(liters_storage::StorageError::Unavailable("bucket unreachable".into()))
    }
    fn open_ltx_file(
        &self,
        _level: u8,
        _min: Txid,
        _max: Txid,
        _offset: u64,
        _size: u64,
    ) -> liters_storage::Result<Box<dyn std::io::Read + Send>> {
        Err(liters_storage::StorageError::Unavailable("bucket unreachable".into()))
    }
    fn write_ltx_file(
        &self,
        _level: u8,
        _min: Txid,
        _max: Txid,
        _rd: &mut (dyn std::io::Read + Send),
    ) -> liters_storage::Result<FileInfo> {
        Err(liters_storage::StorageError::Unavailable("bucket unreachable".into()))
    }
    fn delete_ltx_files(&self, _infos: &[FileInfo]) -> liters_storage::Result<()> {
        Err(liters_storage::StorageError::Unavailable("bucket unreachable".into()))
    }
    fn delete_all(&self) -> liters_storage::Result<()> {
        Err(liters_storage::StorageError::Unavailable("bucket unreachable".into()))
    }
}

// ---------------------------------------------------------------------------

/// O1: Writer::open performs no network I/O (construction with an erroring
/// client succeeds); offline pushes fail transiently but accumulate local
/// L0s; a fresh writer over the same meta dir uploads the whole backlog.
#[test]
fn open_offline_accumulates_and_catches_up() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db_path);
    for i in 0..10 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("o1-a-{i}")]).unwrap();
    }

    // Construction succeeds despite the unreachable bucket.
    let mut w =
        Writer::open(&db_path, Box::new(OfflineClient), WriterOptions::default()).unwrap();

    // Offline pushes: transient error out, but the WAL→L0 conversion landed.
    let err = w.push().expect_err("offline push must fail");
    assert!(err.is_transient(), "offline failure must be transient: {err:?}");
    assert_eq!(w.pos().unwrap().txid, Txid(1), "L0 must accumulate despite the offline bucket");

    for i in 0..10 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("o1-b-{i}")]).unwrap();
    }
    assert!(w.push().expect_err("still offline").is_transient());
    assert_eq!(w.pos().unwrap().txid, Txid(2));
    drop(w);

    // Back online: a fresh writer on the same meta dir uploads the backlog
    // plus the new batch in one push.
    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    for i in 0..10 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("o1-c-{i}")]).unwrap();
    }
    let r = w.push().unwrap();
    assert_eq!(r.txid, Txid(3));
    assert_eq!(r.uploaded, 3, "2 offline L0s + 1 new must upload together");
    assert_eq!(r.remote_txid, Txid(3));

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    let sr = rep.sync().unwrap();
    assert_eq!(sr.to_txid, Txid(3));
    assert_eq!(rows_of(&replica_path), rows_of(&db_path));
}

/// O2: the device-restore scenario the lazy lineage check exists for. A
/// device is backed up at bucket position 3, keeps pushing to 6, then is
/// restored from the backup and used OFFLINE (accumulating local L0s 4..5
/// on the stale lineage). The first online push must detect the bucket
/// beyond the lineage's verified position (3), rebaseline (discarding the
/// offline L0s), and the next push must emit a FULL SNAPSHOT L0 at
/// remote_max+1 — never an incremental, whose pre-state the bucket doesn't
/// have. Gated on the Go oracle when available: the resulting bucket must
/// restore with stock litestream to the restored device's content.
#[test]
fn rebaseline_after_device_restore_with_offline_l0s() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let meta_dir = meta_dir_of(&db_path);

    // Phase 1: the original device pushes txids 1..3.
    {
        let conn = create_db(&db_path);
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        for i in 0..3 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("pre-{i}")]).unwrap();
            w.push().unwrap();
        }
        assert_eq!(w.pos().unwrap().txid, Txid(3));
        // Uploads persisted the verified position.
        assert_eq!(
            std::fs::read_to_string(meta_dir.join("verified-pos")).unwrap().trim(),
            format!("{}", Txid(3)),
        );
        // Make the backup deterministic: fold the WAL into the db file
        // before copying it (the writer must drop first — its read lock
        // starves checkpoints by design).
        drop(w);
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }

    // "Back up the device": db file + meta dir (the WAL was checkpointed
    // into the db on close, as a device backup would capture).
    let backup = tmp.path().join("backup");
    std::fs::create_dir_all(&backup).unwrap();
    std::fs::copy(&db_path, backup.join("app.db")).unwrap();
    copy_dir(&meta_dir, &backup.join("meta"));

    // Phase 2: the device keeps going after the backup, pushing 4..6 with
    // large rows so the pre-restore WAL grows far past anything the
    // restored device will write — making verify()'s truncated-WAL branch
    // the deterministic snapshot trigger after the rebaseline. (The salts
    // of the adopted header belong to a deleted WAL generation, so the
    // salt-mismatch branches would force the same snapshot regardless.)
    {
        let conn = Connection::open(&db_path).unwrap();
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        let big = "x".repeat(32 * 1024);
        for i in 0..3 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("ahead-{i}-{big}")]).unwrap();
            w.push().unwrap();
        }
        assert_eq!(w.pos().unwrap().txid, Txid(6));
    }

    // Device restore: local state rolls back to the backup while the bucket
    // stays at 6.
    remove_if_exists(&db_path);
    remove_if_exists(&sibling(&db_path, "-wal"));
    remove_if_exists(&sibling(&db_path, "-shm"));
    std::fs::remove_dir_all(&meta_dir).unwrap();
    std::fs::copy(backup.join("app.db"), &db_path).unwrap();
    copy_dir(&backup.join("meta"), &meta_dir);

    // Phase 3: the restored device is used offline; L0s 4..5 accumulate on
    // the stale lineage.
    let conn = Connection::open(&db_path).unwrap();
    {
        let mut w =
            Writer::open(&db_path, Box::new(OfflineClient), WriterOptions::default()).unwrap();
        for i in 0..2 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("offline-{i}")]).unwrap();
            assert!(w.push().expect_err("offline push must fail").is_transient());
        }
        assert_eq!(w.pos().unwrap().txid, Txid(5));
    }

    // Phase 4: back online. This push syncs the newest commit into a local
    // L0 first (exercising the synced_to_wal_end reset in the rebaseline),
    // then the lineage check sees bucket max 6 > verified position 3:
    // foreign data — rebaseline. The offline L0s are discarded and nothing
    // uploads; that is the correct outcome for a restored device, because
    // txids 4..5 already name transactions the bucket got from the
    // pre-restore lineage.
    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    conn.execute("INSERT INTO t (v) VALUES ('online-0')", []).unwrap();
    let r = w.push().unwrap();
    assert_eq!(r.txid, Txid(6), "rebaseline must adopt the bucket position");
    assert_eq!(r.uploaded, 0, "the rebaselining push must upload nothing");

    // The push after the rebaseline must land a FULL SNAPSHOT at
    // remote_max+1: the adopted L0's wal_offset/salts describe the
    // pre-restore WAL, so verify() refuses to resume incrementally.
    let r = w.push().unwrap();
    assert!(r.synced);
    assert_eq!(r.txid, Txid(7), "first push after rebaseline must land at remote_max+1");
    assert_eq!(r.uploaded, 1);
    assert_eq!(r.remote_txid, Txid(7));

    // Decode the uploaded L0 7: a single-txid file carrying EVERY page up
    // to its commit — a snapshot image, not an incremental.
    let dir = DirReplicaClient::new(&bucket);
    let mut rd = dir.open_ltx_file(0, Txid(7), Txid(7), 0, 0).unwrap();
    let mut buf = Vec::new();
    rd.read_to_end(&mut buf).unwrap();
    let dec = ltx::Decoder::new(std::io::Cursor::new(&buf));
    let (hdr, _, index) = dec.verify().unwrap();
    assert_eq!(hdr.min_txid, Txid(7));
    assert_eq!(hdr.max_txid, Txid(7));
    assert_eq!(
        index.len() as u32,
        hdr.commit,
        "post-rebaseline L0 must carry every page (snapshot), got {} of {}",
        index.len(),
        hdr.commit,
    );

    // The bucket now restores to the RESTORED device's content (pre +
    // offline + online rows), not the abandoned pre-restore branch.
    let expect = rows_of(&db_path);
    assert_eq!(expect.len(), 6, "3 pre + 2 offline + 1 online rows");

    let replica_path = tmp.path().join("replica.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions::default(),
    );
    assert_eq!(rep.sync().unwrap().to_txid, Txid(7));
    assert_eq!(rows_of(&replica_path), expect);

    // Oracle gate: stock litestream restore agrees.
    if let Some(oracle) = oracle_dir() {
        let restored = tmp.path().join("restored.db");
        litestream_restore(&oracle, &bucket, &restored);
        assert_eq!(rows_of(&restored), expect);
    }
}

// ---------------------------------------------------------------------------
// Additional lineage/recovery scenarios: mid-session foreign advance, wiped
// or reseeded (regressed) buckets, legacy verified-pos migration, and the
// reset_local recovery path. All follow the O1/O2 conventions: bounded DB
// load, restore checks off the bucket, oracle gate where available.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Toggleable "offline" dir client: while `offline` is set every operation
/// fails with a transient transport error; otherwise it delegates.
struct FlakyClient {
    inner: DirReplicaClient,
    offline: Arc<AtomicBool>,
}

impl FlakyClient {
    fn gate(&self) -> liters_storage::Result<()> {
        if self.offline.load(Ordering::SeqCst) {
            Err(liters_storage::StorageError::Unavailable("bucket unreachable".into()))
        } else {
            Ok(())
        }
    }
}

impl ReplicaClient for FlakyClient {
    fn client_type(&self) -> &'static str {
        "flaky"
    }
    fn ltx_files(
        &self,
        level: u8,
        seek: Txid,
        use_metadata: bool,
    ) -> liters_storage::Result<Vec<FileInfo>> {
        self.gate()?;
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
        self.gate()?;
        self.inner.open_ltx_file(level, min, max, offset, size)
    }
    fn write_ltx_file(
        &self,
        level: u8,
        min: Txid,
        max: Txid,
        rd: &mut (dyn std::io::Read + Send),
    ) -> liters_storage::Result<FileInfo> {
        self.gate()?;
        self.inner.write_ltx_file(level, min, max, rd)
    }
    fn delete_ltx_files(&self, infos: &[FileInfo]) -> liters_storage::Result<()> {
        self.gate()?;
        self.inner.delete_ltx_files(infos)
    }
    fn delete_all(&self) -> liters_storage::Result<()> {
        self.gate()?;
        self.inner.delete_all()
    }
}

/// Decodes the bucket's L0 file at `txid` and asserts it is a full-image
/// snapshot: single-txid, carrying every page up to its commit.
fn assert_snapshot_l0(bucket: &Path, txid: Txid) {
    let dir = DirReplicaClient::new(bucket);
    let mut rd = dir.open_ltx_file(0, txid, txid, 0, 0).unwrap();
    let mut buf = Vec::new();
    rd.read_to_end(&mut buf).unwrap();
    let dec = ltx::Decoder::new(std::io::Cursor::new(&buf));
    let (hdr, _, index) = dec.verify().unwrap();
    assert_eq!(hdr.min_txid, txid);
    assert_eq!(hdr.max_txid, txid);
    assert_eq!(
        index.len() as u32,
        hdr.commit,
        "L0 {txid} must carry every page (snapshot), got {} of {}",
        index.len(),
        hdr.commit,
    );
}

/// L0 (min,max) pairs currently in the bucket, ascending.
fn bucket_l0s(bucket: &Path) -> Vec<(u64, u64)> {
    let mut v: Vec<(u64, u64)> = DirReplicaClient::new(bucket)
        .ltx_files(0, Txid(0), false)
        .unwrap()
        .iter()
        .map(|f| (f.min_txid.0, f.max_txid.0))
        .collect();
    v.sort_unstable();
    v
}

/// Flips a byte in the middle of the newest local L0 file so its CRC check
/// fails (the LocalLtx trigger).
fn corrupt_local_l0(meta_dir: &Path, txid: Txid) {
    let path = meta_dir.join("ltx").join("0").join(ltx::format_filename(txid, txid));
    let mut bytes = std::fs::read(&path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
}

/// Restores the bucket into a scratch replica and asserts it matches `db`.
fn assert_bucket_restores_to(bucket: &Path, db: &Path, scratch: &Path, expect_txid: Txid) {
    let mut rep = Replica::open(
        scratch,
        Box::new(DirReplicaClient::new(bucket)),
        ReplicaOptions::default(),
    );
    assert_eq!(rep.sync().unwrap().to_txid, expect_txid);
    assert_eq!(rows_of(scratch), rows_of(db));
    if let Some(oracle) = oracle_dir() {
        let restored = scratch.with_extension("oracle.db");
        litestream_restore(&oracle, bucket, &restored);
        assert_eq!(rows_of(&restored), rows_of(db));
    }
}

/// O3: a bucket that a FOREIGN writer advanced mid-session must not be
/// adopted by upload()'s re-derivation. Device A pushes txid 1, goes
/// offline (accumulating L0 2), and meanwhile device B rebaselines onto the
/// bucket and pushes its own txid 2. A's next push re-derives the remote
/// cursor (its upload failed, invalidating it) and must FAIL — appending
/// A's L0s over B's txid 2 would interleave two lineages — and the push
/// after that must rebaseline and snapshot, exactly like the session-start
/// lineage check would.
#[test]
fn foreign_advance_mid_session_fails_push_then_rebaselines() {
    let tmp = tempfile::tempdir().unwrap();
    let db_a = tmp.path().join("a.db");
    let db_b = tmp.path().join("b.db");
    let bucket = tmp.path().join("bucket");
    let conn_a = create_db(&db_a);

    let offline = Arc::new(AtomicBool::new(false));
    let mut w = Writer::open(
        &db_a,
        Box::new(FlakyClient {
            inner: DirReplicaClient::new(&bucket),
            offline: offline.clone(),
        }),
        WriterOptions::default(),
    )
    .unwrap();

    // Online push: bucket at 1, session lineage-checked, verified 1.
    for i in 0..5 {
        conn_a.execute("INSERT INTO t (v) VALUES (?1)", [format!("a1-{i}")]).unwrap();
    }
    assert_eq!(w.push().unwrap().remote_txid, Txid(1));

    // Offline push: L0 2 accumulates locally, upload cursor invalidated.
    offline.store(true, Ordering::SeqCst);
    for i in 0..5 {
        conn_a.execute("INSERT INTO t (v) VALUES (?1)", [format!("a2-{i}")]).unwrap();
    }
    assert!(w.push().expect_err("offline push must fail").is_transient());
    assert_eq!(w.pos().unwrap().txid, Txid(2));

    // Device B (fresh install, same bucket): rebaselines onto A's txid 1,
    // then pushes ITS OWN transaction as txid 2.
    {
        let conn_b = create_db(&db_b);
        let mut wb = Writer::open(
            &db_b,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        assert_eq!(wb.push().unwrap().uploaded, 0, "B's first push must rebaseline");
        for i in 0..5 {
            conn_b.execute("INSERT INTO t (v) VALUES (?1)", [format!("b-{i}")]).unwrap();
        }
        let r = wb.push().unwrap();
        assert_eq!(r.remote_txid, Txid(2), "B must own bucket txid 2");
    }

    // A back online, same session: the re-derived remote (2) is beyond what
    // this session verified (1) — the push must fail, non-transiently, and
    // must not touch the bucket.
    offline.store(false, Ordering::SeqCst);
    let err = w.push().expect_err("push over a foreign-advanced bucket must fail");
    assert!(!err.is_transient(), "lineage failure must be non-transient: {err:?}");
    assert!(
        err.to_string().contains("foreign writer"),
        "error must name the suspicion: {err}"
    );
    assert_eq!(bucket_l0s(&bucket), vec![(1, 1), (2, 2)], "the failed push must not upload");

    // The next push re-runs the lineage check: rebaseline onto B's head
    // (discarding A's offline L0 2 — its content is still in A's database)…
    let r = w.push().unwrap();
    assert_eq!(r.txid, Txid(2), "rebaseline must adopt the bucket position");
    assert_eq!(r.uploaded, 0);

    // …and the push after that snapshots A's full database at 3.
    conn_a.execute("INSERT INTO t (v) VALUES ('a3-0')", []).unwrap();
    let r = w.push().unwrap();
    assert_eq!((r.txid, r.uploaded, r.remote_txid), (Txid(3), 1, Txid(3)));
    assert_snapshot_l0(&bucket, Txid(3));

    assert_bucket_restores_to(&bucket, &db_a, &tmp.path().join("replica.db"), Txid(3));
}

/// O4: a REGRESSED bucket — wiped and reseeded by a foreign writer to a
/// TXID below this lineage's verified position — must rebaseline, never
/// silently continue incrementally: the reseeder's files at or below
/// remote_max describe ITS transactions, not ours, so our L0s' pre-state is
/// not in the bucket.
#[test]
fn regressed_bucket_rebaselines_and_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let db_a = tmp.path().join("a.db");
    let db_b = tmp.path().join("b.db");
    let bucket = tmp.path().join("bucket");
    let meta_a = meta_dir_of(&db_a);
    let conn_a = create_db(&db_a);

    // Device A pushes 1..5.
    {
        let mut w = Writer::open(
            &db_a,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        for i in 0..5 {
            conn_a.execute("INSERT INTO t (v) VALUES (?1)", [format!("a-{i}")]).unwrap();
            w.push().unwrap();
        }
    }
    assert_eq!(
        std::fs::read_to_string(meta_a.join("verified-pos")).unwrap().trim(),
        format!("{}", Txid(5)),
    );

    // The bucket is wiped and a second, fresh device reseeds it to txid 3.
    std::fs::remove_dir_all(&bucket).unwrap();
    {
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
    }

    // Device A returns: bucket max 3 < verified 5 — foreign lineage. The
    // push must rebaseline (upload nothing), NOT stack A's incrementals on
    // the reseeder's base.
    let mut w = Writer::open(
        &db_a,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    conn_a.execute("INSERT INTO t (v) VALUES ('a-post')", []).unwrap();
    let r = w.push().unwrap();
    assert_eq!(r.txid, Txid(3), "rebaseline must adopt the reseeded bucket position");
    assert_eq!(r.uploaded, 0, "the rebaselining push must upload nothing");

    // The next push lands A's FULL snapshot at remote_max+1.
    let r = w.push().unwrap();
    assert_eq!((r.txid, r.uploaded, r.remote_txid), (Txid(4), 1, Txid(4)));
    assert_snapshot_l0(&bucket, Txid(4));

    assert_bucket_restores_to(&bucket, &db_a, &tmp.path().join("replica.db"), Txid(4));
}

/// O5: a WIPED (empty) bucket below a verified lineage reseeds from
/// scratch: the local L0 chain is discarded and the next push writes a full
/// snapshot as TXID 1 — an empty bucket has no base for any incremental.
#[test]
fn wiped_bucket_reseeds_from_scratch() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let conn = create_db(&db_path);

    {
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        for i in 0..3 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("pre-{i}")]).unwrap();
            w.push().unwrap();
        }
    }

    std::fs::remove_dir_all(&bucket).unwrap();

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    conn.execute("INSERT INTO t (v) VALUES ('post-wipe')", []).unwrap();
    let r = w.push().unwrap();
    assert_eq!(r.txid, Txid(0), "the reseeding push must adopt the empty bucket");
    assert_eq!(r.uploaded, 0);

    let r = w.push().unwrap();
    assert_eq!((r.txid, r.uploaded, r.remote_txid), (Txid(1), 1, Txid(1)));
    assert_snapshot_l0(&bucket, Txid(1));
    assert_eq!(bucket_l0s(&bucket), vec![(1, 1)]);

    assert_bucket_restores_to(&bucket, &db_path, &tmp.path().join("replica.db"), Txid(1));
}

/// O6: litestream-Go → liters handoff. A legacy meta dir (no verified-pos
/// file) with an offline L0 backlog ahead of the bucket must pass the
/// lineage check, upload the backlog, and PERSIST the verified position as
/// it goes — so the next process start does not read its own uploads as
/// foreign data and spuriously rebaseline.
#[test]
fn legacy_meta_dir_migrates_verified_pos_without_rebaseline() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let meta_dir = meta_dir_of(&db_path);
    let conn = create_db(&db_path);

    // Bucket at 2, then an offline push leaves local L0 3 unuploaded.
    {
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        for i in 0..2 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("pre-{i}")]).unwrap();
            w.push().unwrap();
        }
    }
    {
        let mut w =
            Writer::open(&db_path, Box::new(OfflineClient), WriterOptions::default()).unwrap();
        conn.execute("INSERT INTO t (v) VALUES ('offline-0')", []).unwrap();
        assert!(w.push().expect_err("offline push must fail").is_transient());
        assert_eq!(w.pos().unwrap().txid, Txid(3));
    }

    // Simulate the legacy dir: litestream-Go never wrote a verified-pos file.
    std::fs::remove_file(meta_dir.join("verified-pos")).unwrap();

    // First liters push: the check passes on local-L0 evidence (bucket 2 <=
    // local 3), the backlog uploads, and the verified position must be
    // persisted THROUGH the backlog (the in-memory baseline is aligned down
    // to the bucket before upload, so the per-file persistence fires).
    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    let r = w.push().unwrap();
    assert_eq!((r.txid, r.uploaded, r.remote_txid), (Txid(3), 1, Txid(3)));
    assert_eq!(
        std::fs::read_to_string(meta_dir.join("verified-pos")).unwrap().trim(),
        format!("{}", Txid(3)),
        "the backlog upload must persist the verified position",
    );
    drop(w);

    // Next process start: bucket max == persisted verified position, so the
    // check passes and the next push continues INCREMENTALLY (a spurious
    // rebaseline would upload nothing here and then snapshot).
    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    conn.execute("INSERT INTO t (v) VALUES ('online-0')", []).unwrap();
    let r = w.push().unwrap();
    assert_eq!((r.txid, r.uploaded, r.remote_txid), (Txid(4), 1, Txid(4)));
    // Incremental, not a snapshot: only the touched pages travel.
    let dir = DirReplicaClient::new(&bucket);
    let mut rd = dir.open_ltx_file(0, Txid(4), Txid(4), 0, 0).unwrap();
    let mut buf = Vec::new();
    rd.read_to_end(&mut buf).unwrap();
    let (hdr, _, index) = ltx::Decoder::new(std::io::Cursor::new(&buf)).verify().unwrap();
    assert!(
        (index.len() as u32) < hdr.commit,
        "push after migration must stay incremental (no spurious rebaseline), \
         got {} of {} pages",
        index.len(),
        hdr.commit,
    );

    assert_bucket_restores_to(&bucket, &db_path, &tmp.path().join("replica.db"), Txid(4));
}

/// O7: reset_local() recovery against a populated bucket. A corrupt local
/// L0 wedges push with Error::LocalLtx; reset_local clears local state so
/// the next push rebaselines onto the bucket head and the one after emits a
/// FULL snapshot at remote_max+1 — never an incremental whose pre-state the
/// bucket lacks.
#[test]
fn reset_local_recovers_from_corrupt_l0() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let meta_dir = meta_dir_of(&db_path);
    let conn = create_db(&db_path);

    {
        let mut w = Writer::open(
            &db_path,
            Box::new(DirReplicaClient::new(&bucket)),
            WriterOptions::default(),
        )
        .unwrap();
        for i in 0..3 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("pre-{i}")]).unwrap();
            w.push().unwrap();
        }
    }

    // Cycle the WAL (writer dropped, so the checkpoint can run): the
    // adopted remote L0's salts now describe a dead WAL generation, making
    // the post-reset snapshot deterministic. (Without this, the adopted L0
    // — this device's own upload, still matching the live WAL — would let
    // verify() legitimately resume incrementally.)
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

    // Corrupt the newest (and only, post-prune) local L0.
    corrupt_local_l0(&meta_dir, Txid(3));

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    conn.execute("INSERT INTO t (v) VALUES ('post-corrupt')", []).unwrap();
    let err = w.push().expect_err("push over a corrupt local L0 must fail");
    assert!(
        matches!(err, liters::Error::LocalLtx { .. }),
        "expected LocalLtx, got {err:?}"
    );

    // The documented recovery.
    w.reset_local().unwrap();
    assert_eq!(
        std::fs::read_to_string(meta_dir.join("verified-pos")).unwrap().trim(),
        format!("{}", Txid(0)),
    );

    // Next push rebaselines onto the bucket head, uploading nothing…
    let r = w.push().unwrap();
    assert_eq!(r.txid, Txid(3), "reset + push must adopt the bucket position");
    assert_eq!(r.uploaded, 0);

    // …and the push after that lands the full snapshot at 4.
    let r = w.push().unwrap();
    assert_eq!((r.txid, r.uploaded, r.remote_txid), (Txid(4), 1, Txid(4)));
    assert_snapshot_l0(&bucket, Txid(4));

    assert_bucket_restores_to(&bucket, &db_path, &tmp.path().join("replica.db"), Txid(4));
}

/// O8: reset_local() with an EMPTY bucket (nothing was ever uploaded — the
/// corrupt L0 came from offline accumulation): the next push must snapshot
/// the whole database from scratch as TXID 1.
#[test]
fn reset_local_snapshots_from_scratch_on_empty_bucket() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("app.db");
    let bucket = tmp.path().join("bucket");
    let meta_dir = meta_dir_of(&db_path);
    let conn = create_db(&db_path);

    // Offline-only history: local L0s 1..2, bucket untouched.
    {
        let mut w =
            Writer::open(&db_path, Box::new(OfflineClient), WriterOptions::default()).unwrap();
        for i in 0..2 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("off-{i}")]).unwrap();
            assert!(w.push().expect_err("offline push must fail").is_transient());
        }
        assert_eq!(w.pos().unwrap().txid, Txid(2));
    }
    corrupt_local_l0(&meta_dir, Txid(2));

    let mut w = Writer::open(
        &db_path,
        Box::new(DirReplicaClient::new(&bucket)),
        WriterOptions::default(),
    )
    .unwrap();
    conn.execute("INSERT INTO t (v) VALUES ('post-corrupt')", []).unwrap();
    let err = w.push().expect_err("push over a corrupt local L0 must fail");
    assert!(matches!(err, liters::Error::LocalLtx { .. }), "expected LocalLtx, got {err:?}");

    w.reset_local().unwrap();

    // Empty bucket: one push suffices — the sync itself snapshots from
    // scratch (position zero) and the upload seeds the bucket at TXID 1.
    let r = w.push().unwrap();
    assert_eq!((r.txid, r.uploaded, r.remote_txid), (Txid(1), 1, Txid(1)));
    assert_snapshot_l0(&bucket, Txid(1));

    assert_bucket_restores_to(&bucket, &db_path, &tmp.path().join("replica.db"), Txid(1));
}
