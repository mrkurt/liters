//! Layout-parity tests for the directory backend, including the critical
//! oracle check: stock `litestream restore file://…` must succeed against a
//! bucket written by DirReplicaClient.

use std::io::Cursor;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, UNIX_EPOCH};

use ltx::{Encoder, Header, Txid, HEADER_FLAG_NO_CHECKSUM};
use liters_storage::{DirReplicaClient, ReplicaClient, StorageError};

fn oracle_dir() -> Option<PathBuf> {
    let dir = std::env::var_os("LITERS_ORACLE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/oracle")
        });
    if dir.join("litestream").exists() {
        Some(dir)
    } else {
        eprintln!("SKIP: oracle binaries not found in {dir:?}; run `make oracle`");
        None
    }
}

/// Encodes a NoChecksum snapshot L0 file (litestream first-sync style) from
/// raw pages.
fn encode_snapshot_l0(page_size: u32, pages: &[Vec<u8>], timestamp: i64) -> Vec<u8> {
    let mut enc = Encoder::new(Vec::new());
    enc.encode_header(Header {
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size,
        commit: pages.len() as u32,
        min_txid: Txid(1),
        max_txid: Txid(1),
        timestamp,
        ..Default::default()
    })
    .unwrap();
    for (i, page) in pages.iter().enumerate() {
        enc.encode_page((i + 1) as u32, page).unwrap();
    }
    let (buf, _, _) = enc.finish().unwrap();
    buf
}

fn make_db(path: &std::path::Path, rows: usize) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for i in 0..rows {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("value-{i}")]).unwrap();
    }
    conn.pragma_update(None, "wal_checkpoint", "TRUNCATE").unwrap();
}

fn db_pages(path: &std::path::Path) -> (u32, Vec<Vec<u8>>) {
    let bytes = std::fs::read(path).unwrap();
    let mut page_size = u32::from(u16::from_be_bytes([bytes[16], bytes[17]]));
    if page_size == 1 {
        page_size = 65536;
    }
    (page_size, bytes.chunks(page_size as usize).map(|c| c.to_vec()).collect())
}

#[test]
fn layout_and_listing() {
    let tmp = tempfile::tempdir().unwrap();
    let client = DirReplicaClient::new(tmp.path().join("replica"));

    // Empty level: empty list, no error.
    assert!(client.ltx_files(0, Txid(0), false).unwrap().is_empty());

    let pages = vec![vec![1u8; 512]; 3];
    let ts = 1_720_000_000_123i64;
    let bytes = encode_snapshot_l0(512, &pages, ts);

    let info = client.write_ltx_file(0, Txid(1), Txid(1), &mut Cursor::new(&bytes)).unwrap();
    assert_eq!(info.size, bytes.len() as u64);

    // Exact on-disk path matches litestream's file layout.
    let expect = tmp
        .path()
        .join("replica/ltx/0/0000000000000001-0000000000000001.ltx");
    assert!(expect.exists(), "expected {expect:?}");
    assert_eq!(std::fs::read(&expect).unwrap(), bytes);

    // mtime preserves the header timestamp.
    let mtime = std::fs::metadata(&expect).unwrap().modified().unwrap();
    assert_eq!(mtime, UNIX_EPOCH + Duration::from_millis(ts as u64));

    // A second file at a different TXID; listing is sorted and seek filters.
    let mut enc = Encoder::new(Vec::new());
    enc.encode_header(Header {
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size: 512,
        commit: 3,
        min_txid: Txid(2),
        max_txid: Txid(2),
        timestamp: ts + 1000,
        ..Default::default()
    })
    .unwrap();
    enc.encode_page(2, &pages[1]).unwrap();
    let (bytes2, _, _) = enc.finish().unwrap();
    client.write_ltx_file(0, Txid(2), Txid(2), &mut Cursor::new(&bytes2)).unwrap();

    let all = client.ltx_files(0, Txid(0), false).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].min_txid, Txid(1));
    assert_eq!(all[1].min_txid, Txid(2));

    let seeked = client.ltx_files(0, Txid(2), false).unwrap();
    assert_eq!(seeked.len(), 1);
    assert_eq!(seeked[0].min_txid, Txid(2));

    // Ranged read: offset 4, size 4 == bytes[4..8] (flags field).
    let mut r = client.open_ltx_file(0, Txid(1), Txid(1), 4, 4).unwrap();
    let mut got = Vec::new();
    r.read_to_end(&mut got).unwrap();
    assert_eq!(got, &bytes[4..8]);

    // Missing file -> NotFound.
    match client.open_ltx_file(0, Txid(99), Txid(99), 0, 0) {
        Err(StorageError::NotFound { .. }) => {}
        other => panic!("expected NotFound, got {:?}", other.map(|_| ())),
    }

    // Delete is idempotent.
    client.delete_ltx_files(&all).unwrap();
    client.delete_ltx_files(&all).unwrap();
    assert!(client.ltx_files(0, Txid(0), false).unwrap().is_empty());
}

#[test]
fn litestream_restores_our_bucket() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();

    // Build a real SQLite database and snapshot it into the bucket as the
    // first L0 file, exactly like litestream's first sync.
    let db = tmp.path().join("src.db");
    make_db(&db, 200);
    let (page_size, pages) = db_pages(&db);
    let bytes = encode_snapshot_l0(page_size, &pages, 1_720_000_000_000);

    let replica_root = tmp.path().join("replica");
    let client = DirReplicaClient::new(&replica_root);
    client.write_ltx_file(0, Txid(1), Txid(1), &mut Cursor::new(&bytes)).unwrap();

    // Stock litestream must restore it.
    let out_db = tmp.path().join("restored.db");
    let output = Command::new(oracle.join("litestream"))
        .args([
            "restore",
            "-o",
            out_db.to_str().unwrap(),
            &format!("file://{}", replica_root.display()),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "litestream restore failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(
        std::fs::read(&out_db).unwrap(),
        std::fs::read(&db).unwrap(),
        "restored db differs from source"
    );

    // And the restored db must pass integrity check + contain our rows.
    let conn = rusqlite::Connection::open(&out_db).unwrap();
    let ok: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
        .unwrap();
    assert_eq!(ok, "ok");
    let n: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 200);
}

#[test]
fn we_read_litestream_written_bucket() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();

    // Let litestream replicate a database into a file bucket, then list/read
    // it with our client.
    let db = tmp.path().join("src.db");
    make_db(&db, 100);

    let replica_root = tmp.path().join("replica");
    // One-shot replication: `litestream replicate -exec` exits when the
    // subprocess does, syncing once more on shutdown. Sleep must exceed the
    // 1s sync interval for the first upload to land.
    let output = Command::new(oracle.join("litestream"))
        .args([
            "replicate",
            "-exec",
            "sleep 3",
            db.to_str().unwrap(),
            &format!("file://{}", replica_root.display()),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "litestream replicate failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let client = DirReplicaClient::new(&replica_root);
    let files = client.ltx_files(0, Txid(0), false).unwrap();
    assert!(!files.is_empty(), "no L0 files written by litestream");
    assert_eq!(files[0].min_txid, Txid(1));

    // Every listed file must decode and verify with our ltx crate.
    for info in &files {
        let mut r = client
            .open_ltx_file(info.level, info.min_txid, info.max_txid, 0, 0)
            .unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).unwrap();
        assert_eq!(buf.len() as u64, info.size);
        let dec = ltx::Decoder::new(Cursor::new(&buf));
        let (hdr, _, _) = dec.verify().unwrap_or_else(|e| {
            panic!("verify {}-{}: {e}", info.min_txid, info.max_txid)
        });
        assert_eq!(hdr.min_txid, info.min_txid);
        assert_eq!(hdr.max_txid, info.max_txid);
        assert_eq!(hdr.flags, ltx::HEADER_FLAG_NO_CHECKSUM);
    }
}
