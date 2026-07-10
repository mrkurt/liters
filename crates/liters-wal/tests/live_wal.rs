//! Tests against real WALs produced by SQLite (via rusqlite), including
//! uncommitted-tail exclusion and VACUUM shrink handling.

use liters_wal::{DbHeader, WalReader};
use rusqlite::Connection;

fn open_wal_db(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    // Keep everything in the WAL: no auto checkpoints.
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn
}

#[test]
fn page_map_reflects_committed_state() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    let wal_path = tmp.path().join("test.db-wal");

    let conn = open_wal_db(&db_path);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    for i in 0..50 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [vec![i as u8; 100]]).unwrap();
    }

    let wal = std::fs::read(&wal_path).unwrap();
    let mut r = WalReader::new(wal.as_slice()).unwrap();
    let pm = r.page_map().unwrap();
    assert!(pm.commit > 0, "expected committed transactions");
    assert!(!pm.pages.is_empty());
    // Every mapped page is within the database size.
    for &pgno in pm.pages.keys() {
        assert!(pgno >= 1 && pgno <= pm.commit, "pgno {pgno} out of range");
    }
    // max_offset must be frame-aligned and within the file.
    let frame = 24 + r.page_size() as u64;
    assert_eq!((pm.max_offset - 32) % frame, 0);
    assert!(pm.max_offset <= wal.len() as u64);

    // The page images in the WAL at the mapped offsets reconstruct the
    // committed database: check page 1 header magic.
    let off = pm.pages.get(&1).copied();
    if let Some(off) = off {
        let data = &wal[(off + 24) as usize..(off + 24) as usize + r.page_size() as usize];
        assert_eq!(&data[..16], b"SQLite format 3\0");
        let hdr = DbHeader::parse(data).unwrap();
        assert_eq!(hdr.page_size, r.page_size());
    }
    drop(conn);
}

#[test]
fn uncommitted_tail_excluded() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    let wal_path = tmp.path().join("test.db-wal");

    let conn = open_wal_db(&db_path);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    conn.execute("INSERT INTO t (v) VALUES (x'01')", []).unwrap();

    let committed_map = {
        let wal = std::fs::read(&wal_path).unwrap();
        WalReader::new(wal.as_slice()).unwrap().page_map().unwrap()
    };

    // Start a transaction and write without committing; the WAL grows but the
    // page map must not change.
    conn.execute_batch("BEGIN").unwrap();
    for i in 0..20 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [vec![i as u8; 500]]).unwrap();
    }
    // (SQLite spills to the WAL on cache pressure; force it with a large txn.)
    let wal = std::fs::read(&wal_path).unwrap();
    let pm = WalReader::new(wal.as_slice()).unwrap().page_map().unwrap();
    assert_eq!(pm.commit, committed_map.commit, "uncommitted frames leaked into page map");
    assert_eq!(pm.max_offset, committed_map.max_offset);
    conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn vacuum_shrink_drops_out_of_range_pages() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    let wal_path = tmp.path().join("test.db-wal");

    let conn = open_wal_db(&db_path);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    for i in 0..200 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [vec![i as u8; 512]]).unwrap();
    }
    conn.execute("DELETE FROM t WHERE id > 10", []).unwrap();
    conn.execute_batch("VACUUM").unwrap();

    let wal = std::fs::read(&wal_path).unwrap();
    let mut r = WalReader::new(wal.as_slice()).unwrap();
    let pm = r.page_map().unwrap();
    assert!(pm.commit > 0);
    for &pgno in pm.pages.keys() {
        assert!(pgno <= pm.commit, "page {pgno} beyond final commit {}", pm.commit);
    }
    drop(conn);
}
