//! Shared helpers for ltx integration tests.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Locates the Go oracle binaries (`ltx`, `litestream`) built by `make oracle`.
/// Returns None (and prints a skip notice) if they are absent.
pub fn oracle_dir() -> Option<PathBuf> {
    let dir = std::env::var_os("LITERS_ORACLE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/oracle")
                .canonicalize()
                .unwrap_or_else(|_| PathBuf::from("target/oracle"))
        });
    if dir.join("ltx").exists() {
        Some(dir)
    } else {
        eprintln!("SKIP: oracle binaries not found in {dir:?}; run `make oracle`");
        None
    }
}

/// Runs an oracle binary, panicking with full output on failure.
pub fn run_oracle(dir: &Path, bin: &str, args: &[&str]) -> String {
    let out = Command::new(dir.join(bin))
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn {bin}: {e}"));
    assert!(
        out.status.success(),
        "{bin} {args:?} failed ({}):\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Runs an oracle binary, returning Err(stderr) on failure.
pub fn try_run_oracle(dir: &Path, bin: &str, args: &[&str]) -> Result<String, String> {
    let out = Command::new(dir.join(bin))
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn {bin}: {e}"));
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ))
    }
}

/// Creates a SQLite database at `path` with `rows` rows of varied content,
/// then fully checkpoints and closes it so the .db file is self-contained.
pub fn make_test_db(path: &Path, rows: usize) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, blob BLOB);
         CREATE INDEX t_name ON t (name);",
    )
    .unwrap();
    for i in 0..rows {
        conn.execute(
            "INSERT INTO t (name, blob) VALUES (?1, ?2)",
            rusqlite::params![
                format!("row-{i:06}"),
                vec![(i % 251) as u8; 64 + (i % 512)]
            ],
        )
        .unwrap();
    }
    conn.pragma_update(None, "wal_checkpoint", "TRUNCATE").unwrap();
    drop(conn);
}

/// Reads a SQLite database file and returns (page_size, pages).
pub fn read_db_pages(path: &Path) -> (u32, Vec<Vec<u8>>) {
    let bytes = std::fs::read(path).unwrap();
    assert!(bytes.len() >= 100, "not a sqlite db");
    let mut page_size = u32::from(u16::from_be_bytes([bytes[16], bytes[17]]));
    if page_size == 1 {
        page_size = 65536;
    }
    assert_eq!(bytes.len() % page_size as usize, 0, "db not page-aligned");
    let pages = bytes
        .chunks(page_size as usize)
        .map(|c| c.to_vec())
        .collect();
    (page_size, pages)
}
