//! S3 backend oracle tests against a local MinIO server, including the
//! critical interop check: stock `litestream restore s3://…` against a
//! bucket written entirely by the Rust S3 client.
//!
//! Requires a `minio` binary on PATH (brew install minio); skips otherwise.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use liters::{Replica, ReplicaClient, ReplicaOptions, Writer, WriterOptions};
use liters_storage::{S3Config, S3ReplicaClient};
use ltx::Txid;
use rusqlite::Connection;

const ACCESS_KEY: &str = "liters-test-key";
const SECRET_KEY: &str = "liters-test-secret";
const BUCKET: &str = "liters-test";

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

struct Minio {
    child: Child,
    pub endpoint: String,
    _data: tempfile::TempDir,
}

impl Drop for Minio {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn start_minio() -> Option<Minio> {
    let minio_bin = which_minio()?;
    let data = tempfile::tempdir().unwrap();
    // A directory inside the data root is served as a bucket.
    std::fs::create_dir_all(data.path().join(BUCKET)).unwrap();

    let port = free_port();
    let console_port = free_port();
    let child = Command::new(minio_bin)
        .args([
            "server",
            data.path().to_str().unwrap(),
            "--address",
            &format!("127.0.0.1:{port}"),
            "--console-address",
            &format!("127.0.0.1:{console_port}"),
        ])
        .env("MINIO_ROOT_USER", ACCESS_KEY)
        .env("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    let endpoint = format!("http://127.0.0.1:{port}");
    let minio = Minio { child, endpoint: endpoint.clone(), _data: data };

    // Wait for readiness.
    for _ in 0..100 {
        let ready = Command::new("curl")
            .args(["-sf", "-o", "/dev/null", &format!("{endpoint}/minio/health/ready")])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if ready {
            return Some(minio);
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    eprintln!("SKIP: minio did not become ready");
    None
}

fn which_minio() -> Option<PathBuf> {
    let out = Command::new("which").arg("minio").output().ok()?;
    if !out.status.success() {
        eprintln!("SKIP: minio binary not found");
        return None;
    }
    Some(PathBuf::from(String::from_utf8_lossy(&out.stdout).trim().to_string()))
}

fn s3_client(endpoint: &str, prefix: &str) -> S3ReplicaClient {
    S3ReplicaClient::new(S3Config {
        bucket: BUCKET.into(),
        prefix: prefix.into(),
        endpoint: Some(endpoint.into()),
        region: Some("us-east-1".into()),
        access_key_id: Some(ACCESS_KEY.into()),
        secret_access_key: Some(SECRET_KEY.into()),
        force_path_style: true,
        allow_http: true,
    })
    .unwrap()
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

#[test]
fn s3_layout_and_roundtrip_with_litestream_oracle() {
    let Some(oracle) = oracle_dir() else { return };
    let Some(minio) = start_minio() else { return };
    let tmp = tempfile::tempdir().unwrap();

    // App database.
    let db_path = tmp.path().join("app.db");
    let app = Connection::open(&db_path).unwrap();
    app.pragma_update(None, "journal_mode", "WAL").unwrap();
    app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();

    // Rust writer replicates to MinIO through the S3 client.
    let mut w = Writer::open(
        &db_path,
        Box::new(s3_client(&minio.endpoint, "dbs/app")),
        WriterOptions::default(),
    )
    .unwrap();

    for i in 0..5 {
        for j in 0..20 {
            app.execute("INSERT INTO t (v) VALUES (?1)", [format!("s3-{i}-{j}")]).unwrap();
        }
        let r = w.push().unwrap();
        assert!(r.synced);
        assert_eq!(r.remote_txid, Txid(1 + i as u64));
    }

    // Layout check: keys must use the %04x level scheme with no ltx/ segment.
    let probe = s3_client(&minio.endpoint, "dbs/app");
    let files = probe.ltx_files(0, Txid(0), false).unwrap();
    assert_eq!(files.len(), 5);
    assert_eq!(files[0].min_txid, Txid(1));

    // Ranged read parity: bytes 4..8 are the header flags field.
    let mut r = probe.open_ltx_file(0, Txid(1), Txid(1), 4, 4).unwrap();
    let mut flags = Vec::new();
    std::io::Read::read_to_end(&mut r, &mut flags).unwrap();
    assert_eq!(flags, ltx::HEADER_FLAG_NO_CHECKSUM.to_be_bytes());

    // Metadata timestamps round-trip (use_metadata=true).
    let with_meta = probe.ltx_files(0, Txid(0), true).unwrap();
    assert!(with_meta.iter().all(|f| f.created_at.is_some()));

    // THE oracle: stock litestream restores from the MinIO bucket the Rust
    // client wrote.
    let restored = tmp.path().join("restored.db");
    let url = format!(
        "s3://{BUCKET}/dbs/app?endpoint={}&forcePathStyle=true&region=us-east-1",
        minio.endpoint
    );
    let output = Command::new(oracle.join("litestream"))
        .args(["restore", "-o", restored.to_str().unwrap(), &url])
        .env("LITESTREAM_ACCESS_KEY_ID", ACCESS_KEY)
        .env("LITESTREAM_SECRET_ACCESS_KEY", SECRET_KEY)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "litestream restore from minio failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(rows_of(&restored), rows_of(&db_path));

    // Reverse direction: litestream replicates to MinIO; our S3 client and
    // Replica consume it.
    let db2 = tmp.path().join("app2.db");
    let app2 = Connection::open(&db2).unwrap();
    app2.pragma_update(None, "journal_mode", "WAL").unwrap();
    app2.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for i in 0..40 {
        app2.execute("INSERT INTO t (v) VALUES (?1)", [format!("go-{i}")]).unwrap();
    }
    let url2 = format!(
        "s3://{BUCKET}/dbs/app2?endpoint={}&forcePathStyle=true&region=us-east-1",
        minio.endpoint
    );
    let output = Command::new(oracle.join("litestream"))
        .args(["replicate", "-exec", "sleep 3", db2.to_str().unwrap(), &url2])
        .env("LITESTREAM_ACCESS_KEY_ID", ACCESS_KEY)
        .env("LITESTREAM_SECRET_ACCESS_KEY", SECRET_KEY)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "litestream replicate to minio failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let replica_path = tmp.path().join("replica2.db");
    let mut rep = Replica::open(
        &replica_path,
        Box::new(s3_client(&minio.endpoint, "dbs/app2")),
        ReplicaOptions::default(),
    );
    let r = rep.sync().unwrap();
    assert!(r.restored);
    assert_eq!(rows_of(&replica_path), rows_of(&db2));

    // Maintenance over S3: compaction + snapshot + retention, bucket must
    // stay restorable.
    for i in 0..3 {
        app.execute("INSERT INTO t (v) VALUES (?1)", [format!("post-{i}")]).unwrap();
        w.push().unwrap();
    }
    let report = w
        .maintain(&liters::MaintenanceOptions {
            level_intervals: vec![std::time::Duration::ZERO],
            snapshot_interval: std::time::Duration::ZERO,
            snapshot_retention: std::time::Duration::ZERO,
            l0_retention: std::time::Duration::ZERO,
            retention_enabled: true,
        })
        .unwrap();
    assert!(report.compacted_levels.contains(&1));
    assert!(report.snapshot.is_some());

    let restored2 = tmp.path().join("restored-after-maint.db");
    let output = Command::new(oracle.join("litestream"))
        .args(["restore", "-o", restored2.to_str().unwrap(), &url])
        .env("LITESTREAM_ACCESS_KEY_ID", ACCESS_KEY)
        .env("LITESTREAM_SECRET_ACCESS_KEY", SECRET_KEY)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "litestream restore after maintenance failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(rows_of(&restored2), rows_of(&db_path));
}
