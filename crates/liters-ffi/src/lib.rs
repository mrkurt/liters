//! UniFFI surface for liters: blocking, mobile-friendly wrappers around the
//! Writer and Replica. Designed for iOS BGTaskScheduler / Android WorkManager
//! usage: every operation is short, resumable, and crash-safe, so a killed
//! task resumes on the next call rather than restarting.

use std::sync::Mutex;

uniffi::setup_scaffolding!();

/// Where a database's replica lives.
#[derive(uniffi::Enum)]
pub enum Storage {
    /// Local directory in litestream's `file` layout (testing, shared
    /// containers).
    Dir { path: String },
    /// S3-compatible object storage in litestream's S3 layout.
    S3 {
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        force_path_style: bool,
        allow_http: bool,
    },
    /// Another liters instance serving its bucket over HTTP
    /// (`http://host:port[/path]`). Always valid as a Replica source (sync
    /// in a loop to follow; the Rust streaming `follow()` is not exposed
    /// over FFI yet). Valid as a Writer destination when the server runs
    /// with `writable: true` — that is push replication: this device dials
    /// out and pushes, the listening liters receives. Read-only servers
    /// reject the first push with a clear error.
    Http { url: String },
}

impl Storage {
    fn into_client(self) -> Result<Box<dyn liters::ReplicaClient>, LitersError> {
        match self {
            Storage::Dir { path } => Ok(Box::new(liters::DirReplicaClient::new(path))),
            Storage::S3 {
                bucket,
                prefix,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                force_path_style,
                allow_http,
            } => {
                let client = liters_storage::S3ReplicaClient::new(liters_storage::S3Config {
                    bucket,
                    prefix,
                    endpoint,
                    region,
                    access_key_id,
                    secret_access_key,
                    force_path_style,
                    allow_http,
                })
                .map_err(to_ffi_error)?;
                Ok(Box::new(client))
            }
            Storage::Http { url } => {
                let client =
                    liters_storage::HttpReplicaClient::new(url).map_err(to_ffi_error)?;
                Ok(Box::new(client))
            }
        }
    }
}

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum LitersError {
    /// The bucket's history no longer matches the local state; call
    /// `Replica.reset()` (or construct with `auto_reset`) to re-restore.
    #[error("diverged: {message}")]
    Diverged { message: String },
    /// Storage/network failure; safe to retry the same call later.
    #[error("storage: {message}")]
    Storage { message: String },
    #[error("{message}")]
    Other { message: String },
}

fn to_ffi_error<E: std::fmt::Display>(e: E) -> LitersError {
    LitersError::Other { message: e.to_string() }
}

fn map_error(e: liters::Error) -> LitersError {
    match &e {
        liters::Error::Diverged { .. } => LitersError::Diverged { message: e.to_string() },
        liters::Error::Storage(_) => LitersError::Storage { message: e.to_string() },
        _ => LitersError::Other { message: e.to_string() },
    }
}

#[derive(uniffi::Record)]
pub struct PushSummary {
    /// Local replication position (TXID) after the push.
    pub txid: u64,
    /// Whether new committed content was captured into an L0 file.
    pub synced: bool,
    /// Number of files uploaded by this push.
    pub uploaded: u64,
    /// Bucket-side max TXID after the push.
    pub remote_txid: u64,
    /// Whether a WAL checkpoint ran.
    pub checkpointed: bool,
}

#[derive(uniffi::Record)]
pub struct SyncSummary {
    /// Whether a full restore ran (vs. incremental application).
    pub restored: bool,
    pub from_txid: u64,
    pub to_txid: u64,
}

#[derive(uniffi::Record)]
pub struct MaintenanceSummary {
    pub compacted_levels: Vec<u8>,
    pub snapshot_txid: Option<u64>,
    pub deleted_files: u64,
}

/// Replicates a local, app-owned SQLite database to a bucket. Call `push()`
/// after commits (or batched); call `maintain()` opportunistically (wifi +
/// charging) to compact and enforce retention.
#[derive(uniffi::Object)]
pub struct LitersWriter {
    inner: Mutex<liters::Writer>,
}

#[uniffi::export]
impl LitersWriter {
    /// Opens a writer for an existing SQLite database. Switches the database
    /// to WAL mode and takes over checkpointing (do not run your own
    /// `wal_checkpoint`; `wal_autocheckpoint` on your connections is fine —
    /// it will simply never fire while the writer holds its read lock).
    #[uniffi::constructor]
    pub fn new(db_path: String, storage: Storage) -> Result<Self, LitersError> {
        let client = storage.into_client()?;
        let writer = liters::Writer::open(db_path, client, liters::WriterOptions::default())
            .map_err(map_error)?;
        Ok(LitersWriter { inner: Mutex::new(writer) })
    }

    /// Captures all committed changes into the bucket. Short and resumable:
    /// on failure, the next push picks up exactly where this one stopped.
    pub fn push(&self) -> Result<PushSummary, LitersError> {
        let mut w = self.inner.lock().unwrap();
        let r = w.push().map_err(map_error)?;
        Ok(PushSummary {
            txid: r.txid.0,
            synced: r.synced,
            uploaded: r.uploaded,
            remote_txid: r.remote_txid.0,
            checkpointed: r.checkpointed,
        })
    }

    /// Runs due compaction, snapshotting, and retention with litestream's
    /// default cadences.
    pub fn maintain(&self) -> Result<MaintenanceSummary, LitersError> {
        let mut w = self.inner.lock().unwrap();
        let r = w.maintain(&liters::MaintenanceOptions::default()).map_err(map_error)?;
        Ok(MaintenanceSummary {
            compacted_levels: r.compacted_levels,
            snapshot_txid: r.snapshot.map(|t| t.0),
            deleted_files: r.deleted as u64,
        })
    }

    /// Forces a full snapshot to the bucket now.
    pub fn snapshot(&self) -> Result<Option<u64>, LitersError> {
        let mut w = self.inner.lock().unwrap();
        Ok(w.snapshot().map_err(map_error)?.map(|t| t.0))
    }

    /// Current local replication position.
    pub fn position(&self) -> Result<u64, LitersError> {
        let mut w = self.inner.lock().unwrap();
        Ok(w.pos().map_err(map_error)?.txid.0)
    }
}

/// A local read-only materialization of a bucket. `sync()` restores on first
/// use, then applies changes incrementally. Open `db_path()` read-only with
/// your platform SQLite; do not hold read transactions across `sync()` calls
/// from the same process.
#[derive(uniffi::Object)]
pub struct LitersReplica {
    inner: Mutex<liters::Replica>,
    db_path: String,
}

#[uniffi::export]
impl LitersReplica {
    /// `auto_reset`: on bucket divergence, silently delete the local replica
    /// and re-restore instead of raising `Diverged`.
    #[uniffi::constructor]
    pub fn new(db_path: String, storage: Storage, auto_reset: bool) -> Result<Self, LitersError> {
        let client = storage.into_client()?;
        let replica = liters::Replica::open(
            db_path.clone(),
            client,
            liters::ReplicaOptions { auto_reset, ..Default::default() },
        );
        Ok(LitersReplica { inner: Mutex::new(replica), db_path })
    }

    /// Brings the local replica up to date. Short and resumable; safe to
    /// call from a background task with a tight deadline.
    pub fn sync(&self) -> Result<SyncSummary, LitersError> {
        let mut r = self.inner.lock().unwrap();
        let s = r.sync().map_err(map_error)?;
        Ok(SyncSummary {
            restored: s.restored,
            from_txid: s.from_txid.0,
            to_txid: s.to_txid.0,
        })
    }

    /// Last applied TXID (zero if never synced).
    pub fn position(&self) -> Result<u64, LitersError> {
        let r = self.inner.lock().unwrap();
        Ok(r.position().map_err(map_error)?.0)
    }

    /// Path of the local database file to open read-only.
    pub fn db_path(&self) -> String {
        self.db_path.clone()
    }

    /// Deletes the local replica; the next `sync()` restores from scratch.
    pub fn reset(&self) -> Result<(), LitersError> {
        let r = self.inner.lock().unwrap();
        r.reset().map_err(map_error)
    }
}
