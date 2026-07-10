//! Replica storage backends for liters, mirroring litestream's
//! `ReplicaClient` contract (replica_client.go:18-51).
//!
//! A replica client stores one database's LTX files, addressed by
//! `(level, min_txid, max_txid)`. State lives entirely in listings and
//! filenames — there are no manifest objects. Two on-storage layouts exist in
//! litestream v0.5.x:
//!
//! - file/GCS/Azure/SFTP: `{path}/ltx/{level decimal}/{min:016x}-{max:016x}.ltx`
//! - S3/OSS:              `{path}/{level:04x}/{min:016x}-{max:016x}.ltx`
//!
//! The [`DirReplicaClient`] implements the first (byte-identical to
//! litestream's `file` client, so stock `litestream restore file://…` works
//! against it); the S3 backend implements the second.

mod dir;
#[cfg(feature = "s3")]
mod s3;

pub use dir::DirReplicaClient;
#[cfg(feature = "s3")]
pub use s3::{S3Config, S3ReplicaClient};

use std::io::Read;

use ltx::{FileInfo, Txid};

/// The snapshot pseudo-level. Levels 0-8 are compaction levels.
/// (compaction_level.go:9)
pub const SNAPSHOT_LEVEL: u8 = 9;

/// Errors from replica storage.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The requested LTX file does not exist. Mirrors Go's `os.ErrNotExist`
    /// contract on `OpenLTXFile`; readers race compaction/GC and must treat
    /// this as re-plan, not failure. (s3/replica_client.go:1691)
    #[error("ltx file not found: L{level} {min_txid}-{max_txid}")]
    NotFound { level: u8, min_txid: Txid, max_txid: Txid },

    #[error("ltx: {0}")]
    Ltx(#[from] ltx::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Blocking client for one database's replica storage. Mirrors litestream's
/// `ReplicaClient` interface. (replica_client.go:18-51)
pub trait ReplicaClient: Send + Sync {
    /// Short backend name, e.g. `"file"` or `"s3"`.
    fn client_type(&self) -> &'static str;

    /// Lists LTX files at `level` with `min_txid >= seek`, sorted ascending by
    /// `(min_txid, max_txid)`. A missing level yields an empty list, not an
    /// error. `use_metadata` requests accurate per-file creation timestamps
    /// (an extra HEAD per object on S3; free on filesystems).
    fn ltx_files(&self, level: u8, seek: Txid, use_metadata: bool) -> Result<Vec<FileInfo>>;

    /// Opens an LTX file for reading. `offset`/`size` select a byte range;
    /// `size == 0` reads from `offset` to EOF. Missing file →
    /// [`StorageError::NotFound`].
    fn open_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        offset: u64,
        size: u64,
    ) -> Result<Box<dyn Read + Send>>;

    /// Writes an LTX file. Implementations peek the 100-byte header to
    /// extract the timestamp and persist it as storage metadata (mtime on
    /// filesystems, `litestream-timestamp` object metadata on S3). Writes are
    /// atomic and idempotent: re-writing the same key is harmless.
    fn write_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        rd: &mut dyn Read,
    ) -> Result<FileInfo>;

    /// Deletes LTX files. Missing files are not an error.
    fn delete_ltx_files(&self, infos: &[FileInfo]) -> Result<()>;

    /// Deletes everything under this client's path.
    fn delete_all(&self) -> Result<()>;
}

/// Returns the max-TXID file info at a level, or None if the level is empty.
/// (litestream's `MaxLTXFileInfo` equivalent.)
pub fn max_ltx_file_info(client: &dyn ReplicaClient, level: u8) -> Result<Option<FileInfo>> {
    let files = client.ltx_files(level, Txid(0), false)?;
    Ok(files.into_iter().max_by_key(|f| f.max_txid))
}
