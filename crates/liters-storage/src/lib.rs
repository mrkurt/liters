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

mod cancel;
mod dir;
#[cfg(feature = "http")]
mod http;
#[cfg(feature = "s3")]
mod s3;

pub use cancel::CancelToken;
pub use dir::DirReplicaClient;
#[cfg(feature = "http")]
pub use http::{
    Body, HttpClientOptions, HttpReplicaClient, HttpServer, HttpServerOptions, Mount, MountOptions,
    Request, Response, StreamBody,
};
#[cfg(feature = "s3")]
pub use s3::{S3Config, S3ReplicaClient};

use std::io::{Read, Write};

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

    /// Transient transport failure: connect/resolve failure, socket timeout,
    /// connection reset, stream dead-man, truncated body. Safe to retry.
    #[error("storage unavailable: {0}")]
    Unavailable(String),

    /// Authentication required or rejected.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// The server rejected a write for consistency/ownership reasons
    /// (TXID non-monotonic, writer fenced). NOT retryable as-is.
    #[error("conflict: {0}")]
    Conflict(String),

    /// The server is read-only.
    #[error("read-only: {0}")]
    ReadOnly(String),

    /// The operation was cancelled via [`CancelToken`].
    #[error("operation cancelled")]
    Cancelled,

    #[error("{0}")]
    Other(String),
}

impl StorageError {
    /// Whether retrying the same operation later can plausibly succeed:
    /// transport failures and I/O errors. Protocol, auth, consistency, and
    /// cancellation errors are not transient.
    pub fn is_transient(&self) -> bool {
        matches!(self, StorageError::Unavailable(_) | StorageError::Io(_))
    }
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

    /// Opens a live stream of level-0 LTX files starting at `seek` (the
    /// first TXID wanted, i.e. the follower's position + 1). Backends
    /// without streaming support return `Ok(None)` and callers fall back to
    /// polling [`ReplicaClient::ltx_files`].
    fn open_ltx_stream(&self, seek: Txid) -> Result<Option<Box<dyn LtxStream>>> {
        let _ = seek;
        Ok(None)
    }

    /// Installs a cancellation token observed by subsequent blocking
    /// operations on this client (best-effort granularity; backends may only
    /// check between operations). Passing a fresh token replaces the old
    /// one. Default: no-op.
    fn set_cancel(&self, _token: CancelToken) {}
}

/// A live change stream of complete level-0 LTX files, as produced by
/// [`ReplicaClient::open_ltx_stream`]. Streams deliver whole files (the
/// reader CRC-verifies each file before applying any page), never partial
/// ones.
pub trait LtxStream: Send {
    /// Blocks (bounded — implementations tick at roughly one-second
    /// granularity) until the next stream event. For [`StreamEvent::Ltx`]
    /// the complete file body has been copied into `sink` before this
    /// returns; `Idle` is only ever returned *between* frames, so a caller
    /// may discard and reuse `sink` across calls. Once `next` returns an
    /// error the stream is dead: drop it and reconnect.
    fn next(&mut self, sink: &mut dyn Write) -> Result<StreamEvent>;
}

/// One event from an [`LtxStream`]. Non-exhaustive: protocol evolution may
/// add events, and consumers must treat unknown events as "drop the stream
/// and resync via listings".
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamEvent {
    /// A complete level-0 LTX file was written to the sink.
    Ltx(FileInfo),
    /// Keepalive tick — nothing new. Server pings carry the bucket-wide max
    /// TXID so followers can detect divergence (bucket max below their
    /// position) while idle; timeout ticks carry `None`.
    Idle { bucket_max: Option<Txid> },
    /// The requested position is no longer available at level 0 (pruned by
    /// retention); `next` is the oldest available min TXID. The stream ends
    /// after this event — re-plan via listings.
    Gap { next: Txid },
    /// The bucket's max TXID is below the requested position: the bucket
    /// was wiped or reseeded. The stream ends after this event — re-sync to
    /// run divergence handling.
    Reset { bucket_max: Txid },
    /// The server ended the stream cleanly (e.g. shutdown).
    Closed,
}

/// Returns the max-TXID file info at a level, or None if the level is empty.
/// (litestream's `MaxLTXFileInfo` equivalent.)
pub fn max_ltx_file_info(client: &dyn ReplicaClient, level: u8) -> Result<Option<FileInfo>> {
    let files = client.ltx_files(level, Txid(0), false)?;
    Ok(files.into_iter().max_by_key(|f| f.max_txid))
}
