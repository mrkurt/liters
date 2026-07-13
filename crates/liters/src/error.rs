use ltx::Txid;

/// Errors from liters replication.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("ltx: {0}")]
    Ltx(#[from] ltx::Error),

    #[error("wal: {0}")]
    Wal(#[from] liters_wal::WalError),

    #[error("storage: {0}")]
    Storage(#[source] liters_storage::StorageError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// The database is not in WAL journal mode and could not be switched.
    /// (db.go:1021)
    #[error("enable wal failed, mode={0:?}")]
    EnableWalFailed(String),

    /// A local L0 LTX file expected by the writer is missing or corrupt.
    /// Recoverable by resetting local state (which forces a snapshot).
    #[error("local ltx file missing or corrupt: txid {txid}: {msg}")]
    LocalLtx { txid: Txid, msg: String },

    /// The local replica has diverged from the bucket (bucket wiped or
    /// reseeded at a lower TXID); a reset + full re-restore is required.
    #[error("replica diverged: local txid {local} ahead of bucket max {remote}")]
    Diverged { local: Txid, remote: Txid },

    /// The requested transaction cannot be reconstructed from the bucket —
    /// typically an empty (not-yet-seeded) bucket. Mirrors litestream's
    /// `ErrTxNotAvailable` (store.go:27); the display string is litestream's
    /// error text.
    #[error("transaction not available")]
    TxNotAvailable,

    /// The operation was cancelled via a
    /// [`CancelToken`](liters_storage::CancelToken).
    #[error("operation cancelled")]
    Cancelled,

    #[error("{0}")]
    Other(String),
}

/// Storage-level cancellation surfaces as [`Error::Cancelled`] so callers
/// match a single variant no matter which layer observed the token.
impl From<liters_storage::StorageError> for Error {
    fn from(e: liters_storage::StorageError) -> Error {
        match e {
            liters_storage::StorageError::Cancelled => Error::Cancelled,
            e => Error::Storage(e),
        }
    }
}

impl Error {
    /// Whether retrying the same operation later can plausibly succeed:
    /// transient storage failures (per
    /// [`StorageError::is_transient`](liters_storage::StorageError::is_transient))
    /// and local I/O hiccups. Divergence, integrity errors, and cancellation
    /// are never transient.
    pub fn is_transient(&self) -> bool {
        match self {
            Error::Storage(e) => e.is_transient(),
            Error::Io(_) => true,
            _ => false,
        }
    }
}
