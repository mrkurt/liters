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
    Storage(#[from] liters_storage::StorageError),

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

    #[error("{0}")]
    Other(String),
}
