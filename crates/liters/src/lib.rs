//! Litestream-compatible SQLite replication for embedded use.
//!
//! The write side ([`Writer`]) replicates a local SQLite database to a
//! replica bucket as LTX files, driven by explicit [`Writer::push`] calls
//! (the app owns the database and decides when to sync — no daemon, no file
//! watching). The bucket it produces is restorable by stock Go
//! `litestream restore`.
//!
//! The read side (`Replica`) maintains a local read-only materialization of
//! a bucket, applying new LTX files incrementally on sync.

mod checkpoint;
mod compaction;
mod error;
mod manager;
mod meta;
mod plan;
mod replica;
mod retry;
mod sqlite;
mod verify;
mod writer;

pub use compaction::{MaintenanceOptions, MaintenanceReport};
pub use error::Error;
pub use manager::{
    ClientFactory, DbRole, DbState, DbStatus, FollowConfig, Manager, ManagerEvent,
    ManagerObserver, ManagerOptions, PushConfig, StorageConfig,
};
pub use replica::{read_txid_file, FollowOptions, IntegrityCheck, Replica, ReplicaOptions, SyncResult};
pub use retry::Backoff;
pub use writer::{CheckpointMode, PushResult, Writer, WriterOptions};

pub type Result<T> = std::result::Result<T, Error>;

// Re-export the building blocks so app code needs only this crate.
pub use liters_storage::{
    CancelToken, DirReplicaClient, LtxStream, ReplicaClient, StorageError, StreamEvent,
    SNAPSHOT_LEVEL,
};
#[cfg(feature = "http")]
pub use liters_storage::HttpClientOptions;
#[cfg(feature = "s3")]
pub use liters_storage::S3Config;
pub use ltx::{Pos, Txid};
