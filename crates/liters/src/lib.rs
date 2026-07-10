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
mod meta;
mod plan;
mod replica;
mod sqlite;
mod verify;
mod writer;

pub use compaction::{MaintenanceOptions, MaintenanceReport};
pub use error::Error;
pub use replica::{IntegrityCheck, Replica, ReplicaOptions, SyncResult};
pub use writer::{CheckpointMode, PushResult, Writer, WriterOptions};

pub type Result<T> = std::result::Result<T, Error>;

// Re-export the building blocks so app code needs only this crate.
pub use liters_storage::{DirReplicaClient, ReplicaClient, StorageError, SNAPSHOT_LEVEL};
pub use ltx::{Pos, Txid};
