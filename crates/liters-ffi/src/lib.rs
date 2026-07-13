//! UniFFI surface for liters: blocking, mobile-friendly wrappers around the
//! Writer, Replica, and multi-database Manager. Designed for iOS
//! BGTaskScheduler / Android WorkManager usage: every operation is short,
//! resumable, and crash-safe, so a killed task resumes on the next call
//! rather than restarting. Long-running work (the Manager's workers, or an
//! individual push/sync) is interruptible via `cancel()` / `sleep()`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

uniffi::setup_scaffolding!();

/// Locks a mutex, recovering from poison: the guarded values are plain data
/// (no invariant can be torn mid-update in a way that makes the recovered
/// value unsafe to read), and wedging every subsequent FFI call on an
/// earlier panic would be strictly worse on mobile.
fn plock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Milliseconds since the Unix epoch (the FFI's timestamp convention).
fn epoch_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn closed_error(what: &str) -> LitersError {
    LitersError::Other { message: format!("{what} is closed") }
}

/// Installs a fresh token as the object's current one and returns it. Called
/// at the start of every operation, so `cancel()` aborts exactly the
/// in-flight call and can never pre-cancel a future one.
fn install_token(cell: &Mutex<liters::CancelToken>) -> liters::CancelToken {
    let token = liters::CancelToken::new();
    *plock(cell) = token.clone();
    token
}

/// Where a database's replica lives.
#[derive(Debug, uniffi::Enum)]
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
    /// Another liters instance serving a bucket over HTTP
    /// (`http://host:port[/path]`; one database of a multi-DB server is
    /// addressed as `http://host:port/db/{name}`). Valid as a Replica
    /// source — `sync()` to poll, or follow live via
    /// `LitersManager.register_follow`. Valid as a Writer destination when
    /// the server runs with `writable: true` — that is push replication:
    /// this device dials out and pushes, the listening liters receives.
    /// Read-only servers reject the first push with a clear error.
    ///
    /// `auth_token` is sent as `Authorization: Bearer <token>` on every
    /// request, for servers started with an auth token. (Adding this field
    /// is a source-level break for Swift/Kotlin callers constructing
    /// `Storage.Http` positionally; pass `null`/`nil` to keep the old
    /// behavior.)
    Http { url: String, auth_token: Option<String> },
}

impl Storage {
    fn into_config(self) -> liters::StorageConfig {
        match self {
            Storage::Dir { path } => liters::StorageConfig::Dir { path: path.into() },
            Storage::S3 {
                bucket,
                prefix,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                force_path_style,
                allow_http,
            } => liters::StorageConfig::S3 {
                config: liters::S3Config {
                    bucket,
                    prefix,
                    endpoint,
                    region,
                    access_key_id,
                    secret_access_key,
                    force_path_style,
                    allow_http,
                },
            },
            Storage::Http { url, auth_token } => liters::StorageConfig::Http {
                url,
                // writer_id is left None on purpose: the Manager fills it
                // with a per-database persisted id for push registrations;
                // plain LitersWriter pushes stay headerless (no fencing).
                options: liters::HttpClientOptions { auth_token, ..Default::default() },
            },
        }
    }

    fn into_client(self) -> Result<Box<dyn liters::ReplicaClient>, LitersError> {
        self.into_config().build().map_err(map_error)
    }
}

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum LitersError {
    /// The bucket's history no longer matches the local state; call
    /// `Replica.reset()` (or construct with `auto_reset`) to re-restore.
    #[error("diverged: {message}")]
    Diverged { message: String },
    /// Storage/network failure (including local I/O); usually transient —
    /// retry the same call later.
    #[error("storage: {message}")]
    Storage { message: String },
    /// The server rejected a write for consistency/ownership reasons:
    /// another writer holds the bucket lease, or the push was
    /// non-monotonic. Not retryable as-is — the next push after the local
    /// lineage check re-derives the correct baseline.
    #[error("conflict: {message}")]
    Conflict { message: String },
    /// Authentication required or rejected; fix the auth token.
    #[error("unauthorized: {message}")]
    Unauthorized { message: String },
    /// The operation was aborted by `cancel()` (or `close()`). State is
    /// crash-safe: retrying is indistinguishable from resuming after a
    /// process kill.
    #[error("operation cancelled")]
    Cancelled,
    #[error("{message}")]
    Other { message: String },
}

fn map_error(e: liters::Error) -> LitersError {
    let message = e.to_string();
    match e {
        liters::Error::Cancelled => LitersError::Cancelled,
        liters::Error::Diverged { .. } => LitersError::Diverged { message },
        liters::Error::Storage(liters::StorageError::Conflict(_)) => {
            LitersError::Conflict { message }
        }
        liters::Error::Storage(liters::StorageError::Unauthorized(_)) => {
            LitersError::Unauthorized { message }
        }
        liters::Error::Storage(liters::StorageError::Cancelled) => LitersError::Cancelled,
        // Permanent conditions must not land in the retryable Storage
        // variant: ReadOnly (the server rejects every write until it is
        // reconfigured) and StorageError::Other (protocol mismatch, not a
        // liters server) map to Other — non-retryable, message names the
        // cause.
        liters::Error::Storage(liters::StorageError::ReadOnly(_))
        | liters::Error::Storage(liters::StorageError::Other(_)) => {
            LitersError::Other { message }
        }
        // Io is transient per the core's own classification
        // (liters::Error::is_transient), so it maps to Storage, not Other.
        liters::Error::Storage(_) | liters::Error::Io(_) => LitersError::Storage { message },
        _ => LitersError::Other { message },
    }
}

#[derive(Debug, uniffi::Record)]
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

fn push_summary(r: liters::PushResult) -> PushSummary {
    PushSummary {
        txid: r.txid.0,
        synced: r.synced,
        uploaded: r.uploaded,
        remote_txid: r.remote_txid.0,
        checkpointed: r.checkpointed,
    }
}

#[derive(Debug, uniffi::Record)]
pub struct SyncSummary {
    /// Whether a full restore ran (vs. incremental application).
    pub restored: bool,
    pub from_txid: u64,
    pub to_txid: u64,
}

fn sync_summary(s: liters::SyncResult) -> SyncSummary {
    SyncSummary { restored: s.restored, from_txid: s.from_txid.0, to_txid: s.to_txid.0 }
}

#[derive(Debug, uniffi::Record)]
pub struct MaintenanceSummary {
    pub compacted_levels: Vec<u8>,
    pub snapshot_txid: Option<u64>,
    pub deleted_files: u64,
}

/// Replicates a local, app-owned SQLite database to a bucket. Call `push()`
/// after commits (or batched); call `maintain()` opportunistically (wifi +
/// charging) to compact and enforce retention. For multiple databases with
/// scheduled pushes and sleep/resume, use `LitersManager` instead.
#[derive(uniffi::Object)]
pub struct LitersWriter {
    inner: Mutex<Option<liters::Writer>>,
    /// Current operation's token; `cancel()` flips it. Separate from
    /// `inner` so cancel never waits on an in-flight call.
    cancel: Mutex<liters::CancelToken>,
    /// Set (before the token is cancelled) by `close()`. Operations check it
    /// after installing their token, so a call that was queued on `inner`
    /// behind the one `close()` cancelled can never install a fresh token
    /// and run in full — it bails with the closed error instead, keeping
    /// `close()`'s wait bounded by the single in-flight operation.
    closed: AtomicBool,
}

impl LitersWriter {
    /// Runs `f` on the open writer with a freshly installed cancel token;
    /// errors clearly once `close()` has run.
    ///
    /// Ordering: the token is installed under `inner` (first thing after
    /// acquiring it, before any blocking work), so once an operation has
    /// begun, `cancel()`/`close()` always reach *its* token. The remaining
    /// window is benign: a `cancel()` that lands before the operation
    /// installs its token cancels the previous (finished) operation's token
    /// — but the new operation has not started any I/O yet, so there is
    /// nothing to abort; it simply runs (and `close()` still catches it via
    /// the `closed` check below).
    fn with_writer<R>(
        &self,
        f: impl FnOnce(&mut liters::Writer, &liters::CancelToken) -> liters::Result<R>,
    ) -> Result<R, LitersError> {
        let mut guard = plock(&self.inner);
        let token = install_token(&self.cancel);
        // Checked AFTER the install: close() sets the flag before cancelling
        // the token cell, so either close saw our token (and cancelled it),
        // or we see the flag — a call can never slip through with a live
        // token once close() has begun.
        if self.closed.load(Ordering::SeqCst) {
            return Err(closed_error("LitersWriter"));
        }
        let w = guard.as_mut().ok_or_else(|| closed_error("LitersWriter"))?;
        f(w, &token).map_err(map_error)
    }
}

#[uniffi::export]
impl LitersWriter {
    /// Opens a writer for an existing SQLite database. Switches the database
    /// to WAL mode and takes over checkpointing (do not run your own
    /// `wal_checkpoint`; `wal_autocheckpoint` on your connections is fine —
    /// it will simply never fire while the writer holds its read lock).
    ///
    /// Performs no network I/O: opening succeeds offline, and pushes
    /// accumulate local L0 files until the bucket becomes reachable (the
    /// first successful push then uploads the backlog).
    #[uniffi::constructor]
    pub fn new(db_path: String, storage: Storage) -> Result<Self, LitersError> {
        let client = storage.into_client()?;
        let writer = liters::Writer::open(db_path, client, liters::WriterOptions::default())
            .map_err(map_error)?;
        Ok(LitersWriter {
            inner: Mutex::new(Some(writer)),
            cancel: Mutex::new(liters::CancelToken::new()),
            closed: AtomicBool::new(false),
        })
    }

    /// Captures all committed changes into the bucket. Short and resumable:
    /// on failure, the next push picks up exactly where this one stopped.
    pub fn push(&self) -> Result<PushSummary, LitersError> {
        self.with_writer(|w, t| w.push_with(t)).map(push_summary)
    }

    /// Runs due compaction, snapshotting, and retention with litestream's
    /// default cadences.
    pub fn maintain(&self) -> Result<MaintenanceSummary, LitersError> {
        let r = self.with_writer(|w, t| w.maintain_with(t, &liters::MaintenanceOptions::default()))?;
        Ok(MaintenanceSummary {
            compacted_levels: r.compacted_levels,
            snapshot_txid: r.snapshot.map(|t| t.0),
            deleted_files: r.deleted as u64,
        })
    }

    /// Forces a full snapshot to the bucket now.
    pub fn snapshot(&self) -> Result<Option<u64>, LitersError> {
        Ok(self.with_writer(|w, t| w.snapshot_with(t))?.map(|t| t.0))
    }

    /// Current local replication position.
    pub fn position(&self) -> Result<u64, LitersError> {
        Ok(self.with_writer(|w, _| w.pos())?.txid.0)
    }

    /// Recovery for a "local ltx file missing or corrupt" error: wipes local
    /// replication state so the next push re-derives everything from the
    /// bucket (snapshotting if needed). Local database content is untouched.
    pub fn reset_local(&self) -> Result<(), LitersError> {
        self.with_writer(|w, _| w.reset_local())
    }

    /// Aborts the in-flight `push`/`maintain`/`snapshot`, if any: it returns
    /// `Cancelled` promptly (mid-transfer on the HTTP backend). Each call
    /// installs a fresh token at its start, so cancel only ever affects the
    /// operation currently running — never a future call. Cancel-then-retry
    /// is indistinguishable from kill-then-restart.
    pub fn cancel(&self) {
        plock(&self.cancel).cancel();
    }

    /// Tears the writer down: cancels any in-flight operation, waits for it
    /// to return, then releases the WAL read lock, SQLite connections, and
    /// file descriptors. Subsequent calls error with "LitersWriter is
    /// closed" — including calls already queued behind the cancelled one,
    /// which bail instead of running (so this never blocks for more than
    /// the single in-flight operation's cancellation latency). Idempotent.
    pub fn close(&self) {
        // Flag first, then cancel: an operation that misses the cancel (its
        // token was installed after ours was read) is guaranteed to see the
        // flag — see with_writer.
        self.closed.store(true, Ordering::SeqCst);
        plock(&self.cancel).cancel();
        let taken = plock(&self.inner).take();
        drop(taken); // Writer teardown (SQLite calls) runs outside the lock
    }
}

/// A local read-only materialization of a bucket. `sync()` restores on first
/// use, then applies changes incrementally. Open `db_path()` read-only with
/// your platform SQLite; do not hold read transactions across `sync()` calls
/// from the same process. For continuous (live) following, register the
/// database with `LitersManager.register_follow` instead of polling.
#[derive(uniffi::Object)]
pub struct LitersReplica {
    inner: Mutex<Option<liters::Replica>>,
    cancel: Mutex<liters::CancelToken>,
    /// See [`LitersWriter::closed`]: same close()/cancel() discipline.
    closed: AtomicBool,
    db_path: String,
}

impl LitersReplica {
    /// See [`LitersWriter::with_writer`] for the token-install/closed-check
    /// ordering (identical here).
    fn with_replica<R>(
        &self,
        f: impl FnOnce(&mut liters::Replica, &liters::CancelToken) -> liters::Result<R>,
    ) -> Result<R, LitersError> {
        let mut guard = plock(&self.inner);
        let token = install_token(&self.cancel);
        if self.closed.load(Ordering::SeqCst) {
            return Err(closed_error("LitersReplica"));
        }
        let r = guard.as_mut().ok_or_else(|| closed_error("LitersReplica"))?;
        f(r, &token).map_err(map_error)
    }
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
        Ok(LitersReplica {
            inner: Mutex::new(Some(replica)),
            cancel: Mutex::new(liters::CancelToken::new()),
            closed: AtomicBool::new(false),
            db_path,
        })
    }

    /// Brings the local replica up to date. Short and resumable; safe to
    /// call from a background task with a tight deadline.
    pub fn sync(&self) -> Result<SyncSummary, LitersError> {
        self.with_replica(|r, t| r.sync_with(t)).map(sync_summary)
    }

    /// Last applied TXID (zero if never synced).
    pub fn position(&self) -> Result<u64, LitersError> {
        Ok(self.with_replica(|r, _| r.position())?.0)
    }

    /// Path of the local database file to open read-only.
    pub fn db_path(&self) -> String {
        self.db_path.clone()
    }

    /// Deletes the local replica; the next `sync()` restores from scratch.
    pub fn reset(&self) -> Result<(), LitersError> {
        self.with_replica(|r, _| r.reset())
    }

    /// Aborts the in-flight `sync`, if any: it returns `Cancelled` promptly.
    /// Each call installs a fresh token at its start, so cancel only ever
    /// affects the operation currently running. A cancelled sync resumes
    /// exactly where it stopped on the next call.
    pub fn cancel(&self) {
        plock(&self.cancel).cancel();
    }

    /// Tears the replica down: cancels any in-flight sync, waits for it to
    /// return, then drops the storage client. Subsequent calls (except
    /// `db_path()`) error with "LitersReplica is closed" — including calls
    /// already queued behind the cancelled one. Idempotent.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        plock(&self.cancel).cancel();
        let taken = plock(&self.inner).take();
        drop(taken);
    }
}

// ---------------------------------------------------------------------------
// Manager

/// Retry schedule for transient failures: exponential growth from
/// `initial_ms` by `multiplier` per consecutive failure, saturating at
/// `max_ms`, with ±`jitter` randomization. The defaults are the liters
/// defaults (500ms → 60s, ×2, ±25%).
#[derive(Debug, uniffi::Record)]
pub struct Backoff {
    /// Delay before the first retry, in milliseconds.
    #[uniffi(default = 500)]
    pub initial_ms: u64,
    /// Ceiling (pre-jitter) the schedule saturates at, in milliseconds.
    #[uniffi(default = 60000)]
    pub max_ms: u64,
    /// Growth factor per consecutive failure.
    #[uniffi(default = 2.0)]
    pub multiplier: f64,
    /// Jitter fraction in [0, 1]; 0 disables jitter.
    #[uniffi(default = 0.25)]
    pub jitter: f64,
}

impl From<Backoff> for liters::Backoff {
    fn from(b: Backoff) -> liters::Backoff {
        liters::Backoff {
            initial: Duration::from_millis(b.initial_ms),
            max: Duration::from_millis(b.max_ms),
            multiplier: b.multiplier,
            jitter: b.jitter,
        }
    }
}

/// Manager-wide defaults (see `LitersManager.with_options`).
#[derive(Debug, uniffi::Record)]
pub struct ManagerOptions {
    /// Backoff for transient failures; overridable per push registration
    /// and used as the follow-loop retry schedule. `null` = liters
    /// defaults.
    #[uniffi(default = None)]
    pub backoff: Option<Backoff>,
    /// Fallback push cadence (milliseconds) for registrations whose own
    /// `push_interval_ms` is `null`. When both are `null`, those databases
    /// push only on `push_now()`.
    #[uniffi(default = None)]
    pub default_push_interval_ms: Option<u64>,
}

/// Per-database options for `LitersManager.register_push`.
#[derive(Debug, uniffi::Record)]
pub struct PushOptions {
    /// Push cadence in milliseconds. `null` falls back to the manager's
    /// `default_push_interval_ms`; if that is also `null`, this database
    /// pushes only on `push_now()`. With an interval set, the first push
    /// runs immediately after registration.
    #[uniffi(default = None)]
    pub push_interval_ms: Option<u64>,
    /// When set, maintenance (compaction/snapshot/retention with
    /// litestream's default cadences) is attempted after a successful push
    /// once this many milliseconds have passed since the last attempt. A
    /// maintenance failure is reported but never blocks future pushes.
    #[uniffi(default = None)]
    pub maintenance_interval_ms: Option<u64>,
    /// Per-database transient-failure backoff; `null` uses the manager's.
    #[uniffi(default = None)]
    pub backoff: Option<Backoff>,
}

/// Per-database options for `LitersManager.register_follow`.
#[derive(Debug, uniffi::Record)]
pub struct FollowOptions {
    /// On bucket divergence, silently delete the local replica and
    /// re-restore instead of parking in the `Failed` state.
    #[uniffi(default = false)]
    pub auto_reset: bool,
    /// Poll cadence in milliseconds for waiting out an empty bucket and for
    /// backends without live streaming. `null` = 1000.
    #[uniffi(default = None)]
    pub poll_interval_ms: Option<u64>,
    /// Backoff for transient reconnects inside the follow loop; `null` uses
    /// the manager's backoff.
    #[uniffi(default = None)]
    pub retry: Option<Backoff>,
}

/// Whether a registration pushes or follows.
#[derive(Debug, uniffi::Enum)]
pub enum DbRole {
    Push,
    Follow,
}

/// Lifecycle state of one registered database.
#[derive(Debug, uniffi::Enum)]
pub enum DbState {
    /// Registered and waiting for the next tick/nudge (push entries only;
    /// an active follower is `Working` for its whole session).
    Idle,
    /// A push/maintain round or follow session is in progress.
    Working,
    /// A transient failure is waiting out its backoff delay. `until_ms` is
    /// milliseconds since the Unix epoch; `attempt` is the
    /// consecutive-failure count (1 after the first failure).
    BackingOff { until_ms: u64, attempt: u32 },
    /// Slept via `sleep()`: session cancelled, Writer dropped, no storage
    /// traffic until `resume()`.
    Sleeping,
    /// A fatal (non-transient) error parked the worker; it retries once per
    /// nudge (`push_now`/`sync_now`) or on `resume()`.
    Failed,
}

fn map_state(s: liters::DbState) -> DbState {
    match s {
        liters::DbState::Idle => DbState::Idle,
        liters::DbState::Working => DbState::Working,
        liters::DbState::BackingOff { until, attempt } => {
            DbState::BackingOff { until_ms: epoch_ms(until), attempt }
        }
        liters::DbState::Sleeping => DbState::Sleeping,
        liters::DbState::Failed => DbState::Failed,
    }
}

/// Point-in-time snapshot of one registered database.
#[derive(Debug, uniffi::Record)]
pub struct DbStatus {
    pub id: String,
    pub role: DbRole,
    pub state: DbState,
    /// Replication position (TXID): for pushes, the last successful push;
    /// for follows, the last applied transaction. `null` until the first
    /// push/apply.
    pub position: Option<u64>,
    /// Most recent error message; cleared by the next successful round.
    pub last_error: Option<String>,
    /// Completion time of the last successful push/apply, in milliseconds
    /// since the Unix epoch.
    pub last_activity_ms: Option<u64>,
}

fn map_status(s: liters::DbStatus) -> DbStatus {
    DbStatus {
        id: s.id,
        role: match s.role {
            liters::DbRole::Push => DbRole::Push,
            liters::DbRole::Follow => DbRole::Follow,
        },
        state: map_state(s.state),
        position: s.position.map(|t| t.0),
        last_error: s.last_error,
        last_activity_ms: s.last_activity.map(epoch_ms),
    }
}

/// Events delivered to a `ManagerListener`. One database's events arrive in
/// order; followers do not emit per-transaction position events (poll
/// `status()` for live positions) — `SyncCompleted` is reserved for future
/// use.
#[derive(Debug, uniffi::Enum)]
pub enum ManagerEvent {
    StateChanged { id: String, state: DbState },
    PushCompleted { id: String, result: PushSummary },
    SyncCompleted { id: String, result: SyncSummary },
    Error { id: String, message: String, transient: bool },
}

fn map_event(e: liters::ManagerEvent) -> ManagerEvent {
    match e {
        liters::ManagerEvent::StateChanged { id, state } => {
            ManagerEvent::StateChanged { id, state: map_state(state) }
        }
        liters::ManagerEvent::PushCompleted { id, result } => {
            ManagerEvent::PushCompleted { id, result: push_summary(result) }
        }
        liters::ManagerEvent::SyncCompleted { id, result } => {
            ManagerEvent::SyncCompleted { id, result: sync_summary(result) }
        }
        liters::ManagerEvent::Error { id, message, transient } => {
            ManagerEvent::Error { id, message, transient }
        }
    }
}

/// Receives manager events. Callbacks are delivered on liters worker
/// threads (and, for the `Sleeping` state change, on the thread that called
/// `sleep()`): they must return quickly, must never block, and must not
/// call back into `unregister()`/`shutdown()` — hand the event to your own
/// queue/handler instead.
#[uniffi::export(with_foreign)]
pub trait ManagerListener: Send + Sync {
    fn on_event(&self, event: ManagerEvent);
}

/// Bridges the FFI listener onto the core observer trait.
struct ListenerAdapter(Arc<dyn ManagerListener>);

impl liters::ManagerObserver for ListenerAdapter {
    fn on_event(&self, event: liters::ManagerEvent) {
        // Contain foreign exceptions. `ManagerListener::on_event` returns
        // `()` (no Result), and for such callback methods uniffi 0.32's
        // generated glue reports a thrown Kotlin/Swift exception as an
        // unexpected callback error whose default handling is an
        // unconditional Rust panic (`LiftReturn::handle_callback_unexpected_
        // error`, uniffi_core-0.32.0/src/ffi_converter_traits.rs:393-395 —
        // verified). Without this guard that panic would unwind into the
        // Manager worker thread emitting the event and silently kill that
        // database's replication (or abort the process under panic=abort).
        // A listener exception therefore drops the event; replication is
        // never affected.
        let event = map_event(event);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.0.on_event(event);
        }));
    }
}

/// Runs replication for any number of registered databases on background
/// threads: each database either pushes to a bucket or follows one, with
/// automatic retry/backoff, and per-database or global sleep/resume for
/// mobile power management. `sleep()` cancels in-flight transfers and
/// releases the database's WAL lock and file descriptors; `resume()`
/// schedules an immediate catch-up round.
///
/// All methods return immediately except `unregister()` and `shutdown()`,
/// which join worker threads (bounded by the storage backend's cancellation
/// latency — a few seconds worst case over HTTP). Call `shutdown()` on app
/// teardown for deterministic cleanup; it also runs when the object is
/// destroyed.
#[derive(uniffi::Object)]
pub struct LitersManager {
    inner: liters::Manager,
}

impl Default for LitersManager {
    fn default() -> Self {
        Self::new()
    }
}

#[uniffi::export]
impl LitersManager {
    /// A manager with default options (liters backoff, no default push
    /// interval — databases push on `push_now()` unless registered with
    /// their own interval).
    #[uniffi::constructor]
    pub fn new() -> Self {
        Self::with_options(ManagerOptions { backoff: None, default_push_interval_ms: None })
    }

    #[uniffi::constructor]
    pub fn with_options(options: ManagerOptions) -> Self {
        LitersManager {
            inner: liters::Manager::new(liters::ManagerOptions {
                backoff: options.backoff.map(Into::into).unwrap_or_default(),
                default_push_interval: options.default_push_interval_ms.map(Duration::from_millis),
            }),
        }
    }

    /// Registers a local database to push to `storage` and starts its
    /// worker. The database file must exist. Errors on a duplicate id or an
    /// already-registered path. For HTTP storage the manager persists a
    /// per-database writer id next to the database and sends it for
    /// server-side fencing.
    pub fn register_push(
        &self,
        id: String,
        db_path: String,
        storage: Storage,
        options: PushOptions,
    ) -> Result<(), LitersError> {
        let cfg = liters::PushConfig {
            storage: storage.into_config(),
            writer_options: liters::WriterOptions::default(),
            push_interval: options.push_interval_ms.map(Duration::from_millis),
            maintenance: options
                .maintenance_interval_ms
                .map(|ms| (liters::MaintenanceOptions::default(), Duration::from_millis(ms))),
            backoff: options.backoff.map(Into::into),
        };
        self.inner.register_push(id, db_path, cfg).map_err(map_error)
    }

    /// Registers a local path to follow (materialize) a bucket and starts
    /// its worker. `db_path` need not exist yet — the first sync restores
    /// it. Errors on a duplicate id or an already-registered path.
    pub fn register_follow(
        &self,
        id: String,
        db_path: String,
        storage: Storage,
        options: FollowOptions,
    ) -> Result<(), LitersError> {
        let mut follow_options = liters::FollowOptions::default();
        if let Some(ms) = options.poll_interval_ms {
            follow_options.poll_interval = Duration::from_millis(ms);
        }
        follow_options.retry = options.retry.map(Into::into);
        let cfg = liters::FollowConfig {
            storage: storage.into_config(),
            replica_options: liters::ReplicaOptions {
                auto_reset: options.auto_reset,
                ..Default::default()
            },
            follow_options,
        };
        self.inner.register_follow(id, db_path, cfg).map_err(map_error)
    }

    /// Removes a registration: cancels its session, joins its worker, and
    /// releases its database. Blocks briefly; the id becomes free for
    /// re-registration. Unknown id errors.
    pub fn unregister(&self, id: String) -> Result<(), LitersError> {
        self.inner.unregister(&id).map_err(map_error)
    }

    /// Puts one database to sleep: aborts its in-flight transfer, releases
    /// its WAL lock and file descriptors, and stops all storage traffic
    /// until `resume()`. Returns immediately; teardown completes on the
    /// worker. Idempotent.
    pub fn sleep(&self, id: String) -> Result<(), LitersError> {
        self.inner.sleep(&id).map_err(map_error)
    }

    /// Wakes a sleeping database and schedules an immediate catch-up round.
    /// On a running-but-`Failed` database this acts as a retry nudge.
    /// Idempotent.
    pub fn resume(&self, id: String) -> Result<(), LitersError> {
        self.inner.resume(&id).map_err(map_error)
    }

    /// `sleep()` for every registered database (app went to background).
    pub fn sleep_all(&self) {
        self.inner.sleep_all();
    }

    /// `resume()` for every registered database (app returned to
    /// foreground).
    pub fn resume_all(&self) {
        self.inner.resume_all();
    }

    /// Nudges a push registration to run a round now (also the retry path
    /// out of `Failed`). Ignored while sleeping — resume first. Errors on
    /// an unknown id or a follow registration.
    pub fn push_now(&self, id: String) -> Result<(), LitersError> {
        self.inner.push_now(&id).map_err(map_error)
    }

    /// Forces a follow registration to resync immediately by restarting its
    /// follow session. Also the retry path out of `Failed`. Ignored while
    /// sleeping. Errors on an unknown id or a push registration.
    pub fn sync_now(&self, id: String) -> Result<(), LitersError> {
        self.inner.sync_now(&id).map_err(map_error)
    }

    /// Status of one registered database, or `null` for an unknown id.
    pub fn status(&self, id: String) -> Option<DbStatus> {
        self.inner.status(&id).map(map_status)
    }

    /// Statuses of all registered databases, sorted by id.
    pub fn statuses(&self) -> Vec<DbStatus> {
        self.inner.statuses().into_iter().map(map_status).collect()
    }

    /// Installs (or clears, with `null`) the event listener. See
    /// `ManagerListener` for the threading rules.
    pub fn set_listener(&self, listener: Option<Arc<dyn ManagerListener>>) {
        self.inner.set_observer(
            listener.map(|l| Arc::new(ListenerAdapter(l)) as Arc<dyn liters::ManagerObserver>),
        );
    }

    /// Cancels every session and joins every worker thread. Idempotent;
    /// also runs when the object is destroyed.
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_mapping() {
        assert!(matches!(map_error(liters::Error::Cancelled), LitersError::Cancelled));
        assert!(matches!(
            map_error(liters::Error::Storage(liters::StorageError::Cancelled)),
            LitersError::Cancelled
        ));
        assert!(matches!(
            map_error(liters::Error::Diverged {
                local: liters::Txid(2),
                remote: liters::Txid(1)
            }),
            LitersError::Diverged { .. }
        ));
        assert!(matches!(
            map_error(liters::Error::Storage(liters::StorageError::Conflict("owned".into()))),
            LitersError::Conflict { .. }
        ));
        assert!(matches!(
            map_error(liters::Error::Storage(liters::StorageError::Unauthorized("401".into()))),
            LitersError::Unauthorized { .. }
        ));
        // Io is transient per the core's classification → Storage, not Other.
        assert!(matches!(
            map_error(liters::Error::Io(std::io::Error::other("disk"))),
            LitersError::Storage { .. }
        ));
        assert!(matches!(
            map_error(liters::Error::Storage(liters::StorageError::Unavailable("down".into()))),
            LitersError::Storage { .. }
        ));
        // Permanent conditions must not look retryable: ReadOnly and the
        // protocol-level Other map to LitersError::Other, never Storage.
        match map_error(liters::Error::Storage(liters::StorageError::ReadOnly(
            "server is read-only (writable: false)".into(),
        ))) {
            LitersError::Other { message } => {
                assert!(message.contains("read-only"), "message must name the cause: {message}")
            }
            other => panic!("ReadOnly must map to Other, got {other:?}"),
        }
        assert!(matches!(
            map_error(liters::Error::Storage(liters::StorageError::Other(
                "not a liters server".into()
            ))),
            LitersError::Other { .. }
        ));
        assert!(matches!(
            map_error(liters::Error::TxNotAvailable),
            LitersError::Other { .. }
        ));
    }

    #[test]
    fn state_and_status_flattening() {
        let until = UNIX_EPOCH + Duration::from_millis(12_345);
        match map_state(liters::DbState::BackingOff { until, attempt: 3 }) {
            DbState::BackingOff { until_ms, attempt } => {
                assert_eq!(until_ms, 12_345);
                assert_eq!(attempt, 3);
            }
            _ => panic!("wrong variant"),
        }
        let status = map_status(liters::DbStatus {
            id: "app".into(),
            role: liters::DbRole::Follow,
            state: liters::DbState::Working,
            position: Some(liters::Txid(7)),
            last_error: None,
            last_activity: Some(UNIX_EPOCH + Duration::from_millis(99)),
        });
        assert!(matches!(status.role, DbRole::Follow));
        assert!(matches!(status.state, DbState::Working));
        assert_eq!(status.position, Some(7));
        assert_eq!(status.last_activity_ms, Some(99));
    }

    #[test]
    fn backoff_conversion() {
        let b: liters::Backoff =
            Backoff { initial_ms: 10, max_ms: 200, multiplier: 3.0, jitter: 0.0 }.into();
        assert_eq!(b.initial, Duration::from_millis(10));
        assert_eq!(b.max, Duration::from_millis(200));
        assert_eq!(b.delay(0), Duration::from_millis(10));
        assert_eq!(b.delay(1), Duration::from_millis(30));
        assert_eq!(b.delay(10), Duration::from_millis(200));
    }
}
