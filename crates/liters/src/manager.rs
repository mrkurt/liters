//! Multi-database replication manager: one background worker thread per
//! registered database, each either *pushing* a local SQLite database to a
//! bucket ([`Writer`]) or *following* a bucket into a local read replica
//! ([`Replica`]), with per-database and global sleep/resume.
//!
//! Threading model: every registered database gets one std thread named
//! `liters-mgr-{id}`. All coordination goes through a per-entry mutex +
//! condvar and [`CancelToken`]s — no global lock is ever held across a
//! blocking call, so control operations (`sleep`, `resume`, `status`,
//! `push_now`, …) return immediately regardless of what workers are doing.
//!
//! Session discipline: a worker's storage client (and, for pushes, its
//! [`Writer`]) belongs to one *session*. Sleeping cancels the session token
//! (interrupting in-flight transfers on token-aware backends) and the worker
//! drops its Writer as soon as it regains control, releasing the WAL-pinning
//! read transaction and all file descriptors. Resuming installs a fresh
//! token and the next round rebuilds the client via [`StorageConfig::build`]
//! — a cancelled token can never poison a later session.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use liters_storage::{CancelToken, ReplicaClient};
use ltx::Txid;

#[cfg(feature = "http")]
use crate::meta::MetaDir;
use crate::replica::{read_txid_file, txid_path};
use crate::retry::Backoff;
use crate::{
    Error, FollowOptions, MaintenanceOptions, PushResult, Replica, ReplicaOptions, Result,
    SyncResult, Writer, WriterOptions,
};

/// A factory producing a fresh [`ReplicaClient`] per call (see
/// [`StorageConfig::Custom`]).
pub type ClientFactory = Arc<dyn Fn() -> Result<Box<dyn ReplicaClient>> + Send + Sync>;

/// Where a registered database replicates to (push) or from (follow).
/// [`StorageConfig::build`] is called on every session (re)construction, so
/// every variant must describe how to build a client, not hold one.
#[derive(Clone)]
pub enum StorageConfig {
    /// Local/dir bucket (litestream `file://` layout).
    Dir { path: PathBuf },
    /// A liters HTTP server. For push registrations the manager fills
    /// `options.writer_id` (when `None`) with a per-database persisted id
    /// from the meta dir, so server-side fencing identifies this device
    /// across restarts.
    ///
    /// `transport` is an optional shared [`HttpTransport`](liters_storage::HttpTransport).
    /// When `None` (the default) each session builds a fresh built-in
    /// socket transport (one `Connection: close` request per call). When
    /// `Some`, every session of every HTTP-backed database is handed a clone
    /// of the **same** transport, so an embedder (the mobile FFI layer) can
    /// pass in one platform-backed transport and have all followers to a host
    /// coalesce onto a single connection. The `Arc` is cloned per session; the
    /// transport itself owns connection reuse across the sleep/resume that
    /// tears down and rebuilds the clients.
    #[cfg(feature = "http")]
    Http {
        url: String,
        options: liters_storage::HttpClientOptions,
        transport: Option<std::sync::Arc<dyn liters_storage::HttpTransport>>,
    },
    /// An S3-compatible bucket.
    #[cfg(feature = "s3")]
    S3 { config: liters_storage::S3Config },
    /// App-provided factory; must build a FRESH client per call (clients are
    /// never reused across sleep/resume).
    Custom(ClientFactory),
}

impl StorageConfig {
    /// Builds a fresh client. The manager calls this once per session; a
    /// failure is classified like any other worker error (transient →
    /// backoff, fatal → `Failed`).
    pub fn build(&self) -> Result<Box<dyn ReplicaClient>> {
        match self {
            StorageConfig::Dir { path } => {
                Ok(Box::new(liters_storage::DirReplicaClient::new(path)))
            }
            #[cfg(feature = "http")]
            StorageConfig::Http { url, options, transport } => {
                let client = match transport {
                    Some(transport) => liters_storage::HttpReplicaClient::with_transport(
                        url,
                        options.clone(),
                        std::sync::Arc::clone(transport),
                    )?,
                    None => {
                        liters_storage::HttpReplicaClient::with_options(url, options.clone())?
                    }
                };
                Ok(Box::new(client))
            }
            #[cfg(feature = "s3")]
            StorageConfig::S3 { config } => {
                Ok(Box::new(liters_storage::S3ReplicaClient::new(config.clone())?))
            }
            StorageConfig::Custom(factory) => factory(),
        }
    }
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageConfig::Dir { path } => f.debug_struct("Dir").field("path", path).finish(),
            #[cfg(feature = "http")]
            StorageConfig::Http { url, .. } => {
                f.debug_struct("Http").field("url", url).finish_non_exhaustive()
            }
            #[cfg(feature = "s3")]
            StorageConfig::S3 { .. } => f.debug_struct("S3").finish_non_exhaustive(),
            StorageConfig::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

/// Manager-wide defaults.
#[derive(Clone, Debug, Default)]
pub struct ManagerOptions {
    /// Backoff schedule for transient failures; per-database overridable via
    /// [`PushConfig::backoff`], and installed as [`FollowOptions::retry`] on
    /// followers that left it `None`.
    pub backoff: Backoff,
    /// Fallback push cadence for [`PushConfig`]s whose `push_interval` is
    /// `None`. When both are `None`, that database pushes only on
    /// [`Manager::push_now`].
    pub default_push_interval: Option<Duration>,
}

/// Configuration for a push registration ([`Manager::register_push`]).
#[derive(Clone, Debug)]
pub struct PushConfig {
    pub storage: StorageConfig,
    pub writer_options: WriterOptions,
    /// Push cadence. `None` falls back to
    /// [`ManagerOptions::default_push_interval`]; if that is also `None`,
    /// pushes run only on [`Manager::push_now`]. When an interval is set,
    /// the first push runs immediately after registration.
    pub push_interval: Option<Duration>,
    /// Maintenance (compaction/snapshot/retention) options plus how often to
    /// *attempt* a run. Attempted after a successful push once the interval
    /// has elapsed since the previous attempt; a maintenance failure is
    /// reported but never blocks future pushes.
    pub maintenance: Option<(MaintenanceOptions, Duration)>,
    /// Transient-failure backoff; `None` uses the manager default.
    pub backoff: Option<Backoff>,
}

/// Configuration for a follow registration ([`Manager::register_follow`]).
#[derive(Clone, Debug)]
pub struct FollowConfig {
    pub storage: StorageConfig,
    pub replica_options: ReplicaOptions,
    /// Options for the underlying [`Replica::follow`] loop. A `retry` of
    /// `None` is overridden with the manager backoff so transient
    /// reconnection is handled *inside* follow; only fatal errors reach the
    /// worker (and produce the `Failed` state).
    pub follow_options: FollowOptions,
}

/// Whether a registration pushes or follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbRole {
    Push,
    Follow,
}

/// Lifecycle state of one registered database, as reported by
/// [`Manager::status`] and [`ManagerEvent::StateChanged`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbState {
    /// Registered and waiting for the next tick/nudge (push entries only;
    /// an active follower is `Working` for its whole session).
    Idle,
    /// A push/maintain round or follow session is in progress.
    Working,
    /// A transient failure is waiting out its backoff delay. `attempt` is
    /// the consecutive-failure count (1 after the first failure).
    BackingOff { until: SystemTime, attempt: u32 },
    /// Slept via [`Manager::sleep`]: session cancelled, Writer dropped, no
    /// storage traffic until [`Manager::resume`].
    Sleeping,
    /// A fatal (non-transient) error parked the worker. It retries once per
    /// nudge ([`Manager::push_now`] / [`Manager::sync_now`]) or on
    /// [`Manager::resume`].
    Failed,
}

/// Point-in-time snapshot of one registered database.
#[derive(Clone, Debug)]
pub struct DbStatus {
    pub id: String,
    pub role: DbRole,
    pub state: DbState,
    /// Replication position: for pushes, the TXID of the last successful
    /// push this manager ran; for follows, the live `-txid` sidecar value
    /// (read on demand — the Replica itself is worker-owned). `None` until
    /// the first push/apply.
    pub position: Option<Txid>,
    /// Most recent error message; cleared by the next successful round.
    pub last_error: Option<String>,
    /// Pushes: completion time of the last successful push. Follows: mtime
    /// of the `-txid` sidecar, i.e. the last applied transaction.
    pub last_activity: Option<SystemTime>,
}

/// Receives manager events. Callbacks run on worker threads or on the
/// control thread whose call produced an event (see [`ManagerEvent`] for
/// the per-database ordering guarantee): they MUST NOT block, and must not
/// call back into methods that join workers ([`Manager::unregister`],
/// [`Manager::shutdown`]) — a worker joining itself would deadlock.
pub trait ManagerObserver: Send + Sync {
    fn on_event(&self, event: ManagerEvent);
}

/// Events delivered to the [`ManagerObserver`].
///
/// Ordering guarantee: events for one database are delivered in the exact
/// order their state mutations happened. Every event is queued under the
/// same per-entry lock that applies its mutation and delivered FIFO by a
/// single drainer at a time, so a worker's `Working` can never arrive after
/// a control thread's `Sleeping` that superseded it — once the queue
/// drains, the last `StateChanged` an observer saw matches
/// [`Manager::status`]. One database's callbacks never run concurrently,
/// but may be invoked from either a worker thread or the control thread
/// whose call produced the event (whichever reaches the queue first
/// delivers). [`Manager::resume`] emits nothing itself (the resumed
/// round's `Working` transition is the signal).
///
/// Followers do not emit per-transaction position events in v1 (the follow
/// loop runs uninterrupted inside the worker); poll [`Manager::status`] for
/// live follower positions. `SyncCompleted` is reserved for that future use.
#[derive(Clone, Debug)]
pub enum ManagerEvent {
    StateChanged { id: String, state: DbState },
    PushCompleted { id: String, result: PushResult },
    SyncCompleted { id: String, result: SyncResult },
    Error { id: String, message: String, transient: bool },
}

// ---------------------------------------------------------------------------
// Internals

/// Locks a mutex, recovering from poison: entry state is plain data (no
/// invariants can be torn by a panicking thread mid-update that would make
/// the recovered value unsafe to read), and wedging every control call on a
/// worker bug would be strictly worse on mobile.
fn plock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Control-plane directive for a worker; separate from the observable
/// `DbState` because e.g. a *sleeping* entry can simultaneously be `Failed`
/// in spirit — the directive says what the worker must do, the state says
/// what the app sees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ctrl {
    Running,
    Sleeping,
    Shutdown,
}

struct EntryShared {
    ctrl: Ctrl,
    /// One-shot "run a round now" flag (push_now/sync_now/resume).
    nudged: bool,
    /// Current session token. Cancelled by sleep/shutdown (and swapped by
    /// sync_now); workers clone it at the start of every round so control
    /// methods can interrupt exactly the in-flight session.
    session: CancelToken,
    state: DbState,
    /// Pushes only: TXID of the last successful push (follows read their
    /// sidecar on demand instead).
    position: Option<Txid>,
    last_error: Option<String>,
    last_activity: Option<SystemTime>,
    /// Observer events queued in state-mutation order (enqueued under this
    /// lock, delivered FIFO by [`drain_events`]); together with `emitting`
    /// this is what makes emission order match mutation order without ever
    /// invoking a callback under the lock.
    pending_events: VecDeque<ManagerEvent>,
    /// True while some thread is delivering this entry's queued events; at
    /// most one drainer runs at a time, so one database's callbacks never
    /// overlap or reorder.
    emitting: bool,
}

struct Entry {
    id: String,
    db_path: PathBuf,
    /// Canonicalized (best-effort) db path for duplicate detection.
    canonical: PathBuf,
    role: DbRole,
    shared: Mutex<EntryShared>,
    cond: Condvar,
}

impl Entry {
    fn new(id: String, db_path: PathBuf, role: DbRole) -> Entry {
        // Canonicalize so two spellings of one file can't both register; a
        // not-yet-materialized follow target can't canonicalize, so fall
        // back to the raw path (still catches exact-duplicate spellings).
        let canonical = std::fs::canonicalize(&db_path).unwrap_or_else(|_| db_path.clone());
        Entry {
            id,
            db_path,
            canonical,
            role,
            shared: Mutex::new(EntryShared {
                ctrl: Ctrl::Running,
                nudged: false,
                session: CancelToken::new(),
                state: DbState::Idle,
                position: None,
                last_error: None,
                last_activity: None,
                pending_events: VecDeque::new(),
                emitting: false,
            }),
            cond: Condvar::new(),
        }
    }

    /// Worker-side state transition. Applies only while the entry is
    /// `Running`: control-plane states (`Sleeping`) always win, so a worker
    /// observing its cancellation late can never repaint a slept entry as
    /// `Working`. The event is queued under the same lock that applies the
    /// mutation; the caller must follow up with [`drain_events`].
    fn transition_running(&self, new: DbState) {
        let mut g = plock(&self.shared);
        if g.ctrl != Ctrl::Running || g.state == new {
            return;
        }
        g.state = new.clone();
        g.pending_events
            .push_back(ManagerEvent::StateChanged { id: self.id.clone(), state: new });
    }

    /// Records `message` as the entry's last error and queues the matching
    /// `Error` event (same lock, so status and event stream agree on order).
    /// The caller must follow up with [`drain_events`].
    fn record_error(&self, message: String, transient: bool) {
        let mut g = plock(&self.shared);
        g.last_error = Some(message.clone());
        g.pending_events.push_back(ManagerEvent::Error { id: self.id.clone(), message, transient });
    }
}

/// Delivers `entry`'s queued events to the observer, FIFO, never holding the
/// entry lock across a callback. At most one drainer runs per entry: a
/// thread that finds a drain in progress returns immediately (its event will
/// be delivered, in order, by the current drainer's loop), so control calls
/// like [`Manager::sleep`] never block behind an observer callback. The
/// `emitting` flag is cleared on unwind too, so a panicking observer cannot
/// permanently wedge the entry's event stream.
fn drain_events(shared: &ManagerShared, entry: &Entry) {
    struct ClearOnDrop<'a>(&'a Entry);
    impl Drop for ClearOnDrop<'_> {
        fn drop(&mut self) {
            plock(&self.0.shared).emitting = false;
        }
    }

    loop {
        let ev = {
            let mut g = plock(&entry.shared);
            if g.emitting {
                return;
            }
            match g.pending_events.pop_front() {
                Some(ev) => {
                    g.emitting = true;
                    ev
                }
                None => return,
            }
        };
        let guard = ClearOnDrop(entry);
        shared.emit(ev);
        drop(guard);
        // Loop: pick up events queued while the callback ran (their
        // enqueuers saw `emitting` and left delivery to us).
    }
}

/// When the worker's next self-scheduled round is due (nudges and control
/// changes always take effect immediately regardless).
#[derive(Clone, Copy)]
enum Due {
    Now,
    At(Instant),
    /// Nothing scheduled: wait for a nudge/control change.
    Never,
}

/// What the wait phase told the worker to do next.
enum Action {
    /// Run one round with this session token.
    Round(CancelToken),
    /// Entry is sleeping and the worker still holds a session (Writer +
    /// client): release it, then come back to park.
    Release,
    /// Exit the worker thread.
    Exit,
}

/// The single blocking wait both workers park in. Owns every wake-up rule:
/// shutdown beats everything; sleeping parks (after a one-time `Release` if
/// the worker still holds a session); while running, a nudge wins over the
/// `Failed` park which wins over the schedule.
fn wait_for_action(entry: &Entry, session_alive: bool, due: Due) -> Action {
    let mut g = plock(&entry.shared);
    loop {
        match g.ctrl {
            Ctrl::Shutdown => return Action::Exit,
            Ctrl::Sleeping => {
                if session_alive {
                    return Action::Release;
                }
                g = entry.cond.wait(g).unwrap_or_else(PoisonError::into_inner);
            }
            Ctrl::Running => {
                if g.nudged {
                    g.nudged = false;
                    return Action::Round(g.session.clone());
                }
                // Fatal errors park the worker: only a nudge (checked
                // above), resume, sleep, or shutdown moves it again.
                if g.state == DbState::Failed {
                    g = entry.cond.wait(g).unwrap_or_else(PoisonError::into_inner);
                    continue;
                }
                match due {
                    Due::Now => return Action::Round(g.session.clone()),
                    Due::Never => {
                        g = entry.cond.wait(g).unwrap_or_else(PoisonError::into_inner);
                    }
                    Due::At(t) => {
                        let now = Instant::now();
                        if now >= t {
                            return Action::Round(g.session.clone());
                        }
                        g = entry
                            .cond
                            .wait_timeout(g, t - now)
                            .unwrap_or_else(PoisonError::into_inner)
                            .0;
                    }
                }
            }
        }
    }
}

struct ManagerShared {
    opts: ManagerOptions,
    entries: Mutex<HashMap<String, Registered>>,
    observer: Mutex<Option<Arc<dyn ManagerObserver>>>,
}

struct Registered {
    entry: Arc<Entry>,
    handle: Option<JoinHandle<()>>,
}

impl ManagerShared {
    /// Emits to the current observer. Callers must not hold any entry or
    /// registry lock: the observer is app code.
    fn emit(&self, event: ManagerEvent) {
        let obs = plock(&self.observer).clone();
        if let Some(obs) = obs {
            obs.on_event(event);
        }
    }
}

// ---------------------------------------------------------------------------
// Manager

/// Runs replication for any number of registered databases on background
/// threads. See the module docs for the threading and session model.
///
/// Constructible with zero databases; registrations and removals are dynamic.
/// Dropping the manager (or calling [`Manager::shutdown`]) cancels and joins
/// every worker.
pub struct Manager {
    shared: Arc<ManagerShared>,
}

impl Manager {
    pub fn new(opts: ManagerOptions) -> Manager {
        Manager {
            shared: Arc::new(ManagerShared {
                opts,
                entries: Mutex::new(HashMap::new()),
                observer: Mutex::new(None),
            }),
        }
    }

    /// Registers a database to push to `cfg.storage` and starts its worker.
    /// The database file must exist (worker construction fails into the
    /// `Failed`/backoff path otherwise, it does not panic). Errors on a
    /// duplicate id or a db_path already registered under any id.
    pub fn register_push(
        &self,
        id: impl Into<String>,
        db_path: impl Into<PathBuf>,
        cfg: PushConfig,
    ) -> Result<()> {
        let id = id.into();
        let db_path: PathBuf = db_path.into();
        #[allow(unused_mut)] // mutated only when the http feature fills writer_id
        let mut cfg = cfg;

        // HTTP pushes are fenced server-side by writer id: fill in the
        // per-database persisted id when the app didn't choose one. Done
        // before taking the registry lock (touches the filesystem).
        #[cfg(feature = "http")]
        if let StorageConfig::Http { options, .. } = &mut cfg.storage {
            if options.writer_id.is_none() {
                options.writer_id = Some(MetaDir::for_db(&db_path).writer_id()?);
            }
        }

        let entry = Arc::new(Entry::new(id.clone(), db_path, DbRole::Push));
        let shared = Arc::clone(&self.shared);
        self.insert_and_spawn(entry.clone(), move || push_worker(shared, entry, cfg))
    }

    /// Registers a database to follow (materialize) a bucket and starts its
    /// worker. `db_path` need not exist yet — the first sync restores it.
    /// Errors on a duplicate id or a db_path already registered under any id.
    pub fn register_follow(
        &self,
        id: impl Into<String>,
        db_path: impl Into<PathBuf>,
        cfg: FollowConfig,
    ) -> Result<()> {
        let entry = Arc::new(Entry::new(id.into(), db_path.into(), DbRole::Follow));
        let shared = Arc::clone(&self.shared);
        self.insert_and_spawn(entry.clone(), move || follow_worker(shared, entry, cfg))
    }

    fn insert_and_spawn(
        &self,
        entry: Arc<Entry>,
        worker: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        let mut entries = plock(&self.shared.entries);
        if entries.contains_key(&entry.id) {
            return Err(Error::Other(format!("db id already registered: {:?}", entry.id)));
        }
        if let Some(other) = entries.values().find(|r| r.entry.canonical == entry.canonical) {
            return Err(Error::Other(format!(
                "db path {:?} already registered as {:?}",
                entry.db_path, other.entry.id
            )));
        }
        let handle = std::thread::Builder::new()
            .name(format!("liters-mgr-{}", entry.id))
            .spawn(worker)
            .map_err(|e| Error::Other(format!("spawn worker thread: {e}")))?;
        let id = entry.id.clone();
        entries.insert(id, Registered { entry, handle: Some(handle) });
        Ok(())
    }

    fn get(&self, id: &str) -> Result<Arc<Entry>> {
        plock(&self.shared.entries)
            .get(id)
            .map(|r| Arc::clone(&r.entry))
            .ok_or_else(|| Error::Other(format!("unknown db id: {id:?}")))
    }

    /// Removes a registration: cancels its session, joins the worker, and
    /// drops its Writer/Replica. Blocks until the worker exits (bounded by
    /// the storage backend's cancellation latency). The id becomes free for
    /// re-registration. Unknown id → `Err`.
    pub fn unregister(&self, id: &str) -> Result<()> {
        let mut reg = plock(&self.shared.entries)
            .remove(id)
            .ok_or_else(|| Error::Other(format!("unknown db id: {id:?}")))?;
        // Registry lock released: the join below must never block other
        // control calls.
        signal_shutdown(&reg.entry);
        if let Some(handle) = reg.handle.take() {
            let _ = handle.join();
        }
        Ok(())
    }

    /// Puts a database to sleep: cancels the in-flight operation (mid-
    /// transfer on token-aware backends), after which the worker drops its
    /// Writer — releasing the WAL-pinning read transaction and every fd —
    /// and parks with zero storage traffic. Returns immediately; the
    /// teardown completes asynchronously on the worker. Idempotent.
    pub fn sleep(&self, id: &str) -> Result<()> {
        let entry = self.get(id)?;
        self.sleep_entry(&entry);
        Ok(())
    }

    /// Wakes a sleeping database: installs a fresh session token and
    /// schedules an immediate push/sync round (which rebuilds client and
    /// Writer from `StorageConfig`). On a running-but-`Failed` entry this
    /// acts as a retry nudge. Idempotent.
    pub fn resume(&self, id: &str) -> Result<()> {
        let entry = self.get(id)?;
        self.resume_entry(&entry);
        Ok(())
    }

    pub fn sleep_all(&self) {
        for entry in self.snapshot_entries() {
            self.sleep_entry(&entry);
        }
    }

    pub fn resume_all(&self) {
        for entry in self.snapshot_entries() {
            self.resume_entry(&entry);
        }
    }

    /// Nudges a push registration to run a round now (also the retry path
    /// out of `Failed`). Ignored while sleeping — resume first. Errors on an
    /// unknown id or a follow registration.
    pub fn push_now(&self, id: &str) -> Result<()> {
        let entry = self.get(id)?;
        if entry.role != DbRole::Push {
            return Err(Error::Other(format!("{id:?} is a follow registration; use sync_now")));
        }
        let mut g = plock(&entry.shared);
        if g.ctrl != Ctrl::Running {
            return Ok(()); // nudges are ignored while sleeping
        }
        g.nudged = true;
        drop(g);
        entry.cond.notify_all();
        Ok(())
    }

    /// Forces a follow registration to resync immediately by ending the
    /// current follow session (token swap) and starting a fresh one — a
    /// follow restart begins with a full `sync()` round by design. Also the
    /// retry path out of `Failed`. Ignored while sleeping. Errors on an
    /// unknown id or a push registration.
    pub fn sync_now(&self, id: &str) -> Result<()> {
        let entry = self.get(id)?;
        if entry.role != DbRole::Follow {
            return Err(Error::Other(format!("{id:?} is a push registration; use push_now")));
        }
        let mut g = plock(&entry.shared);
        if g.ctrl != Ctrl::Running {
            return Ok(()); // nudges are ignored while sleeping
        }
        // Cancel-then-replace under one lock: the in-flight session sees
        // only its own (old) token cancelled, the restart clones the fresh
        // one.
        g.session.cancel();
        g.session = CancelToken::new();
        g.nudged = true;
        drop(g);
        entry.cond.notify_all();
        Ok(())
    }

    pub fn status(&self, id: &str) -> Option<DbStatus> {
        let entry = plock(&self.shared.entries).get(id).map(|r| Arc::clone(&r.entry))?;
        Some(status_of(&entry))
    }

    /// Statuses of all registered databases, sorted by id.
    pub fn statuses(&self) -> Vec<DbStatus> {
        let mut entries = self.snapshot_entries();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        entries.iter().map(|e| status_of(e)).collect()
    }

    /// Installs (or clears) the observer. Takes effect for subsequently
    /// emitted events; events already being delivered may still reach the
    /// previous observer.
    pub fn set_observer(&self, observer: Option<Arc<dyn ManagerObserver>>) {
        *plock(&self.shared.observer) = observer;
    }

    /// Cancels every session and joins every worker thread. Idempotent;
    /// also runs on drop. Bounded by the storage backends' cancellation
    /// latency (a few seconds worst case for the token-aware HTTP client),
    /// not by push/follow schedules.
    pub fn shutdown(&self) {
        let taken: Vec<Registered> = plock(&self.shared.entries).drain().map(|(_, r)| r).collect();
        // Signal everything first so workers wind down in parallel, then
        // join; joining one at a time before signalling the rest would
        // serialize the cancellation latencies.
        for reg in &taken {
            signal_shutdown(&reg.entry);
        }
        for mut reg in taken {
            if let Some(handle) = reg.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn snapshot_entries(&self) -> Vec<Arc<Entry>> {
        plock(&self.shared.entries).values().map(|r| Arc::clone(&r.entry)).collect()
    }

    fn sleep_entry(&self, entry: &Entry) {
        {
            let mut g = plock(&entry.shared);
            // Never overwrite Shutdown (an unregister/shutdown racing this
            // call must still win, or its join would hang on a parked
            // worker); re-sleeping a sleeper is a no-op.
            if g.ctrl != Ctrl::Running {
                return;
            }
            g.ctrl = Ctrl::Sleeping;
            g.session.cancel();
            g.state = DbState::Sleeping;
            g.pending_events.push_back(ManagerEvent::StateChanged {
                id: entry.id.clone(),
                state: DbState::Sleeping,
            });
        }
        drain_events(&self.shared, entry);
        entry.cond.notify_all();
    }

    fn resume_entry(&self, entry: &Entry) {
        {
            let mut g = plock(&entry.shared);
            match g.ctrl {
                Ctrl::Shutdown => return,
                Ctrl::Running => {
                    // Not asleep — but resume() doubles as the documented
                    // way out of Failed, retrying once.
                    if g.state == DbState::Failed {
                        g.nudged = true;
                        drop(g);
                        entry.cond.notify_all();
                    }
                    return;
                }
                Ctrl::Sleeping => {
                    g.ctrl = Ctrl::Running;
                    // Fresh token: the slept session's cancelled token must
                    // never leak into the new session.
                    g.session = CancelToken::new();
                    g.nudged = true; // schedule an immediate round
                    g.state = DbState::Idle;
                }
            }
        }
        // Deliberately no StateChanged(Idle) event: the resumed round's
        // Working transition is the observable; status() reports Idle in
        // the gap.
        entry.cond.notify_all();
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn signal_shutdown(entry: &Entry) {
    {
        let mut g = plock(&entry.shared);
        g.ctrl = Ctrl::Shutdown;
        g.session.cancel();
    }
    entry.cond.notify_all();
}

fn status_of(entry: &Entry) -> DbStatus {
    // Follower position/activity come from the sidecar files, read before
    // taking the entry lock (file I/O outside any lock).
    let (follow_pos, follow_activity) = match entry.role {
        DbRole::Follow => (
            read_txid_file(&entry.db_path).ok().filter(|t| !t.is_zero()),
            std::fs::metadata(txid_path(&entry.db_path)).and_then(|m| m.modified()).ok(),
        ),
        DbRole::Push => (None, None),
    };
    let g = plock(&entry.shared);
    DbStatus {
        id: entry.id.clone(),
        role: entry.role,
        state: g.state.clone(),
        position: match entry.role {
            DbRole::Push => g.position,
            DbRole::Follow => follow_pos,
        },
        last_error: g.last_error.clone(),
        last_activity: match entry.role {
            DbRole::Push => g.last_activity,
            DbRole::Follow => follow_activity,
        },
    }
}

// ---------------------------------------------------------------------------
// Workers

/// Builds the Writer lazily (fresh client per session; `Writer::open` does
/// no network I/O, so this succeeds offline).
fn ensure_writer<'a>(
    writer: &'a mut Option<Writer>,
    cfg: &PushConfig,
    db_path: &Path,
) -> Result<&'a mut Writer> {
    if writer.is_none() {
        let client = cfg.storage.build()?;
        *writer = Some(Writer::open(db_path, client, cfg.writer_options.clone())?);
    }
    Ok(writer.as_mut().expect("just constructed"))
}

fn push_worker(shared: Arc<ManagerShared>, entry: Arc<Entry>, cfg: PushConfig) {
    let backoff = cfg.backoff.clone().unwrap_or_else(|| shared.opts.backoff.clone());
    let interval = cfg.push_interval.or(shared.opts.default_push_interval);

    let mut writer: Option<Writer> = None;
    // Consecutive transient-failure count; drives backoff.delay and resets
    // on any success.
    let mut attempt: u32 = 0;
    let mut retry_at: Option<Instant> = None;
    // With an interval configured the first push runs immediately, so a
    // fresh registration replicates without waiting out a full period.
    let mut next_push: Option<Instant> = interval.map(|_| Instant::now());
    let mut last_maintain: Option<Instant> = None;

    loop {
        // An armed retry supersedes the interval tick: the retry IS the
        // next push, and running both would double-fire.
        let due = match (retry_at, next_push) {
            (Some(t), _) | (None, Some(t)) => Due::At(t),
            (None, None) => Due::Never,
        };
        let token = match wait_for_action(&entry, writer.is_some(), due) {
            Action::Exit => return,
            Action::Release => {
                // Sleeping: dropping the Writer releases the WAL-pinning
                // read transaction, both SQLite connections, and the raw db
                // fd. Done outside the entry lock (Drop runs SQLite calls).
                writer = None;
                continue;
            }
            Action::Round(token) => token,
        };
        retry_at = None;

        entry.transition_running(DbState::Working);
        drain_events(&shared, &entry);

        match ensure_writer(&mut writer, &cfg, &entry.db_path).and_then(|w| w.push_with(&token)) {
            Ok(result) => {
                attempt = 0;
                {
                    let mut g = plock(&entry.shared);
                    g.position = Some(result.txid);
                    g.last_activity = Some(SystemTime::now());
                    g.last_error = None;
                    g.pending_events
                        .push_back(ManagerEvent::PushCompleted { id: entry.id.clone(), result });
                }
                drain_events(&shared, &entry);

                if let Some((mopts, every)) = &cfg.maintenance {
                    // The cadence measures attempts, not successes: a
                    // persistently failing maintain must not turn every
                    // subsequent push into a maintain attempt.
                    if last_maintain.is_none_or(|t| t.elapsed() >= *every) {
                        last_maintain = Some(Instant::now());
                        match writer.as_mut().expect("push succeeded").maintain_with(&token, mopts)
                        {
                            Ok(_) => {}
                            // Sleep/shutdown mid-maintain: the wait phase
                            // routes it; nothing to report.
                            Err(Error::Cancelled) => {}
                            Err(e) => {
                                // Reported but deliberately neither Failed
                                // nor backoff: a maintain failure never
                                // blocks future pushes.
                                entry.record_error(e.to_string(), e.is_transient());
                                drain_events(&shared, &entry);
                            }
                        }
                    }
                }
                entry.transition_running(DbState::Idle);
                drain_events(&shared, &entry);
            }
            // Expected during sleep/shutdown (and the benign sleep→resume
            // race, where resume's nudge reruns us with the fresh token):
            // silent, the wait phase routes it.
            Err(Error::Cancelled) => {}
            Err(e) if e.is_transient() => {
                let delay = backoff.delay(attempt);
                attempt = attempt.saturating_add(1);
                retry_at = Some(Instant::now() + delay);
                entry.record_error(e.to_string(), true);
                entry.transition_running(DbState::BackingOff {
                    until: SystemTime::now() + delay,
                    attempt,
                });
                drain_events(&shared, &entry);
            }
            Err(e) => {
                attempt = 0;
                // End the session: the retry (nudge/resume) starts over
                // with a fresh client from StorageConfig, so a client wedged
                // by whatever failed fatally can't outlive its failure.
                writer = None;
                entry.record_error(e.to_string(), false);
                entry.transition_running(DbState::Failed);
                drain_events(&shared, &entry);
            }
        }
        next_push = interval.map(|i| Instant::now() + i);
    }
}

fn follow_worker(shared: Arc<ManagerShared>, entry: Arc<Entry>, cfg: FollowConfig) {
    let mut fo = cfg.follow_options.clone();
    if fo.retry.is_none() {
        // Transient reconnection is handled INSIDE follow() (backoff, resync,
        // resume-from-sidecar); only fatal errors escape to this worker.
        fo.retry = Some(shared.opts.backoff.clone());
    }
    let backoff = fo.retry.clone().expect("installed above");

    // Client-construction failures are the only transient errors this
    // worker sees (follow retries its own); they get the same backoff.
    let mut attempt: u32 = 0;
    let mut retry_at: Option<Instant> = None;

    loop {
        // A running follower is always due: its "round" is a whole follow
        // session that only ends on cancellation or a fatal error.
        let due = match retry_at {
            Some(t) => Due::At(t),
            None => Due::Now,
        };
        let token = match wait_for_action(&entry, false, due) {
            Action::Exit => return,
            // Follow sessions hold nothing between rounds (the Replica and
            // its client are per-session locals).
            Action::Release => continue,
            Action::Round(token) => token,
        };
        retry_at = None;

        let client = match cfg.storage.build() {
            Ok(client) => client,
            Err(Error::Cancelled) => continue,
            Err(e) => {
                let transient = e.is_transient();
                entry.record_error(e.to_string(), transient);
                let next_state = if transient {
                    let delay = backoff.delay(attempt);
                    attempt = attempt.saturating_add(1);
                    retry_at = Some(Instant::now() + delay);
                    DbState::BackingOff { until: SystemTime::now() + delay, attempt }
                } else {
                    attempt = 0;
                    DbState::Failed
                };
                entry.transition_running(next_state);
                drain_events(&shared, &entry);
                continue;
            }
        };
        attempt = 0;

        let mut replica = Replica::open(&entry.db_path, client, cfg.replica_options.clone());
        entry.transition_running(DbState::Working);
        drain_events(&shared, &entry);

        match replica.follow(&token, &fo) {
            // Clean stop: the session token was cancelled — by sleep (the
            // wait phase parks), shutdown (it exits), or a sync_now session
            // swap (it immediately starts a fresh session, which begins
            // with a full sync round).
            Ok(()) => {}
            Err(e) => {
                // Fatal for this configuration (e.g. Diverged without
                // auto_reset): park until sync_now/resume retries.
                entry.record_error(e.to_string(), e.is_transient());
                entry.transition_running(DbState::Failed);
                drain_events(&shared, &entry);
            }
        }
    }
}
