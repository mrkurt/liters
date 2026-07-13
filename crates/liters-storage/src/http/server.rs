//! Embeddable HTTP server that serves databases' buckets to other liters
//! instances: listings, LTX file bodies, and a long-lived `/stream` endpoint
//! that pushes new level-0 files to followers as the writer produces them.
//! Wire protocol: docs/http-protocol.md.
//!
//! One server can carry many databases: [`HttpServer::bind`] serves a root
//! bucket at `/ltx/...` (and still accepts added DBs), while
//! [`HttpServer::bind_multi`] starts with none; either way,
//! [`HttpServer::add_db`]/[`HttpServer::remove_db`] register buckets under
//! `/db/{name}/...` while serving. Each bucket has its own `/stream` wake
//! signal and its own writer lease (fencing). Optional bearer-token auth
//! ([`HttpServerOptions::auth_token`]) gates every route except the
//! `GET /` health check.
//!
//! Every endpoint can be mounted under a URL path prefix
//! ([`HttpServerOptions::base_path`], e.g. `/db`) so the server shares an
//! origin with unrelated apps behind a path-routing reverse proxy; the
//! prefix is stripped before routing, so the root and `/db/{name}` layouts
//! are unchanged beneath it. Followers reach a mounted server through the
//! client URL's base path (`http://host:port/db`).
//!
//! The server reads from any [`ReplicaClient`]; the intended source is a
//! [`DirReplicaClient`](crate::DirReplicaClient) over the same bucket the
//! local `Writer` pushes to. Wrap the writer's client with
//! [`HttpServer::notifying_client`] so pushes wake `/stream` followers
//! immediately; without the tee (e.g. an external process writes the
//! bucket), streamers still pick changes up by re-listing every
//! `poll_interval`. Serving an S3-backed client works but is not the design
//! target: the S3 backend buffers whole objects in memory and serializes on
//! its private runtime.
//!
//! With `writable: true` the roles reverse: this server *receives*
//! replication. A remote `Writer` whose destination is an
//! [`HttpReplicaClient`](super::HttpReplicaClient) pushes its LTX files
//! here (`PUT`/`DELETE`), useful when the writer can dial out but cannot be
//! reached (NAT, mobile). Accepted writes wake `/stream` followers, so a
//! writable server is simultaneously a relay: devices push in, downstream
//! replicas stream out, and the local process can follow its own bucket
//! over loopback for a live materialized copy.
//!
//! Threading: one accept thread plus one thread per connection (peers are
//! few — they are replicas, not browsers). All are joined by
//! [`HttpServer::shutdown`], which also closes peer sockets so threads
//! blocked in writes exit immediately. Mutations of one bucket
//! (`PUT`/`DELETE`) are serialized by a per-bucket write gate so the fence
//! decision is atomic with the commit; PUT bodies are spooled to a local
//! file *before* the gate is taken, so a slow uplink never stalls other
//! requests.

use std::collections::HashMap;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, UNIX_EPOCH};

use ltx::{parse_filename, FileInfo, Txid};

use crate::{CancelToken, ReplicaClient, Result, StorageError, SNAPSHOT_LEVEL};

use super::wire::{
    header, query_param, read_head, ChunkedReader, ChunkedWriter, PROTOCOL_HEADER,
    PROTOCOL_VERSION,
};

/// How long a connection may take to send its request head.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Per-read timeout while receiving a pushed file body. More lenient than
/// the head timeout: mobile uplinks stall; a PUT that stops flowing for 30s
/// is aborted (PUTs are idempotent — the writer re-pushes).
const PUT_BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// A peer that accepts zero bytes for this long is stalled or suspended
/// (slow-but-progressing drains never trip a per-write timeout).
const WRITE_TIMEOUT: Duration = Duration::from_secs(60);
/// Accept-loop poll cadence (the listener is non-blocking so shutdown never
/// races a blocked accept).
const ACCEPT_TICK: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub struct HttpServerOptions {
    /// How often `/stream` handlers re-list level 0 when idle. This is the
    /// change-latency bound for buckets written without the notifying tee.
    pub poll_interval: Duration,
    /// Keepalive cadence on `/stream`. Pings carry the bucket-wide max TXID
    /// (divergence evidence for idle followers). Must stay well under the
    /// client's 45s dead-man timeout.
    pub ping_interval: Duration,
    /// Accept pushed replication: `PUT`/`DELETE` on LTX files and
    /// `DELETE /all`, written through to the backing [`ReplicaClient`].
    /// This is how a remote `Writer` pushes to this server (reversed roles:
    /// the receiver listens, the writer dials out). Every accepted write
    /// wakes local `/stream` followers, so a writable server is also a
    /// relay. Default `false` (read-only). Set
    /// [`HttpServerOptions::auth_token`] or keep writable servers on
    /// private interfaces: an unauthenticated writable server accepts
    /// `DELETE /all` from anyone who can reach it.
    pub writable: bool,
    /// When set, every route except the `GET /` health check requires
    /// `authorization: Bearer <token>` (case-insensitive scheme, exact
    /// token match); anything else is `401`. `None` (the default) disables
    /// auth — behavior is byte-identical to servers without this option.
    pub auth_token: Option<String>,
    /// How long a writer lease is held after the owner's last accepted
    /// write before another writer id may claim the bucket without a
    /// takeover header (docs/http-protocol.md "Fencing"). Leases are
    /// in-memory and reset on restart.
    pub lease_ttl: Duration,
    /// Mount every endpoint under a URL path prefix so the server can share
    /// an origin with unrelated apps behind a path-routing reverse proxy.
    /// With `Some("/db")` the endpoints move under `/db` (`/db/ltx/...`,
    /// `/db/stream`, multi-DB at `/db/db/{name}/...`) and any request whose
    /// path is not under the prefix is `404`. Leading/trailing slashes are
    /// optional and interior `//` is collapsed; `None`, `""`, and `"/"` all
    /// mount at the root — byte-identical to a server without this option.
    /// The bare-root `GET /` health check answers regardless of the prefix.
    /// Followers address a mounted server through the client URL's base
    /// path (`http://host:port/db`).
    pub base_path: Option<String>,
}

impl Default for HttpServerOptions {
    fn default() -> Self {
        HttpServerOptions {
            poll_interval: Duration::from_secs(1),
            ping_interval: Duration::from_secs(15),
            writable: false,
            auth_token: None,
            lease_ttl: Duration::from_secs(24 * 60 * 60),
            base_path: None,
        }
    }
}

/// Per-bucket change signal: a generation counter under a mutex plus a
/// condvar. Bumped by the notifying tee and the write endpoints on every
/// mutation; that bucket's `/stream` handlers wait on it (with a
/// poll_interval timeout) when caught up. Server shutdown wakes every
/// bucket's waiters — the stop flag is part of the wait predicate.
struct Notify {
    generation: Mutex<u64>,
    cond: Condvar,
}

impl Notify {
    fn new() -> Notify {
        Notify { generation: Mutex::new(0), cond: Condvar::new() }
    }

    fn notify(&self) {
        let mut g = self.generation.lock().unwrap();
        *g = g.wrapping_add(1);
        drop(g);
        self.cond.notify_all();
    }

    /// Waits until the generation moves past `seen`, `stop` is set, or
    /// `timeout` elapses.
    fn wait(&self, seen: u64, timeout: Duration, stop: &AtomicBool) {
        let g = self.generation.lock().unwrap();
        let _unused = self
            .cond
            .wait_timeout_while(g, timeout, |g| *g == seen && !stop.load(Ordering::Relaxed))
            .unwrap();
    }

    fn generation(&self) -> u64 {
        *self.generation.lock().unwrap()
    }
}

/// The most recent writer to touch a bucket with an `x-liters-writer-id`
/// header. In-memory only — leases reset on server restart — so fencing is
/// a dual-writer *detector*, not a distributed lock; the TXID monotonicity
/// rule is what protects bucket integrity across restarts.
struct WriterLease {
    id: String,
    last_seen: Instant,
}

/// One served bucket: its backing client, its `/stream` wake signal, its
/// writer lease, and the write gate that serializes mutations.
struct Bucket {
    client: Arc<dyn ReplicaClient>,
    notify: Notify,
    lease: Mutex<Option<WriterLease>>,
    /// Serializes bucket mutations (`PUT`/`DELETE`) so the fence decision
    /// and the backend commit are atomic per bucket — without it two
    /// concurrent same-TXID PUTs could both pass the fence and splice
    /// divergent lineage. Never held across a network read: PUT bodies are
    /// spooled to a local file before the gate is taken.
    write_gate: Mutex<()>,
}

impl Bucket {
    fn new(client: Arc<dyn ReplicaClient>) -> Bucket {
        Bucket {
            client,
            notify: Notify::new(),
            lease: Mutex::new(None),
            write_gate: Mutex::new(()),
        }
    }
}

struct Shared {
    /// The bucket served at the root paths (`/ltx/...`, `/stream`); `None`
    /// for servers started with [`HttpServer::bind_multi`].
    root: Option<Arc<Bucket>>,
    /// Named buckets served under `/db/{name}/...`. Handlers resolve the
    /// bucket once per request, so a concurrent `remove_db` lets in-flight
    /// requests (including open streams) finish against the removed bucket
    /// — the Arc keeps it alive — while new requests 404.
    dbs: RwLock<HashMap<String, Arc<Bucket>>>,
    /// Weak refs to every bucket ever created. Shutdown must wake `/stream`
    /// handlers parked on a bucket's condvar even after `remove_db` made
    /// the bucket unreachable through `dbs` (in-flight streams deliberately
    /// keep serving it) — otherwise a parked handler sleeps out its whole
    /// poll interval and shutdown blocks joining it. Dead entries are
    /// pruned opportunistically.
    buckets: Mutex<Vec<Weak<Bucket>>>,
    /// The mount prefix ([`HttpServerOptions::base_path`]) as its non-empty
    /// path segments; empty for a root-mounted server. Stripped from every
    /// request path before routing.
    base_segments: Vec<String>,
    opts: HttpServerOptions,
    stop: AtomicBool,
}

struct Conn {
    stream: TcpStream,
    handle: Option<JoinHandle<()>>,
    done: Arc<AtomicBool>,
}

/// Serves one or more buckets over HTTP so other liters instances can
/// restore from them and follow them. See the module docs for wiring and
/// docs/http-protocol.md for the protocol.
pub struct HttpServer {
    shared: Arc<Shared>,
    local_addr: SocketAddr,
    accept: Option<JoinHandle<()>>,
    conns: Arc<Mutex<Vec<Conn>>>,
}

impl HttpServer {
    /// Binds and starts serving `client` at the root paths immediately
    /// (added DBs are also supported, see [`HttpServer::add_db`]). Use
    /// port 0 to let the OS pick (see [`HttpServer::local_addr`]). No TLS
    /// in protocol v1: bind loopback or a private interface, front with a
    /// reverse proxy, or at least set [`HttpServerOptions::auth_token`].
    pub fn bind(
        addr: impl ToSocketAddrs,
        client: Arc<dyn ReplicaClient>,
        opts: HttpServerOptions,
    ) -> Result<HttpServer> {
        Self::bind_inner(addr, Some(Arc::new(Bucket::new(client))), opts)
    }

    /// Binds with no root bucket: every database is registered dynamically
    /// with [`HttpServer::add_db`] and served under `/db/{name}/...`. The
    /// root paths answer only the `GET /` health check; everything else
    /// there is 404.
    pub fn bind_multi(addr: impl ToSocketAddrs, opts: HttpServerOptions) -> Result<HttpServer> {
        Self::bind_inner(addr, None, opts)
    }

    fn bind_inner(
        addr: impl ToSocketAddrs,
        root: Option<Arc<Bucket>>,
        opts: HttpServerOptions,
    ) -> Result<HttpServer> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;

        let base_segments = normalize_base(opts.base_path.as_deref());
        let shared = Arc::new(Shared {
            buckets: Mutex::new(root.iter().map(Arc::downgrade).collect()),
            root,
            dbs: RwLock::new(HashMap::new()),
            base_segments,
            opts,
            stop: AtomicBool::new(false),
        });
        let conns: Arc<Mutex<Vec<Conn>>> = Arc::new(Mutex::new(Vec::new()));

        let accept = {
            let shared = Arc::clone(&shared);
            let conns = Arc::clone(&conns);
            std::thread::Builder::new()
                .name("liters-http-accept".into())
                .spawn(move || accept_loop(listener, shared, conns))?
        };

        Ok(HttpServer { shared, local_addr, accept: Some(accept), conns })
    }

    /// The bound address (resolves port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Registers a bucket served under `/db/{name}/...`. Callable while
    /// serving. `name` must match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`;
    /// registering a duplicate name is an error.
    pub fn add_db(&self, name: &str, client: Arc<dyn ReplicaClient>) -> Result<()> {
        if !valid_db_name(name) {
            return Err(StorageError::Other(format!(
                "invalid db name {name:?}: want [A-Za-z0-9][A-Za-z0-9._-]{{0,127}}"
            )));
        }
        let mut dbs = self.shared.dbs.write().unwrap();
        if dbs.contains_key(name) {
            return Err(StorageError::Other(format!("db {name:?} is already registered")));
        }
        let bucket = Arc::new(Bucket::new(client));
        {
            // Register for the shutdown wake (see Shared::buckets), pruning
            // buckets no request holds anymore.
            let mut registry = self.shared.buckets.lock().unwrap();
            registry.retain(|w| w.strong_count() > 0);
            registry.push(Arc::downgrade(&bucket));
        }
        dbs.insert(name.to_string(), bucket);
        Ok(())
    }

    /// Unregisters a named bucket so new requests 404; returns whether the
    /// name was registered. In-flight requests (including open `/stream`s)
    /// may finish against the removed bucket — that is deliberate; the
    /// bucket only becomes unreachable, not invalid.
    pub fn remove_db(&self, name: &str) -> bool {
        self.shared.dbs.write().unwrap().remove(name).is_some()
    }

    /// Wraps a [`ReplicaClient`] so every successful mutation wakes the
    /// *root* bucket's `/stream` followers. Hand the wrapped client to the
    /// local `Writer` and pushes are delivered to followers with no poll
    /// latency.
    ///
    /// # Panics
    ///
    /// If the server was started with [`HttpServer::bind_multi`] (no root
    /// bucket) — use [`HttpServer::notifying_client_for`] instead.
    pub fn notifying_client(&self, inner: Box<dyn ReplicaClient>) -> Box<dyn ReplicaClient> {
        let bucket = self
            .shared
            .root
            .clone()
            .expect("server has no root bucket (bind_multi); use notifying_client_for");
        Box::new(NotifyingClient { inner, bucket })
    }

    /// Per-DB variant of [`HttpServer::notifying_client`]: mutations wake
    /// the named bucket's `/stream` followers. Errors if `name` is not
    /// registered.
    pub fn notifying_client_for(
        &self,
        name: &str,
        inner: Box<dyn ReplicaClient>,
    ) -> Result<Box<dyn ReplicaClient>> {
        let bucket = self
            .shared
            .dbs
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| StorageError::Other(format!("no such db {name:?}")))?;
        Ok(Box::new(NotifyingClient { inner, bucket }))
    }

    /// Stops accepting, closes every peer socket (unblocking any thread
    /// stuck in a write to a stalled peer), and joins all threads.
    /// Idempotent; also runs on drop.
    pub fn shutdown(&mut self) {
        if !self.shared.stop.swap(true, Ordering::SeqCst) {
            // Wake every live bucket's `/stream` waiters through the
            // registry — it covers the root bucket, registered DBs, AND
            // buckets removed via remove_db that in-flight streams keep
            // alive (unreachable through `dbs`, but their condvars still
            // have parked waiters). Missing one would block the join below
            // for up to a full poll_interval.
            let mut registry = self.shared.buckets.lock().unwrap();
            registry.retain(|w| match w.upgrade() {
                Some(bucket) => {
                    bucket.notify.notify();
                    true
                }
                None => false,
            });
        }
        {
            let conns = self.conns.lock().unwrap();
            for conn in conns.iter() {
                let _ = conn.stream.shutdown(Shutdown::Both);
            }
        }
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
        // Close again in the drain pass: a connection the accept thread
        // registered *after* the first close pass would otherwise be joined
        // with its socket still open — an unbounded hang.
        let mut conns = self.conns.lock().unwrap();
        for mut conn in conns.drain(..) {
            let _ = conn.stream.shutdown(Shutdown::Both);
            if let Some(h) = conn.handle.take() {
                let _ = h.join();
            }
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn accept_loop(listener: TcpListener, shared: Arc<Shared>, conns: Arc<Mutex<Vec<Conn>>>) {
    while !shared.stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _peer)) => {
                if configure_socket(&stream).is_err() {
                    continue;
                }
                // The registry clone must exist BEFORE the handler thread:
                // an unregistered handler would be invisible to shutdown()
                // (unclosable, unjoinable). No clone -> drop the connection;
                // the peer retries.
                let Ok(registered) = stream.try_clone() else { continue };
                let done = Arc::new(AtomicBool::new(false));
                let handle = {
                    let shared = Arc::clone(&shared);
                    let done = Arc::clone(&done);
                    std::thread::Builder::new()
                        .name("liters-http-conn".into())
                        .spawn(move || {
                            let _guard = DoneGuard(&done);
                            handle_connection(stream, &shared);
                        })
                };
                if let Ok(handle) = handle {
                    conns.lock().unwrap().push(Conn {
                        stream: registered,
                        handle: Some(handle),
                        done,
                    });
                }
            }
            // Non-blocking listener: poll so shutdown is never stuck in
            // accept regardless of bind interface. Other errors (EMFILE...)
            // get the same backoff.
            Err(_) => std::thread::sleep(ACCEPT_TICK),
        }
        reap(&conns);
    }
}

struct DoneGuard<'a>(&'a AtomicBool);

impl Drop for DoneGuard<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn reap(conns: &Mutex<Vec<Conn>>) {
    let mut conns = conns.lock().unwrap();
    conns.retain_mut(|c| {
        if c.done.load(Ordering::Acquire) {
            if let Some(h) = c.handle.take() {
                let _ = h.join();
            }
            false
        } else {
            true
        }
    });
}

fn configure_socket(stream: &TcpStream) -> std::io::Result<()> {
    // Accepted sockets inherit O_NONBLOCK from the listener on BSD-derived
    // platforms; undo it explicitly everywhere.
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(REQUEST_READ_TIMEOUT))?;
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
    set_nosigpipe(stream);
    Ok(())
}

/// Embedded (non-`fn main`) processes on Apple platforms may not ignore
/// SIGPIPE; a write to a disconnected peer would kill the app. Linux/Android
/// need nothing: std sends with MSG_NOSIGNAL there.
#[cfg(target_vendor = "apple")]
fn set_nosigpipe(stream: &TcpStream) {
    use std::os::unix::io::AsRawFd;
    let one: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            &one as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

#[cfg(not(target_vendor = "apple"))]
fn set_nosigpipe(_stream: &TcpStream) {}

// ---------------------------------------------------------------------------
// Request handling

fn handle_connection(mut stream: TcpStream, shared: &Shared) {
    let Ok((request_line, headers)) = read_head(&mut stream) else {
        return;
    };
    let mut parts = request_line.split_whitespace();
    let (method, target) = match (parts.next(), parts.next()) {
        (Some(m), Some(t)) => (m, t),
        _ => return,
    };

    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (target, ""),
    };

    // All error paths write-and-return; write failures mean the peer is
    // gone, which is fine.
    let _ = route(&mut stream, shared, method, path, query, &headers);
}

fn route(
    stream: &mut TcpStream,
    shared: &Shared,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(String, String)],
) -> std::io::Result<()> {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();

    // The health check stays open (liveness probes carry no secrets);
    // everything else — including `/db/{name}` probes — sits behind auth.
    if let ("GET", [""]) = (method, segments.as_slice()) {
        return respond_text(stream, 200, &format!("liters {}\n", env!("CARGO_PKG_VERSION")));
    }

    if let Some(expected) = &shared.opts.auth_token {
        if !authorized(headers, expected) {
            // Drain (bounded) so the rejection reaches a pusher already
            // streaming a large body, instead of an RST eating it.
            let r = respond_error(stream, 401, "authorization required");
            if method == "PUT" {
                drain_body(stream);
            }
            return r;
        }
    }

    // Strip the configured mount prefix (empty for a root-mounted server —
    // then this is a no-op and routing is byte-identical to before the
    // option existed). A path that is not under the mount belongs to some
    // other app sharing this origin behind a path-routing proxy: 404 it,
    // after auth so the token gates any probing of the mount layout. A
    // request landing exactly on the mount root becomes `[""]`, so the
    // `[] | [""]` version-probe arm fires just as `/` does at the root.
    let base = &shared.base_segments;
    let under_mount = segments.len() >= base.len()
        && base.iter().zip(&segments).all(|(b, s)| b.as_str() == *s);
    if !under_mount {
        let r = respond_error(stream, 404, "not found");
        if method == "PUT" || has_body(headers) {
            drain_body(stream);
        }
        return r;
    }
    let mount_rest = &segments[base.len()..];
    let root_slot = [""];
    let routed: &[&str] = if mount_rest.is_empty() { &root_slot } else { mount_rest };

    // `/db/{name}/...` resolves a registered bucket; every other path is
    // the root bucket. Resolution happens once per request: a concurrently
    // removed DB's in-flight requests finish, new ones 404.
    let (bucket, rest): (Arc<Bucket>, &[&str]) = match routed {
        ["db", name, rest @ ..] => {
            match shared.dbs.read().unwrap().get(*name).cloned() {
                Some(bucket) => (bucket, rest),
                None => {
                    let r = respond_error(stream, 404, "no such db");
                    if method == "PUT" {
                        drain_body(stream);
                    }
                    return r;
                }
            }
        }
        rest => match shared.root.clone() {
            Some(bucket) => (bucket, rest),
            None => {
                // bind_multi server: nothing lives at the root.
                let r = respond_error(stream, 404, "no such db");
                if method == "PUT" {
                    drain_body(stream);
                }
                return r;
            }
        },
    };

    route_bucket(stream, shared, &bucket, method, rest, query, headers)
}

/// Routes one request against its resolved bucket. `segments` is the path
/// with any `/db/{name}` prefix already stripped, so the root bucket and
/// named buckets share these match arms exactly.
fn route_bucket(
    stream: &mut TcpStream,
    shared: &Shared,
    bucket: &Bucket,
    method: &str,
    segments: &[&str],
    query: &str,
    headers: &[(String, String)],
) -> std::io::Result<()> {
    match (method, segments) {
        // `GET /db/{name}` — same version line as `GET /`, a handy probe
        // that a name resolves. (The root form was answered pre-auth.)
        ("GET", [] | [""]) => {
            respond_text(stream, 200, &format!("liters {}\n", env!("CARGO_PKG_VERSION")))
        }
        ("GET", ["ltx", level]) => {
            let Some(level) = parse_level(level) else {
                return respond_error(stream, 404, "no such level");
            };
            serve_listing(stream, bucket, level, query)
        }
        ("GET", ["ltx", level, name]) => {
            let Some(level) = parse_level(level) else {
                return respond_error(stream, 404, "no such level");
            };
            let Some((min_txid, max_txid)) = parse_filename(name) else {
                return respond_error(stream, 404, "no such file");
            };
            serve_file(stream, shared, bucket, level, min_txid, max_txid, query)
        }
        ("GET", ["stream"]) => serve_stream(stream, shared, bucket, query),
        ("PUT", ["ltx", level, name]) => {
            if !shared.opts.writable {
                // Drain (bounded) so the rejection reaches a pusher already
                // streaming a large body, instead of an RST eating it.
                let r = respond_error(stream, 403, "server is read-only (writable: false)");
                drain_body(stream);
                return r;
            }
            let Some(level) = parse_level(level) else {
                let r = respond_error(stream, 404, "no such level");
                drain_body(stream);
                return r;
            };
            let Some((min_txid, max_txid)) = parse_filename(name) else {
                let r = respond_error(stream, 404, "bad ltx filename");
                drain_body(stream);
                return r;
            };
            // Advisory pre-body fence: reject clearly-bad pushes before
            // their body is received (saves the uplink). It commits nothing
            // — no lease is taken — because the *authoritative* fence runs
            // again under the bucket's write gate in accept_file, once the
            // body has been fully spooled.
            if let Err(rej) =
                check_fence(bucket, &shared.opts, headers, Some((level, min_txid, max_txid)))
            {
                let r = respond_fence_reject(stream, &rej);
                drain_body(stream);
                return r;
            }
            accept_file(stream, shared, bucket, level, min_txid, max_txid, headers)
        }
        ("DELETE", ["ltx", level, name]) => {
            if !shared.opts.writable {
                return respond_error(stream, 403, "server is read-only (writable: false)");
            }
            let Some(level) = parse_level(level) else {
                return respond_error(stream, 404, "no such level");
            };
            let Some((min_txid, max_txid)) = parse_filename(name) else {
                return respond_error(stream, 404, "bad ltx filename");
            };
            // Deletes take the same write gate as PUTs: a fence must never
            // be evaluated against a bucket another request is mid-commit
            // into.
            let _gate = bucket.write_gate.lock().unwrap();
            let pass = match check_fence(bucket, &shared.opts, headers, None) {
                Ok(pass) => pass,
                Err(rej) => return respond_fence_reject(stream, &rej),
            };
            let info = FileInfo { level, min_txid, max_txid, ..Default::default() };
            match bucket.client.delete_ltx_files(&[info]) {
                Ok(()) => {
                    commit_lease(bucket, &pass);
                    bucket.notify.notify();
                    respond_text(stream, 200, "deleted\n")
                }
                Err(e) => respond_error(stream, 500, &format!("delete: {e}")),
            }
        }
        ("DELETE", ["all"]) => {
            if !shared.opts.writable {
                return respond_error(stream, 403, "server is read-only (writable: false)");
            }
            let _gate = bucket.write_gate.lock().unwrap();
            let pass = match check_fence(bucket, &shared.opts, headers, None) {
                Ok(pass) => pass,
                Err(rej) => return respond_fence_reject(stream, &rej),
            };
            match bucket.client.delete_all() {
                Ok(()) => {
                    commit_lease(bucket, &pass);
                    bucket.notify.notify();
                    respond_text(stream, 200, "deleted\n")
                }
                Err(e) => respond_error(stream, 500, &format!("delete all: {e}")),
            }
        }
        ("GET", _) | ("PUT", _) | ("DELETE", _) => {
            // Even unmatched paths must drain a request body (bounded)
            // before the connection closes: an RST from unread bytes can
            // destroy the 404 in flight and make a permanent
            // misconfiguration (wrong base path) look transient to the
            // pusher.
            let r = respond_error(stream, 404, "not found");
            if method == "PUT" || has_body(headers) {
                drain_body(stream);
            }
            r
        }
        _ => {
            let r = respond_error(stream, 405, "method not allowed");
            if has_body(headers) {
                drain_body(stream);
            }
            r
        }
    }
}

/// Whether the request head declares a body (chunked or a non-zero
/// content-length) — error responses must drain it (bounded) before the
/// connection closes.
fn has_body(headers: &[(String, String)]) -> bool {
    if header(headers, "transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        return true;
    }
    header(headers, "content-length")
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|n| n > 0)
}

/// Splits a configured mount prefix ([`HttpServerOptions::base_path`]) into
/// its non-empty path segments. `None`, `""`, `"/"`, and `"///"` all yield
/// an empty vec (root mount); leading/trailing slashes and interior `//` are
/// tolerated, matching how request paths are split (`trim_matches('/')` then
/// `split('/')`).
fn normalize_base(base: Option<&str>) -> Vec<String> {
    base.unwrap_or("")
        .split('/')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// `[A-Za-z0-9][A-Za-z0-9._-]{0,127}` — names can never collide with the
/// root endpoints and need no percent-encoding.
fn valid_db_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// `authorization: Bearer <token>` — case-insensitive scheme, exact token
/// match.
fn authorized(headers: &[(String, String)], expected: &str) -> bool {
    let Some(value) = header(headers, "authorization") else { return false };
    let Some((scheme, token)) = value.split_once(char::is_whitespace) else { return false };
    scheme.eq_ignore_ascii_case("bearer") && token.trim() == expected
}

enum FenceReject {
    /// 409: lease conflict or non-monotonic TXID.
    Conflict(String),
    /// Backend listing failure while evaluating the gate (500).
    Backend(StorageError),
}

/// A passed fence: what the caller may do, and what to commit if the
/// backend mutation succeeds.
struct FencePass {
    /// Writer id to record as the lease holder once the write is accepted
    /// (`None` for headerless v1 pushers). Rejected or failed requests
    /// never touch the lease — see [`commit_lease`].
    claim: Option<String>,
    /// For an L0 push that matched an existing exact `{min,max}` file (the
    /// idempotent re-push arm): the stored file's listing entry. The caller
    /// must verify the pushed content is byte-identical before accepting —
    /// a same-name push with different bytes is the dual-writer splice the
    /// fence exists to stop.
    existing_l0: Option<FileInfo>,
}

fn respond_fence_reject(stream: &mut TcpStream, rej: &FenceReject) -> std::io::Result<()> {
    match rej {
        FenceReject::Conflict(msg) => respond_error(stream, 409, msg),
        FenceReject::Backend(e) => respond_error(stream, 500, &format!("fence: {e}")),
    }
}

/// Records `pass.claim` as the bucket's lease holder. Called only after the
/// backend mutation succeeded, so the lease always reflects the owner's
/// last *accepted* write — a rejected or failed request neither claims a
/// free lease nor refreshes an existing one.
fn commit_lease(bucket: &Bucket, pass: &FencePass) {
    if let Some(id) = &pass.claim {
        *bucket.lease.lock().unwrap() =
            Some(WriterLease { id: id.clone(), last_seen: Instant::now() });
    }
}

/// Write-fencing gate (docs/http-protocol.md "Fencing"), applied after auth
/// and before the backend is touched. `put` carries a PUT's target; DELETEs
/// pass `None` (lease rules only — deletes are retention/GC, which TXID
/// monotonicity does not constrain).
///
/// Purely a *check*: it never mutates the lease (that is [`commit_lease`],
/// after the backend write succeeds). For PUTs it runs twice — once before
/// the body as an advisory early reject, and once under the bucket's write
/// gate as the authoritative decision, atomic with the commit.
///
/// Cost note: the monotonicity rules list the bucket per call — one L0
/// listing for L0 pushes, one listing per level (`bucket_max`) for
/// higher-level pushes; PUTs pay it twice (advisory + authoritative).
/// Deliberate: the design target is a `DirReplicaClient` backing where a
/// listing is one readdir; on a remote backing this would be extra
/// round-trips per accepted file.
fn check_fence(
    bucket: &Bucket,
    opts: &HttpServerOptions,
    headers: &[(String, String)],
    put: Option<(u8, Txid, Txid)>,
) -> std::result::Result<FencePass, FenceReject> {
    let mut pass = FencePass { claim: None, existing_l0: None };
    // Lease: only requests that identify themselves participate; plain v1
    // pushers (no header) skip lease logic entirely but still face
    // monotonicity.
    if let Some(id) = header(headers, "x-liters-writer-id") {
        let takeover = header(headers, "x-liters-writer-takeover") == Some("1");
        let lease = bucket.lease.lock().unwrap();
        let held_by_other = lease
            .as_ref()
            .filter(|l| l.id != id && l.last_seen.elapsed() < opts.lease_ttl);
        if let Some(l) = held_by_other {
            if !takeover {
                // Reveal only the owner's id, nothing more.
                return Err(FenceReject::Conflict(format!(
                    "bucket is owned by writer {}",
                    l.id
                )));
            }
        }
        pass.claim = Some(id.to_string());
    }

    let Some((level, min_txid, max_txid)) = put else { return Ok(pass) };
    if level == 0 {
        // L0 is the replication log: accept only an append (min == cur+1),
        // an idempotent re-push of an exact existing file (content
        // equality verified by the caller under the write gate), or the
        // first file into an empty L0.
        let files = bucket.client.ltx_files(0, Txid(0), false).map_err(FenceReject::Backend)?;
        let cur = files.iter().map(|f| f.max_txid).max().unwrap_or(Txid(0));
        let existing =
            files.iter().find(|f| f.min_txid == min_txid && f.max_txid == max_txid);
        if let Some(existing) = existing {
            pass.existing_l0 = Some(existing.clone());
        } else if !(files.is_empty() || min_txid.0 == cur.0 + 1) {
            return Err(FenceReject::Conflict(format!(
                "non-monotonic L0 push: {min_txid}-{max_txid} offered, bucket at {cur}"
            )));
        }
    } else {
        // Levels 1..9 only ever summarize *uploaded* history: the file's
        // max TXID may not exceed the bucket-wide max, so pushers must
        // upload their L0 backlog before compactions/snapshots
        // (docs/http-protocol.md "Fencing"). An exact re-push always
        // satisfies this (the existing file's max is part of bucket_max),
        // so no separate idempotency check is needed here.
        let bmax = bucket_max(bucket.client.as_ref()).map_err(FenceReject::Backend)?;
        if max_txid > bmax {
            return Err(FenceReject::Conflict(format!(
                "L{level} push beyond bucket max: {min_txid}-{max_txid} offered, bucket max \
                 is {bmax}; upload the L0 backlog before pushing compactions or snapshots"
            )));
        }
    }
    Ok(pass)
}

/// `PUT /ltx/{level}/{name}` — accepts one pushed LTX file. The body is
/// first received in full into an unlinked local spool file, so the
/// bucket's write gate is never held across a (possibly slow) network
/// read; then, under the write gate, the fence is re-evaluated against
/// fresh listings — atomic with the commit — and the file is written
/// through the backing client (which owns atomicity: tmp + rename on the
/// dir backend). Success takes/refreshes the writer lease and wakes
/// `/stream` followers, making a writable server a relay. Responds with
/// the file's listing line so the pusher gets an authoritative `FileInfo`
/// back.
fn accept_file(
    stream: &mut TcpStream,
    shared: &Shared,
    bucket: &Bucket,
    level: u8,
    min_txid: Txid,
    max_txid: Txid,
    headers: &[(String, String)],
) -> std::io::Result<()> {
    let _ = stream.set_read_timeout(Some(PUT_BODY_READ_TIMEOUT));

    let chunked = header(headers, "transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
    let content_length: Option<u64> =
        header(headers, "content-length").and_then(|v| v.parse().ok());

    let spooled = {
        let mut body: Box<dyn Read + '_> = if chunked {
            Box::new(ChunkedReader::new(&mut *stream))
        } else if let Some(n) = content_length {
            // ExactLen, not take(): a clean FIN short of content-length must
            // be an error, never a shorter file — a truncated body committed
            // with 200 would sit at the bucket's max TXID and the pusher
            // would never re-upload it (permanent corruption).
            Box::new(ExactLen { inner: &mut *stream, remaining: n })
        } else {
            let r = respond_error(stream, 411, "length required (content-length or chunked)");
            drain_body(stream);
            return r;
        };
        spool_put_body(&mut *body)
    };
    // A body that ends short of its declared length is the pusher's problem
    // (400); any other receive failure is 500. Both drain a bounded amount
    // of unread body so the response is delivered on a clean FIN instead of
    // racing an RST.
    let (mut spool, spool_len) = match spooled {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            let r = respond_error(stream, 400, &format!("short or truncated ltx body: {e}"));
            drain_body(stream);
            return r;
        }
        Err(e) => {
            let r = respond_error(stream, 500, &format!("read body: {e}"));
            drain_body(stream);
            return r;
        }
    };

    // Authoritative fence + commit, atomic per bucket. The body is fully
    // local now, so the gate is held only for local listings and the
    // backend write — never a network wait. From here on the request body
    // is fully consumed, so rejections need no drain.
    let _gate = bucket.write_gate.lock().unwrap();
    let pass =
        match check_fence(bucket, &shared.opts, headers, Some((level, min_txid, max_txid))) {
            Ok(pass) => pass,
            Err(rej) => return respond_fence_reject(stream, &rej),
        };

    if let Some(existing) = &pass.existing_l0 {
        // Idempotent re-push arm: an exact `{min,max}` L0 file already
        // exists. Only a byte-identical body is re-accepted — a writer's
        // crash-retry resends the same local file, so equality is the
        // normal case. The listing size is the cheap first gate; equal
        // sizes are then byte-compared against the stored file (one extra
        // read on a rare path, one open+read on the dir backing — the
        // design target). Divergent same-TXID content — two writers
        // restored from the same backup racing the same position — is 409,
        // so the loser learns its push was NOT accepted instead of having
        // its lineage silently spliced.
        let same = existing.size == spool_len
            && match stored_matches_spool(bucket, existing, &mut spool) {
                Ok(same) => same,
                Err(e) => return respond_error(stream, 500, &format!("fence: {e}")),
            };
        if !same {
            return respond_error(
                stream,
                409,
                &format!(
                    "L0 re-push of {min_txid}-{max_txid} does not match the stored file \
                     (divergent writer?)"
                ),
            );
        }
    }

    if let Err(e) = spool.seek(SeekFrom::Start(0)) {
        return respond_error(stream, 500, &format!("spool: {e}"));
    }
    match bucket.client.write_ltx_file(level, min_txid, max_txid, &mut spool) {
        Ok(info) => {
            commit_lease(bucket, &pass);
            bucket.notify.notify();
            respond_text(stream, 200, &listing_line(&info))
        }
        // A body that does not start with a valid 100-byte LTX header (or
        // is shorter than one) is the pusher's problem (400); everything
        // else is a backend failure (500).
        Err(StorageError::Ltx(e)) => respond_error(stream, 400, &format!("bad ltx file: {e}")),
        Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            respond_error(stream, 400, &format!("short or truncated ltx body: {e}"))
        }
        Err(e) => respond_error(stream, 500, &format!("write: {e}")),
    }
}

/// Receives a PUT body in full into an unlinked local spool file and
/// returns it rewound, with its length. Bounded only by disk — the same
/// stance as the client-side GET spool.
fn spool_put_body(body: &mut dyn Read) -> std::io::Result<(std::fs::File, u64)> {
    let mut spool = super::unlinked_temp_file()?;
    let mut buf = vec![0u8; 64 << 10];
    let mut len: u64 = 0;
    loop {
        match body.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                spool.write_all(&buf[..n])?;
                len += n as u64;
            }
            Err(e) => return Err(e),
        }
    }
    spool.seek(SeekFrom::Start(0))?;
    Ok((spool, len))
}

/// Byte-compares the stored `existing` L0 file against the spooled body
/// (whose size already matched the listing). `Ok(false)` on any
/// difference, including the stored file coming up short.
fn stored_matches_spool(
    bucket: &Bucket,
    existing: &FileInfo,
    spool: &mut std::fs::File,
) -> Result<bool> {
    spool.seek(SeekFrom::Start(0))?;
    let mut stored = bucket.client.open_ltx_file(
        existing.level,
        existing.min_txid,
        existing.max_txid,
        0,
        0,
    )?;
    let mut stored_buf = vec![0u8; 64 << 10];
    let mut spool_buf = vec![0u8; 64 << 10];
    loop {
        let n = stored.read(&mut stored_buf)?;
        if n == 0 {
            // Sizes matched up front, so both are exhausted together.
            return Ok(true);
        }
        let mut filled = 0;
        while filled < n {
            let m = spool.read(&mut spool_buf[filled..n])?;
            if m == 0 {
                return Ok(false); // spool shorter than the stored file
            }
            filled += m;
        }
        if stored_buf[..n] != spool_buf[..n] {
            return Ok(false);
        }
    }
}

/// Reads exactly `remaining` bytes from `inner`; EOF any earlier is an
/// `UnexpectedEof` error rather than a short read.
struct ExactLen<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> Read for ExactLen<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        let want = self.remaining.min(buf.len() as u64) as usize;
        let n = self.inner.read(&mut buf[..want])?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request body shorter than content-length",
            ));
        }
        self.remaining -= n as u64;
        Ok(n)
    }
}

/// Best-effort bounded drain of an unread request body after an error
/// response: closing with unread data makes the kernel send RST, which can
/// destroy the just-written response before the pusher reads it.
fn drain_body(stream: &mut TcpStream) {
    const DRAIN_LIMIT: u64 = 4 << 20;
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let mut buf = [0u8; 8192];
    let mut left = DRAIN_LIMIT;
    while left > 0 {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => left = left.saturating_sub(n as u64),
        }
    }
}

fn parse_level(s: &str) -> Option<u8> {
    // Strict decimal 0..=9, no leading zeros ("00" is not a level).
    if s.len() != 1 {
        return None;
    }
    let level: u8 = s.parse().ok()?;
    (level <= SNAPSHOT_LEVEL).then_some(level)
}

fn write_head(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    extra: &[(&str, &str)],
) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\n{PROTOCOL_HEADER}: {PROTOCOL_VERSION}\r\nserver: liters\r\nconnection: close\r\n"
    );
    for (name, value) in extra {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())
}

fn respond_text(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        _ => "Internal Server Error",
    };
    write_head(
        stream,
        status,
        reason,
        &[
            ("content-type", "text/plain; charset=utf-8"),
            ("content-length", &body.len().to_string()),
        ],
    )?;
    stream.write_all(body.as_bytes())
}

fn respond_error(stream: &mut TcpStream, status: u16, msg: &str) -> std::io::Result<()> {
    respond_text(stream, status, &format!("{msg}\n"))
}

/// `GET /ltx/{level}?seek={txid:016x}&meta=1` — text listing, one file per
/// line: `{min:016x}-{max:016x}.ltx {size} {created_ms|-}`.
fn serve_listing(
    stream: &mut TcpStream,
    bucket: &Bucket,
    level: u8,
    query: &str,
) -> std::io::Result<()> {
    let seek = match query_param(query, "seek") {
        None => Txid(0),
        Some(s) => match Txid::parse(s) {
            Some(t) => t,
            None => return respond_error(stream, 400, "bad seek"),
        },
    };
    let use_metadata = query_param(query, "meta") == Some("1");

    let infos = match bucket.client.ltx_files(level, seek, use_metadata) {
        Ok(infos) => infos,
        Err(e) => return respond_error(stream, 500, &format!("list: {e}")),
    };

    let mut body = String::new();
    for info in infos {
        body.push_str(&listing_line(&info));
    }
    respond_text(stream, 200, &body)
}

/// One `{name} {size} {created_ms|-}\n` line — the listing format, also the
/// body of a successful PUT response.
fn listing_line(info: &FileInfo) -> String {
    let created = info
        .created_at
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| (d.as_millis().min(u64::MAX as u128) as u64).to_string());
    format!("{} {} {}\n", info.filename(), info.size, created.as_deref().unwrap_or("-"))
}

/// `GET /ltx/{level}/{name}?offset=N&size=M` — the file bytes, chunked.
/// The backend reader is opened *before* the status line is written and held
/// for the whole transfer, reproducing the dir backend's FD-held-during-merge
/// safety for restores racing retention.
fn serve_file(
    stream: &mut TcpStream,
    shared: &Shared,
    bucket: &Bucket,
    level: u8,
    min_txid: Txid,
    max_txid: Txid,
    query: &str,
) -> std::io::Result<()> {
    let parse_u64 = |name: &str| -> Option<u64> {
        match query_param(query, name) {
            None => Some(0),
            Some(v) => v.parse().ok(),
        }
    };
    let (Some(offset), Some(size)) = (parse_u64("offset"), parse_u64("size")) else {
        return respond_error(stream, 400, "bad offset/size");
    };

    let mut rd = match bucket.client.open_ltx_file(level, min_txid, max_txid, offset, size) {
        Ok(rd) => rd,
        Err(StorageError::NotFound { .. }) => return respond_error(stream, 404, "no such file"),
        Err(e) => return respond_error(stream, 500, &format!("open: {e}")),
    };

    write_head(stream, 200, "OK", &[
        ("content-type", "application/octet-stream"),
        ("transfer-encoding", "chunked"),
    ])?;
    let mut out = ChunkedWriter::new(BufWriter::with_capacity(64 << 10, &mut *stream));
    let mut buf = vec![0u8; 64 << 10];
    loop {
        if shared.stop.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("server shutting down"));
        }
        match rd.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.chunk(&buf[..n])?,
            // Mid-body backend error: abort the connection (no clean finish)
            // so the client sees a truncated chunked body, not a short file.
            Err(e) => return Err(e),
        }
    }
    out.finish()
}

/// `GET /stream?seek={txid:016x}` — long-lived frame stream of new level-0
/// files. See docs/http-protocol.md for the frame grammar.
fn serve_stream(
    stream: &mut TcpStream,
    shared: &Shared,
    bucket: &Bucket,
    query: &str,
) -> std::io::Result<()> {
    let seek = match query_param(query, "seek").and_then(Txid::parse) {
        Some(t) if !t.is_zero() => t,
        _ => return respond_error(stream, 400, "stream requires seek >= 1"),
    };

    write_head(stream, 200, "OK", &[
        ("content-type", "application/x-liters-ltx-stream"),
        ("transfer-encoding", "chunked"),
        // Reverse proxies must not buffer or transform this response.
        ("cache-control", "no-cache, no-transform"),
        ("x-accel-buffering", "no"),
    ])?;
    let mut out = ChunkedWriter::new(BufWriter::with_capacity(64 << 10, stream.try_clone()?));
    out.chunk(format!("liters-stream {PROTOCOL_VERSION}\n").as_bytes())?;
    out.flush()?;

    // `cursor` is the next TXID the follower wants; `cursor - 1` is its
    // position. Divergence rule (finish_incremental parity): only positive
    // evidence counts — a non-empty bucket whose max is below the follower's
    // position. An empty bucket is a wipe-then-reseed window, not divergence.
    let mut cursor = seek;
    let is_reset = |bucket_max: Txid, cursor: Txid| !bucket_max.is_zero() && bucket_max.0 < cursor.0 - 1;
    let send_reset = |out: &mut ChunkedWriter<BufWriter<TcpStream>>, bucket_max: Txid| {
        out.chunk(format!("reset {bucket_max}\n").as_bytes())
    };

    if let Ok(m) = bucket_max(bucket.client.as_ref()) {
        if is_reset(m, cursor) {
            send_reset(&mut out, m)?;
            return out.finish();
        }
    }

    let mut last_ping = Instant::now();
    loop {
        if shared.stop.load(Ordering::Relaxed) {
            return out.finish();
        }
        let generation = bucket.notify.generation();

        // Full L0 listing each round (L0 is retention-pruned and small):
        // seek-filtered listings would hide multi-TXID files that overlap
        // the cursor, which buckets written by stock litestream contain.
        let files = match bucket.client.ltx_files(0, Txid(0), false) {
            Ok(files) => files,
            Err(_) => return Ok(()), // abort; client resyncs via listings
        };

        let l0_max = files.iter().map(|f| f.max_txid).max().unwrap_or(Txid(0));
        let mut progressed = false;
        let mut gap: Option<Txid> = None;
        for info in files {
            if info.max_txid.0 < cursor.0 {
                continue; // already applied by the follower
            }
            if info.min_txid.0 > cursor.0 {
                gap = Some(info.min_txid);
                break;
            }
            // min <= cursor <= max: contiguous (single-TXID L0s) or
            // overlapping (multi-TXID L0s) — the follower applies either.
            match send_ltx_frame(&mut out, shared, bucket, &info)? {
                true => {
                    cursor = Txid(info.max_txid.0 + 1);
                    progressed = true;
                }
                false => break, // 404 race with retention: re-list
            }
        }

        if let Some(next) = gap {
            out.chunk(format!("gap {next}\n").as_bytes())?;
            return out.finish();
        }
        if progressed {
            last_ping = Instant::now(); // frames are liveness for the peer
            continue;
        }

        // Poll-cadence divergence check, free of extra listings: the newest
        // L0 always survives retention, so a non-empty L0 whose max trails
        // the follower's position is reseed evidence. Confirm bucket-wide
        // before declaring it (poll-mode parity — never wait for a ping).
        if is_reset(l0_max, cursor) {
            if let Ok(m) = bucket_max(bucket.client.as_ref()) {
                if is_reset(m, cursor) {
                    send_reset(&mut out, m)?;
                    return out.finish();
                }
            }
        }

        // Caught up. Wait for a push notification or the poll tick,
        // whichever is sooner; ping (with a divergence check) on cadence.
        let until_ping = shared.opts.ping_interval.saturating_sub(last_ping.elapsed());
        if until_ping.is_zero() {
            let bucket_max = bucket_max(bucket.client.as_ref()).unwrap_or(Txid(0));
            if is_reset(bucket_max, cursor) {
                send_reset(&mut out, bucket_max)?;
                return out.finish();
            }
            out.chunk(format!("ping {bucket_max}\n").as_bytes())?;
            out.flush()?;
            last_ping = Instant::now();
            continue;
        }
        bucket.notify.wait(generation, shared.opts.poll_interval.min(until_ping), &shared.stop);
    }
}

/// Sends one `ltx` frame. `Ok(false)` = the file 404ed between list and open
/// (retention race) — caller re-lists. The frame line's declared size must
/// match the body exactly; a short backend read aborts the connection.
fn send_ltx_frame(
    out: &mut ChunkedWriter<BufWriter<TcpStream>>,
    shared: &Shared,
    bucket: &Bucket,
    info: &FileInfo,
) -> std::io::Result<bool> {
    let rd = match bucket.client.open_ltx_file(info.level, info.min_txid, info.max_txid, 0, 0) {
        Ok(rd) => rd,
        Err(StorageError::NotFound { .. }) => return Ok(false),
        // Backend failure mid-stream: abort the connection; the follower
        // falls back to sync() where the error surfaces properly.
        Err(e) => return Err(std::io::Error::other(e.to_string())),
    };

    out.chunk(
        format!(
            "ltx {} {} {} {}\n",
            info.level, info.min_txid, info.max_txid, info.size
        )
        .as_bytes(),
    )?;

    let mut rd = rd.take(info.size);
    let mut buf = vec![0u8; 64 << 10];
    let mut sent: u64 = 0;
    loop {
        if shared.stop.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("server shutting down"));
        }
        match rd.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                out.chunk(&buf[..n])?;
                sent += n as u64;
            }
            Err(e) => return Err(e),
        }
    }
    if sent != info.size {
        // Listing size and file bytes disagree; the frame is corrupt and
        // unrecoverable in-stream. Abort the connection.
        return Err(std::io::Error::other("ltx file shorter than its listed size"));
    }
    out.flush()?;
    Ok(true)
}

/// Bucket-wide max TXID across all levels, the same evidence
/// finish_incremental uses for divergence detection.
fn bucket_max(client: &dyn ReplicaClient) -> Result<Txid> {
    let mut max = Txid(0);
    for level in 0..=SNAPSHOT_LEVEL {
        for info in client.ltx_files(level, Txid(0), false)? {
            if info.max_txid > max {
                max = info.max_txid;
            }
        }
    }
    Ok(max)
}

// ---------------------------------------------------------------------------
// Notifying tee

/// Wraps a [`ReplicaClient`]; every successful mutation wakes one bucket's
/// `/stream` followers. Reads pass straight through.
struct NotifyingClient {
    inner: Box<dyn ReplicaClient>,
    bucket: Arc<Bucket>,
}

impl ReplicaClient for NotifyingClient {
    fn client_type(&self) -> &'static str {
        self.inner.client_type()
    }

    fn ltx_files(&self, level: u8, seek: Txid, use_metadata: bool) -> Result<Vec<FileInfo>> {
        self.inner.ltx_files(level, seek, use_metadata)
    }

    fn open_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        offset: u64,
        size: u64,
    ) -> Result<Box<dyn Read + Send>> {
        self.inner.open_ltx_file(level, min_txid, max_txid, offset, size)
    }

    fn write_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        rd: &mut dyn Read,
    ) -> Result<FileInfo> {
        let info = self.inner.write_ltx_file(level, min_txid, max_txid, rd)?;
        self.bucket.notify.notify();
        Ok(info)
    }

    fn delete_ltx_files(&self, infos: &[FileInfo]) -> Result<()> {
        self.inner.delete_ltx_files(infos)?;
        self.bucket.notify.notify();
        Ok(())
    }

    fn delete_all(&self) -> Result<()> {
        self.inner.delete_all()?;
        self.bucket.notify.notify();
        Ok(())
    }

    fn open_ltx_stream(&self, seek: Txid) -> Result<Option<Box<dyn crate::LtxStream>>> {
        self.inner.open_ltx_stream(seek)
    }

    fn set_cancel(&self, token: CancelToken) {
        // The tee must stay transparent: cancellation reaches the wrapped
        // backend.
        self.inner.set_cancel(token)
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_base;

    #[test]
    fn base_path_normalization() {
        // Root mount: several spellings, all empty.
        for spec in [None, Some(""), Some("/"), Some("///")] {
            assert!(normalize_base(spec).is_empty(), "{spec:?}");
        }
        // Leading/trailing slashes optional, interior `//` collapsed.
        for spec in ["/db", "db", "/db/", "db/"] {
            assert_eq!(normalize_base(Some(spec)), ["db"], "{spec:?}");
        }
        for spec in ["/a/b/c", "a/b/c/", "/a//b/c//"] {
            assert_eq!(normalize_base(Some(spec)), ["a", "b", "c"], "{spec:?}");
        }
    }
}
