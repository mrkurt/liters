//! Batteries-included HTTP server: a thin `TcpListener` driver over a single
//! [`Mount`](super::Mount). It owns the socket lifecycle — accept loop,
//! thread-per-connection, timeouts, chunked framing, shutdown — and delegates
//! all protocol decisions to the mount, which is transport-agnostic. Protocol
//! spec: docs/http-protocol.md (the liters HTTP replication protocol — ours,
//! not litestream's).
//!
//! One server serves one database. To serve several from one listener, or to
//! serve liters alongside unrelated routes, embed [`Mount`](super::Mount) in
//! your own Rust HTTP server instead (see the [`mount`](super::mount) module):
//! its router owns paths and ports, and it calls [`Mount::handle`] per request.
//!
//! The server reads from any [`ReplicaClient`]; the intended source is a
//! [`DirReplicaClient`](crate::DirReplicaClient) over the same bucket the local
//! `Writer` pushes to. Wrap the writer's client with
//! [`HttpServer::notifying_client`] so pushes wake `/stream` followers
//! immediately; without the tee (e.g. an external process writes the bucket),
//! streamers still pick changes up by re-listing every `poll_interval`.
//!
//! With `writable: true` the roles reverse: the server *receives* replication.
//! A remote `Writer` whose destination is an
//! [`HttpReplicaClient`](super::HttpReplicaClient) pushes its LTX files here
//! (`PUT`/`DELETE`), useful when the writer can dial out but cannot be reached
//! (NAT, mobile). Accepted writes wake `/stream` followers, so a writable
//! server is simultaneously a relay.
//!
//! The whole server can be mounted under a URL path prefix
//! ([`HttpServerOptions::base_path`], e.g. `/db`) so it shares an origin with
//! unrelated apps behind a path-routing reverse proxy; the prefix is stripped
//! before the request reaches the mount.
//!
//! Threading: one accept thread plus one thread per connection (peers are few
//! — they are replicas, not browsers). All are joined by
//! [`HttpServer::shutdown`], which also closes peer sockets so threads blocked
//! in writes exit immediately.

use std::io::{BufWriter, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::{CancelToken, ReplicaClient, Result};

use super::mount::{version_line, Body, Mount, MountOptions, Request, Response};
use super::wire::{
    header, read_head, ChunkedReader, ChunkedWriter, PROTOCOL_HEADER, PROTOCOL_VERSION,
};

/// How long a connection may take to send its request head.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Per-read timeout while receiving a pushed file body. More lenient than the
/// head timeout: mobile uplinks stall; a PUT that stops flowing for 30s is
/// aborted (PUTs are idempotent — the writer re-pushes).
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
    /// `DELETE /all`, written through to the backing [`ReplicaClient`]. This
    /// is how a remote `Writer` pushes to this server. Every accepted write
    /// wakes local `/stream` followers, so a writable server is also a relay.
    /// Default `false` (read-only). Set [`HttpServerOptions::auth_token`] or
    /// keep writable servers on private interfaces: an unauthenticated
    /// writable server accepts `DELETE /all` from anyone who can reach it.
    pub writable: bool,
    /// When set, every route except the `GET /` health check requires
    /// `authorization: Bearer <token>` (case-insensitive scheme, exact token
    /// match); anything else is `401`. `None` (the default) disables auth.
    pub auth_token: Option<String>,
    /// How long a writer lease is held after the owner's last accepted write
    /// before another writer id may claim the bucket without a takeover header
    /// (docs/http-protocol.md "Fencing"). Leases are in-memory and reset on
    /// restart.
    pub lease_ttl: Duration,
    /// Mount every endpoint under a URL path prefix so the server can share an
    /// origin with unrelated apps behind a path-routing reverse proxy. With
    /// `Some("/db")` the endpoints move under `/db` (`/db/ltx/...`,
    /// `/db/stream`) and any request whose path is not under the prefix is
    /// `404`. Leading/trailing slashes are optional and interior `//` is
    /// collapsed; `None`, `""`, and `"/"` all mount at the root —
    /// byte-identical to a server without this option. The bare-root `GET /`
    /// health check answers regardless of the prefix. Followers address a
    /// mounted server through the client URL's base path
    /// (`http://host:port/db`).
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

impl HttpServerOptions {
    fn mount_options(&self) -> MountOptions {
        MountOptions {
            writable: self.writable,
            auth_token: self.auth_token.clone(),
            lease_ttl: self.lease_ttl,
            poll_interval: self.poll_interval,
            ping_interval: self.ping_interval,
        }
    }
}

struct Shared {
    mount: Arc<Mount>,
    /// The mount prefix ([`HttpServerOptions::base_path`]) as its non-empty
    /// path segments; empty for a root-mounted server. Stripped from every
    /// request path before it reaches the mount.
    base_segments: Vec<String>,
    /// Cancels in-flight `/stream`s and breaks the accept loop on shutdown.
    stop: CancelToken,
}

struct Conn {
    stream: TcpStream,
    handle: Option<JoinHandle<()>>,
    done: Arc<std::sync::atomic::AtomicBool>,
}

/// Serves one bucket over HTTP so other liters instances can restore from it
/// and follow it. A thin driver over [`Mount`](super::Mount); see the module
/// docs for wiring and docs/http-protocol.md for the protocol.
pub struct HttpServer {
    shared: Arc<Shared>,
    local_addr: SocketAddr,
    accept: Option<JoinHandle<()>>,
    conns: Arc<Mutex<Vec<Conn>>>,
}

impl HttpServer {
    /// Binds and starts serving `client`. Use port 0 to let the OS pick (see
    /// [`HttpServer::local_addr`]). No TLS in protocol v1: bind loopback or a
    /// private interface, front with a reverse proxy, or at least set
    /// [`HttpServerOptions::auth_token`].
    pub fn bind(
        addr: impl ToSocketAddrs,
        client: Arc<dyn ReplicaClient>,
        opts: HttpServerOptions,
    ) -> Result<HttpServer> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;

        let base_segments = normalize_base(opts.base_path.as_deref());
        let mount = Arc::new(Mount::new(client, opts.mount_options()));
        let shared = Arc::new(Shared { mount, base_segments, stop: CancelToken::new() });
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

    /// Wraps a [`ReplicaClient`] so every successful mutation wakes this
    /// server's `/stream` followers. Hand the wrapped client to the local
    /// `Writer` and pushes are delivered to followers with no poll latency.
    pub fn notifying_client(&self, inner: Box<dyn ReplicaClient>) -> Box<dyn ReplicaClient> {
        self.shared.mount.notifying_client(inner)
    }

    /// Stops accepting, closes every peer socket (unblocking any thread stuck
    /// in a write to a stalled peer), and joins all threads. Idempotent; also
    /// runs on drop.
    pub fn shutdown(&mut self) {
        if !self.shared.stop.is_cancelled() {
            self.shared.stop.cancel();
            // Wake the mount's `/stream` waiters so a parked handler doesn't
            // sleep out its whole poll interval before the join below.
            self.shared.mount.wake();
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
    while !shared.stop.is_cancelled() {
        match listener.accept() {
            Ok((stream, _peer)) => {
                if configure_socket(&stream).is_err() {
                    continue;
                }
                // The registry clone must exist BEFORE the handler thread: an
                // unregistered handler would be invisible to shutdown()
                // (unclosable, unjoinable). No clone -> drop the connection;
                // the peer retries.
                let Ok(registered) = stream.try_clone() else { continue };
                let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
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
            // Non-blocking listener: poll so shutdown is never stuck in accept
            // regardless of bind interface. Other errors (EMFILE...) get the
            // same backoff.
            Err(_) => std::thread::sleep(ACCEPT_TICK),
        }
        reap(&conns);
    }
}

struct DoneGuard<'a>(&'a std::sync::atomic::AtomicBool);

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
// Request handling: parse the head, strip the mount prefix, adapt the socket
// into a `Request`, delegate to the mount, and write the `Response` back.

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
    // All error paths write-and-return; write failures mean the peer is gone,
    // which is fine.
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
    let should_drain = method == "PUT" || has_body(headers);

    // Bare-root health check (liveness probes carry no secrets), answered
    // pre-auth and regardless of any configured mount prefix.
    if let ("GET", [""]) = (method, segments.as_slice()) {
        return write_response(stream, Response::text(200, version_line()), should_drain, &shared.stop);
    }

    // Strip the configured mount prefix (empty for a root-mounted server). A
    // path not under the mount belongs to some other app sharing this origin
    // behind a path-routing proxy: 404 it. (Auth lives in the mount, so the
    // prefix layout is not probeable without the token where one is set.)
    let base = &shared.base_segments;
    let under_mount = segments.len() >= base.len()
        && base.iter().zip(&segments).all(|(b, s)| b.as_str() == *s);
    if !under_mount {
        return write_response(stream, Response::error(404, "not found"), should_drain, &shared.stop);
    }
    let mount_path = segments[base.len()..].join("/");

    // Framing: PUT bodies must declare a length (content-length or chunked).
    let chunked =
        header(headers, "transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
    let content_length: Option<u64> =
        header(headers, "content-length").and_then(|v| v.parse().ok());
    if method == "PUT" && !chunked && content_length.is_none() {
        return write_response(
            stream,
            Response::error(411, "length required (content-length or chunked)"),
            true,
            &shared.stop,
        );
    }

    // Decode the request body (for PUT) and hand the mount a parsed request.
    // The body borrow is released before the response is written.
    let resp = {
        let mut empty = std::io::empty();
        let mut body: Box<dyn Read>;
        let body_ref: &mut dyn Read = if method == "PUT" {
            let _ = stream.set_read_timeout(Some(PUT_BODY_READ_TIMEOUT));
            body = if chunked {
                Box::new(ChunkedReader::new(&mut *stream))
            } else {
                // ExactLen, not take(): a clean FIN short of content-length must
                // be an error, never a shorter file.
                Box::new(ExactLen { inner: &mut *stream, remaining: content_length.unwrap() })
            };
            &mut *body
        } else {
            &mut empty
        };
        let req = Request {
            method,
            path: &mount_path,
            query,
            headers,
            body: body_ref,
            cancel: shared.stop.clone(),
        };
        shared.mount.handle(req)
    };

    write_response(stream, resp, should_drain, &shared.stop)
}

/// Whether the request head declares a body (chunked or a non-zero
/// content-length) — error responses must drain it (bounded) before close.
fn has_body(headers: &[(String, String)]) -> bool {
    if header(headers, "transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        return true;
    }
    header(headers, "content-length")
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|n| n > 0)
}

/// Splits a configured mount prefix ([`HttpServerOptions::base_path`]) into its
/// non-empty path segments. `None`, `""`, `"/"`, and `"///"` all yield an empty
/// vec (root mount); leading/trailing slashes and interior `//` are tolerated.
fn normalize_base(base: Option<&str>) -> Vec<String> {
    base.unwrap_or("")
        .split('/')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Writes a [`Response`] to the socket: status line, the protocol/transport
/// headers, the mount's semantic headers, then the body (content-length for
/// bytes, chunked for readers/streams). After the response, drains any unread
/// request body (bounded) so a close-with-unread-bytes RST can't destroy the
/// response the pusher is about to read.
fn write_response(
    stream: &mut TcpStream,
    resp: Response,
    drain: bool,
    stop: &CancelToken,
) -> std::io::Result<()> {
    let result = write_response_body(stream, resp, stop);
    if drain {
        drain_body(stream);
    }
    result
}

fn write_response_body(
    stream: &mut TcpStream,
    resp: Response,
    stop: &CancelToken,
) -> std::io::Result<()> {
    let Response { status, headers, body } = resp;
    let mut head = format!(
        "HTTP/1.1 {status} {}\r\n{PROTOCOL_HEADER}: {PROTOCOL_VERSION}\r\nserver: liters\r\nconnection: close\r\n",
        reason(status)
    );
    for (name, value) in &headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    match body {
        Body::Bytes(b) => {
            head.push_str(&format!("content-length: {}\r\n\r\n", b.len()));
            stream.write_all(head.as_bytes())?;
            stream.write_all(&b)
        }
        Body::Reader(mut rd) => {
            head.push_str("transfer-encoding: chunked\r\n\r\n");
            stream.write_all(head.as_bytes())?;
            let mut out = ChunkedWriter::new(BufWriter::with_capacity(64 << 10, &mut *stream));
            let mut buf = vec![0u8; 64 << 10];
            loop {
                if stop.is_cancelled() {
                    return Err(std::io::Error::other("server shutting down"));
                }
                match rd.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => out.chunk(&buf[..n])?,
                    // Mid-body backend error: abort (drop `out` without finish)
                    // so the client sees a truncated body, not a short file.
                    Err(e) => return Err(e),
                }
            }
            out.finish()
        }
        Body::Stream(s) => {
            head.push_str("transfer-encoding: chunked\r\n\r\n");
            stream.write_all(head.as_bytes())?;
            let mut out = ChunkedWriter::new(BufWriter::with_capacity(64 << 10, &mut *stream));
            // `write_to` returns Err to abort — drop `out` unfinished so the
            // follower sees a truncated body and resyncs.
            s.write_to(&mut out)?;
            out.finish()
        }
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        _ => "Internal Server Error",
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

/// Best-effort bounded drain of an unread request body after a response:
/// closing with unread data makes the kernel send RST, which can destroy the
/// just-written response before the pusher reads it.
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
