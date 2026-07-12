//! Embeddable HTTP server that serves one database's bucket to other liters
//! instances: listings, LTX file bodies, and a long-lived `/stream` endpoint
//! that pushes new level-0 files to followers as the writer produces them.
//! Wire protocol: docs/http-protocol.md.
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
//! blocked in writes exit immediately.

use std::io::{BufWriter, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, UNIX_EPOCH};

use ltx::{parse_filename, FileInfo, Txid};

use crate::{ReplicaClient, Result, StorageError, SNAPSHOT_LEVEL};

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
    /// relay. Default `false` (read-only). There is no auth in protocol v1
    /// — only enable on private interfaces or behind an authenticating
    /// reverse proxy: a writable server accepts `DELETE /all` from anyone
    /// who can reach it.
    pub writable: bool,
}

impl Default for HttpServerOptions {
    fn default() -> Self {
        HttpServerOptions {
            poll_interval: Duration::from_secs(1),
            ping_interval: Duration::from_secs(15),
            writable: false,
        }
    }
}

struct Shared {
    client: Arc<dyn ReplicaClient>,
    opts: HttpServerOptions,
    stop: AtomicBool,
    /// Bumped by the notifying tee on every bucket mutation; `/stream`
    /// handlers wait on it (with a poll_interval timeout) when caught up.
    generation: Mutex<u64>,
    cond: Condvar,
}

impl Shared {
    fn notify(&self) {
        let mut g = self.generation.lock().unwrap();
        *g = g.wrapping_add(1);
        drop(g);
        self.cond.notify_all();
    }

    /// Waits until the generation moves past `seen`, the server stops, or
    /// `timeout` elapses.
    fn wait(&self, seen: u64, timeout: Duration) {
        let g = self.generation.lock().unwrap();
        let _unused = self
            .cond
            .wait_timeout_while(g, timeout, |g| *g == seen && !self.stop.load(Ordering::Relaxed))
            .unwrap();
    }

    fn generation(&self) -> u64 {
        *self.generation.lock().unwrap()
    }
}

struct Conn {
    stream: TcpStream,
    handle: Option<JoinHandle<()>>,
    done: Arc<AtomicBool>,
}

/// Serves a bucket over HTTP so other liters instances can restore from it
/// and follow it. See the module docs for wiring and docs/http-protocol.md
/// for the protocol.
pub struct HttpServer {
    shared: Arc<Shared>,
    local_addr: SocketAddr,
    accept: Option<JoinHandle<()>>,
    conns: Arc<Mutex<Vec<Conn>>>,
}

impl HttpServer {
    /// Binds and starts serving immediately. Use port 0 to let the OS pick
    /// (see [`HttpServer::local_addr`]). No TLS/auth in protocol v1: bind
    /// loopback or a private interface, or front with a reverse proxy.
    pub fn bind(
        addr: impl ToSocketAddrs,
        client: Arc<dyn ReplicaClient>,
        opts: HttpServerOptions,
    ) -> Result<HttpServer> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;

        let shared = Arc::new(Shared {
            client,
            opts,
            stop: AtomicBool::new(false),
            generation: Mutex::new(0),
            cond: Condvar::new(),
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

    /// Wraps a [`ReplicaClient`] so every successful mutation wakes this
    /// server's `/stream` followers. Hand the wrapped client to the local
    /// `Writer` and pushes are delivered to followers with no poll latency.
    pub fn notifying_client(&self, inner: Box<dyn ReplicaClient>) -> Box<dyn ReplicaClient> {
        Box::new(NotifyingClient { inner, shared: Arc::clone(&self.shared) })
    }

    /// Stops accepting, closes every peer socket (unblocking any thread
    /// stuck in a write to a stalled peer), and joins all threads.
    /// Idempotent; also runs on drop.
    pub fn shutdown(&mut self) {
        if !self.shared.stop.swap(true, Ordering::SeqCst) {
            self.shared.notify();
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
    match (method, segments.as_slice()) {
        ("GET", [""]) => {
            respond_text(stream, 200, &format!("liters {}\n", env!("CARGO_PKG_VERSION")))
        }
        ("GET", ["ltx", level]) => {
            let Some(level) = parse_level(level) else {
                return respond_error(stream, 404, "no such level");
            };
            serve_listing(stream, shared, level, query)
        }
        ("GET", ["ltx", level, name]) => {
            let Some(level) = parse_level(level) else {
                return respond_error(stream, 404, "no such level");
            };
            let Some((min_txid, max_txid)) = parse_filename(name) else {
                return respond_error(stream, 404, "no such file");
            };
            serve_file(stream, shared, level, min_txid, max_txid, query)
        }
        ("GET", ["stream"]) => serve_stream(stream, shared, query),
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
            accept_file(stream, shared, level, min_txid, max_txid, headers)
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
            let info = FileInfo { level, min_txid, max_txid, ..Default::default() };
            match shared.client.delete_ltx_files(&[info]) {
                Ok(()) => {
                    shared.notify();
                    respond_text(stream, 200, "deleted\n")
                }
                Err(e) => respond_error(stream, 500, &format!("delete: {e}")),
            }
        }
        ("DELETE", ["all"]) => {
            if !shared.opts.writable {
                return respond_error(stream, 403, "server is read-only (writable: false)");
            }
            match shared.client.delete_all() {
                Ok(()) => {
                    shared.notify();
                    respond_text(stream, 200, "deleted\n")
                }
                Err(e) => respond_error(stream, 500, &format!("delete all: {e}")),
            }
        }
        ("GET", _) | ("PUT", _) | ("DELETE", _) => respond_error(stream, 404, "not found"),
        _ => respond_error(stream, 405, "method not allowed"),
    }
}

/// `PUT /ltx/{level}/{name}` — accepts one pushed LTX file, streamed
/// straight into the backing client (which owns atomicity: tmp + rename on
/// the dir backend, so a connection cut mid-body leaves nothing behind).
/// Success wakes `/stream` followers, making a writable server a relay.
/// Responds with the file's listing line so the pusher gets an authoritative
/// `FileInfo` back.
fn accept_file(
    stream: &mut TcpStream,
    shared: &Shared,
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

    let result = {
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
        shared.client.write_ltx_file(level, min_txid, max_txid, &mut *body)
    };

    match result {
        Ok(info) => {
            shared.notify();
            respond_text(stream, 200, &listing_line(&info))
        }
        // A body that does not start with a valid 100-byte LTX header, or
        // that ends short of its declared length, is the pusher's problem
        // (400); everything else is a backend failure (500). The error paths
        // drain a bounded amount of unread body so the response is delivered
        // on a clean FIN instead of racing an RST.
        Err(StorageError::Ltx(e)) => {
            let r = respond_error(stream, 400, &format!("bad ltx file: {e}"));
            drain_body(stream);
            r
        }
        Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            let r = respond_error(stream, 400, &format!("short or truncated ltx body: {e}"));
            drain_body(stream);
            r
        }
        Err(e) => {
            let r = respond_error(stream, 500, &format!("write: {e}"));
            drain_body(stream);
            r
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
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
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
    shared: &Shared,
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

    let infos = match shared.client.ltx_files(level, seek, use_metadata) {
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

    let mut rd = match shared.client.open_ltx_file(level, min_txid, max_txid, offset, size) {
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
fn serve_stream(stream: &mut TcpStream, shared: &Shared, query: &str) -> std::io::Result<()> {
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

    if let Ok(m) = bucket_max(shared.client.as_ref()) {
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
        let generation = shared.generation();

        // Full L0 listing each round (L0 is retention-pruned and small):
        // seek-filtered listings would hide multi-TXID files that overlap
        // the cursor, which buckets written by stock litestream contain.
        let files = match shared.client.ltx_files(0, Txid(0), false) {
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
            match send_ltx_frame(&mut out, shared, &info)? {
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
            if let Ok(m) = bucket_max(shared.client.as_ref()) {
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
            let bucket_max = bucket_max(shared.client.as_ref()).unwrap_or(Txid(0));
            if is_reset(bucket_max, cursor) {
                send_reset(&mut out, bucket_max)?;
                return out.finish();
            }
            out.chunk(format!("ping {bucket_max}\n").as_bytes())?;
            out.flush()?;
            last_ping = Instant::now();
            continue;
        }
        shared.wait(generation, shared.opts.poll_interval.min(until_ping));
    }
}

/// Sends one `ltx` frame. `Ok(false)` = the file 404ed between list and open
/// (retention race) — caller re-lists. The frame line's declared size must
/// match the body exactly; a short backend read aborts the connection.
fn send_ltx_frame(
    out: &mut ChunkedWriter<BufWriter<TcpStream>>,
    shared: &Shared,
    info: &FileInfo,
) -> std::io::Result<bool> {
    let rd = match shared.client.open_ltx_file(info.level, info.min_txid, info.max_txid, 0, 0) {
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

/// Wraps a [`ReplicaClient`]; every successful mutation wakes the paired
/// server's `/stream` followers. Reads pass straight through.
struct NotifyingClient {
    inner: Box<dyn ReplicaClient>,
    shared: Arc<Shared>,
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
        self.shared.notify();
        Ok(info)
    }

    fn delete_ltx_files(&self, infos: &[FileInfo]) -> Result<()> {
        self.inner.delete_ltx_files(infos)?;
        self.shared.notify();
        Ok(())
    }

    fn delete_all(&self) -> Result<()> {
        self.inner.delete_all()?;
        self.shared.notify();
        Ok(())
    }

    fn open_ltx_stream(&self, seek: Txid) -> Result<Option<Box<dyn crate::LtxStream>>> {
        self.inner.open_ltx_stream(seek)
    }
}
