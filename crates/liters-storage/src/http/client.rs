//! HTTP replica client for a bucket served by a liters [`HttpServer`] (see
//! docs/http-protocol.md for the wire protocol). Reads always work; writes
//! (`write_ltx_file`/deletes — i.e. using this client as a `Writer`
//! destination to *push* replication to a listening liters) require the
//! server to run with `writable: true`, otherwise they fail with a clear
//! read-only error. Every request is its own `Connection: close` socket, so
//! the restore path's N concurrently open plan files are N sockets.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use ltx::{format_filename, parse_filename, FileInfo, Txid};

use crate::{CancelToken, LtxStream, ReplicaClient, Result, StorageError, StreamEvent};

use super::wire::{
    header, is_timeout, read_head, ChunkedReader, LineBuf, PROTOCOL_HEADER, PROTOCOL_VERSION,
};

/// (status, headers, socket positioned at the body).
type Response = (u16, Vec<(String, String)>, TcpStream);

/// Socket read timeout while a stream is idle; each expiry surfaces as one
/// [`StreamEvent::Idle`] tick so callers can check their stop flag.
const STREAM_TICK: Duration = Duration::from_secs(1);
/// Socket write timeout: each expiry is one cancellation check inside
/// [`write_full`], which owns the cumulative `io_timeout` stall budget.
const WRITE_TICK: Duration = Duration::from_secs(2);

/// Options for [`HttpReplicaClient`]. The defaults reproduce the v1
/// constants, so [`HttpReplicaClient::new`] behaves exactly as before these
/// options existed.
#[derive(Clone, Debug)]
pub struct HttpClientOptions {
    /// Sent as `authorization: Bearer <token>` on every request. Must be
    /// visible ASCII (no whitespace or control bytes) — validated at
    /// construction.
    pub auth_token: Option<String>,
    /// Sent as `x-liters-writer-id` on write requests (PUT/DELETE): this
    /// writer's identity for server-side fencing
    /// (docs/http-protocol.md "Fencing"). Same charset rule.
    pub writer_id: Option<String>,
    /// Sent as `x-liters-writer-takeover: 1` on write requests: claim the
    /// bucket even if another writer id holds the lease. Default `false`.
    pub takeover: bool,
    /// TCP connect timeout, per resolved address. Default 10s.
    pub connect_timeout: Duration,
    /// Socket read timeout, and the cumulative zero-progress budget for
    /// socket writes (which internally tick every 2s so cancellation stays
    /// prompt). Default 30s.
    pub io_timeout: Duration,
    /// A `/stream` that has produced no bytes at all (not even a ping) for
    /// this long is declared dead. Must comfortably exceed the server's
    /// ping interval. Default 45s.
    pub stream_deadman: Duration,
}

impl Default for HttpClientOptions {
    fn default() -> Self {
        HttpClientOptions {
            auth_token: None,
            writer_id: None,
            takeover: false,
            connect_timeout: Duration::from_secs(10),
            io_timeout: Duration::from_secs(30),
            stream_deadman: Duration::from_secs(45),
        }
    }
}

impl HttpClientOptions {
    /// Token and writer id are interpolated into request heads verbatim, so
    /// anything outside visible ASCII (0x21..=0x7e) — notably CR/LF — could
    /// split headers and is rejected up front.
    fn validate(&self) -> Result<()> {
        for (name, value) in [("auth_token", &self.auth_token), ("writer_id", &self.writer_id)] {
            if let Some(v) = value {
                if v.is_empty() || !v.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
                    return Err(StorageError::Other(format!(
                        "{name} must be non-empty visible ASCII \
                         (no spaces, CR/LF, or control bytes)"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Blocking client for a bucket served over HTTP by another liters instance.
///
/// `url` is `http://host:port[/base]` — https is not supported in v1; put a
/// reverse proxy in front for TLS (and disable response buffering for
/// `/stream`, see docs/http-protocol.md). A multi-DB server's buckets are
/// addressed through the base path: `http://host:port/db/{name}`.
pub struct HttpReplicaClient {
    host: String,
    port: u16,
    /// Path prefix, `""` or `/prefix` (no trailing slash).
    base: String,
    opts: HttpClientOptions,
    /// Current cancellation token ([`ReplicaClient::set_cancel`]). Blocking
    /// loops — uploads, body reads, stream ticks — observe the *current*
    /// token through this shared cell, so a token installed before or
    /// during an operation interrupts it.
    cancel: Arc<Mutex<CancelToken>>,
}

impl HttpReplicaClient {
    pub fn new(url: impl AsRef<str>) -> Result<HttpReplicaClient> {
        Self::with_options(url, HttpClientOptions::default())
    }

    pub fn with_options(
        url: impl AsRef<str>,
        options: HttpClientOptions,
    ) -> Result<HttpReplicaClient> {
        options.validate()?;
        let url = url.as_ref();
        let rest = if let Some(rest) = url.strip_prefix("http://") {
            rest
        } else if url.starts_with("https://") {
            return Err(StorageError::Other(
                "https is not supported; terminate TLS with a reverse proxy and use http://"
                    .into(),
            ));
        } else {
            return Err(StorageError::Other(format!("invalid liters url {url:?}: expected http://host:port[/path]")));
        };

        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], rest[i..].trim_end_matches('/')),
            None => (rest, ""),
        };
        if authority.contains('@') {
            return Err(StorageError::Other("userinfo in url is not supported".into()));
        }

        // host[:port], with [v6]:port bracket support.
        let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
            let (host, rest) = v6
                .split_once(']')
                .ok_or_else(|| StorageError::Other(format!("bad ipv6 authority {authority:?}")))?;
            let port = match rest {
                "" => 80,
                rest => rest
                    .strip_prefix(':')
                    .and_then(|p| p.parse::<u16>().ok())
                    .ok_or_else(|| {
                        StorageError::Other(format!("bad port in {authority:?}"))
                    })?,
            };
            (host.to_string(), port)
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (
                    h.to_string(),
                    p.parse::<u16>()
                        .map_err(|_| StorageError::Other(format!("bad port in {authority:?}")))?,
                ),
                None => (authority.to_string(), 80),
            }
        };
        if host.is_empty() {
            return Err(StorageError::Other(format!("missing host in url {url:?}")));
        }

        Ok(HttpReplicaClient {
            host,
            port,
            base: path.to_string(),
            opts: options,
            cancel: Arc::default(),
        })
    }

    fn check_cancel(&self) -> Result<()> {
        check_token(&self.cancel)
    }

    fn connect(&self) -> Result<TcpStream> {
        self.check_cancel()?;
        let addrs: Vec<_> = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|e| {
                StorageError::Unavailable(format!("resolve {}:{}: {e}", self.host, self.port))
            })?
            .collect();
        self.try_addrs(addrs)
    }

    /// Attempts each resolved address in turn. The token is checked before
    /// every attempt: a multi-address host (dual-stack, multi-homed) whose
    /// SYNs are all blackholed must not multiply worst-case cancellation
    /// latency to `addrs.len() x connect_timeout` — the bound stays a
    /// single `connect_timeout`.
    fn try_addrs(&self, addrs: Vec<SocketAddr>) -> Result<TcpStream> {
        let mut last_err = None;
        for addr in addrs {
            self.check_cancel()?;
            match TcpStream::connect_timeout(&addr, self.opts.connect_timeout) {
                Ok(s) => {
                    // A cancel during the connect itself resolves within
                    // connect_timeout; that granularity is accepted.
                    self.check_cancel()?;
                    s.set_nodelay(true)?;
                    s.set_read_timeout(Some(self.opts.io_timeout))?;
                    s.set_write_timeout(Some(self.opts.io_timeout.min(WRITE_TICK)))?;
                    return Ok(s);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(match last_err {
            Some(e) => {
                StorageError::Unavailable(format!("connect {}:{}: {e}", self.host, self.port))
            }
            None => {
                StorageError::Unavailable(format!("no addresses for {}:{}", self.host, self.port))
            }
        })
    }

    /// Optional request headers: `authorization` on every request, the
    /// fencing headers (`x-liters-writer-id`/`x-liters-writer-takeover`) on
    /// write ops only. Values were validated at construction, so straight
    /// interpolation cannot split headers.
    fn extra_headers(&self, write_op: bool) -> String {
        let mut h = String::new();
        if let Some(token) = &self.opts.auth_token {
            h.push_str("authorization: Bearer ");
            h.push_str(token);
            h.push_str("\r\n");
        }
        if write_op {
            if let Some(id) = &self.opts.writer_id {
                h.push_str("x-liters-writer-id: ");
                h.push_str(id);
                h.push_str("\r\n");
            }
            if self.opts.takeover {
                h.push_str("x-liters-writer-takeover: 1\r\n");
            }
        }
        h
    }

    /// One bodyless request (GET/DELETE): connect, send, parse the response
    /// head, and validate the liters protocol header — every response must
    /// carry it, so proxies and foreign servers fail loudly rather than
    /// being misparsed.
    fn request(&self, method: &str, target: &str) -> Result<Response> {
        let stream = self.connect()?;
        let head = format!(
            "{method} {target} HTTP/1.1\r\nhost: {}:{}\r\n{}connection: close\r\n\r\n",
            self.host,
            self.port,
            self.extra_headers(method != "GET"),
        );
        write_full(&stream, head.as_bytes(), &self.cancel, self.opts.io_timeout)?;
        self.read_response(target, stream)
    }

    fn get(&self, target: &str) -> Result<Response> {
        self.request("GET", target)
    }

    /// One PUT streaming `body` as a chunked request body (its length is
    /// unknown — it comes straight off the writer's local file or the
    /// compactor's pipe). The chunk framing is inlined rather than using
    /// `ChunkedWriter`: retried writes are only sound around a single raw
    /// `write` call whose consumed count is tracked (see [`write_full`]),
    /// and the token must be checked per chunk.
    fn put(&self, target: &str, body: &mut dyn Read) -> Result<Response> {
        let stream = self.connect()?;
        let head = format!(
            "PUT {target} HTTP/1.1\r\nhost: {}:{}\r\n{}transfer-encoding: chunked\r\nconnection: close\r\n\r\n",
            self.host,
            self.port,
            self.extra_headers(true),
        );
        let upload = (|| -> Result<()> {
            write_full(&stream, head.as_bytes(), &self.cancel, self.opts.io_timeout)?;
            let mut buf = vec![0u8; 64 << 10];
            loop {
                self.check_cancel()?;
                let n = match body.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    // The body source is local (writer's file, compactor
                    // spool): its failures stay Io, not Unavailable.
                    Err(e) => return Err(e.into()),
                };
                write_full(&stream, format!("{n:x}\r\n").as_bytes(), &self.cancel, self.opts.io_timeout)?;
                write_full(&stream, &buf[..n], &self.cancel, self.opts.io_timeout)?;
                write_full(&stream, b"\r\n", &self.cancel, self.opts.io_timeout)?;
            }
            write_full(&stream, b"0\r\n\r\n", &self.cancel, self.opts.io_timeout)?;
            Ok(())
        })();
        if let Err(upload_err) = upload {
            // Cancellation must return promptly — never wait out a
            // response read.
            if matches!(upload_err, StorageError::Cancelled) {
                return Err(upload_err);
            }
            // The server may have rejected early (401/403/409, 400 bad
            // file) and stopped reading; its response usually still arrived.
            // Prefer that diagnostic over a raw broken-pipe error.
            if let Ok(resp) = self.read_response(target, stream) {
                return Ok(resp);
            }
            return Err(upload_err);
        }
        self.read_response(target, stream)
    }

    fn read_response(&self, target: &str, mut stream: TcpStream) -> Result<Response> {
        let (status_line, headers) = match read_head(&mut stream) {
            Ok(head) => head,
            Err(e) => {
                // A cancel that lands during a blocked head read surfaces
                // as the read's timeout error; report Cancelled, not
                // Unavailable.
                self.check_cancel()?;
                return Err(transport_err(&format!("{target}: read response"), e));
            }
        };
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| StorageError::Other(format!("bad status line {status_line:?}")))?;

        match header(&headers, PROTOCOL_HEADER) {
            Some(PROTOCOL_VERSION) => {}
            Some(v) => {
                return Err(StorageError::Other(format!(
                    "server speaks liters protocol {v:?}, this client speaks {PROTOCOL_VERSION}"
                )))
            }
            None => {
                return Err(StorageError::Other(format!(
                    "{}:{}{target} is not a liters server (status {status}, no {PROTOCOL_HEADER} header)",
                    self.host, self.port
                )))
            }
        }
        Ok((status, headers, stream))
    }

    /// Short error-body excerpt for diagnostics (e.g. the server's
    /// "read-only" message).
    fn error_detail(headers: &[(String, String)], stream: TcpStream) -> String {
        let mut buf = String::new();
        let _ = Self::body_reader(headers, stream).take(512).read_to_string(&mut buf);
        let trimmed = buf.trim();
        if trimmed.is_empty() {
            String::new()
        } else {
            format!(": {trimmed}")
        }
    }

    /// Maps a non-success response to the error taxonomy
    /// (docs/http-protocol.md "Error mapping"), consuming a bounded body
    /// excerpt for the message. 404 is mapped by the callers that have a
    /// NotFound meaning for it; anything unrecognized is `Other`.
    fn status_error(
        &self,
        ctx: &str,
        status: u16,
        write_op: bool,
        headers: &[(String, String)],
        stream: TcpStream,
    ) -> StorageError {
        let msg = format!("{ctx}: http status {status}{}", Self::error_detail(headers, stream));
        match status {
            401 => StorageError::Unauthorized(msg),
            // 403 on a *read* is a proxy/misconfig, not "server is
            // read-only" — that stays Other.
            403 if write_op => StorageError::ReadOnly(msg),
            409 => StorageError::Conflict(msg),
            _ => StorageError::Other(msg),
        }
    }

    fn body_reader(headers: &[(String, String)], stream: TcpStream) -> Box<dyn Read + Send> {
        if header(headers, "transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
        {
            Box::new(ChunkedReader::new(stream))
        } else if let Some(n) = header(headers, "content-length").and_then(|v| v.parse().ok()) {
            let n: u64 = n;
            Box::new(stream.take(n))
        } else {
            // Connection: close delimited.
            Box::new(stream)
        }
    }
}

impl ReplicaClient for HttpReplicaClient {
    fn client_type(&self) -> &'static str {
        "http"
    }

    fn ltx_files(&self, level: u8, seek: Txid, use_metadata: bool) -> Result<Vec<FileInfo>> {
        let mut target = format!("{}/ltx/{}?seek={}", self.base, level, seek);
        if use_metadata {
            target.push_str("&meta=1");
        }
        let ctx = format!("list level {level}");
        let (status, headers, stream) = self.get(&target)?;
        if status != 200 {
            return Err(self.status_error(&ctx, status, false, &headers, stream));
        }

        // Listings always carry content-length (a close-delimited body
        // cannot distinguish completion from a cut connection, and a
        // silently truncated listing could masquerade as divergence).
        let declared: u64 = header(&headers, "content-length")
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| {
                StorageError::Other(format!("list level {level}: missing content-length"))
            })?;
        if declared > 64 << 20 {
            return Err(StorageError::Other(format!(
                "list level {level}: implausible listing size {declared}"
            )));
        }
        // Manual read loop: socket errors are transport (Unavailable), and
        // the token is checked per chunk.
        let mut raw = Vec::new();
        let mut rd = Self::body_reader(&headers, stream);
        let mut buf = vec![0u8; 64 << 10];
        loop {
            self.check_cancel()?;
            match rd.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => raw.extend_from_slice(&buf[..n]),
                Err(e) => {
                    // Prefer Cancelled when a cancel landed mid-read.
                    self.check_cancel()?;
                    return Err(transport_err(&ctx, e));
                }
            }
        }
        if raw.len() as u64 != declared {
            return Err(StorageError::Unavailable(format!(
                "list level {level}: truncated listing ({} of {declared} bytes)",
                raw.len()
            )));
        }
        let body = String::from_utf8(raw).map_err(|_| {
            StorageError::Other(format!("list level {level}: non-utf8 listing"))
        })?;

        let mut infos = Vec::new();
        for line in body.lines() {
            // Unparseable lines are skipped, mirroring the dir backend's
            // stray-file stance — and keeping old clients tolerant of
            // future additive columns is NOT allowed by protocol v1 (any
            // format change bumps the version), so skipping is purely
            // defensive.
            if let Some(info) = parse_listing_line(line, level) {
                infos.push(info);
            }
        }
        infos.sort_by_key(|f| (f.min_txid, f.max_txid));
        Ok(infos)
    }

    fn open_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        offset: u64,
        size: u64,
    ) -> Result<Box<dyn Read + Send>> {
        let mut target =
            format!("{}/ltx/{}/{}", self.base, level, format_filename(min_txid, max_txid));
        if offset > 0 || size > 0 {
            target.push_str(&format!("?offset={offset}&size={size}"));
        }
        // The request is sent and the status validated here (eager open):
        // a 404 surfaces as NotFound at open time, exactly like the dir
        // backend, preserving the caller's list/open race handling.
        let (status, headers, stream) = self.get(&target)?;
        match status {
            // Whole-file reads (the restore/apply paths) are spooled to an
            // unlinked temp file before returning: the k-way restore merge
            // can leave a reader untouched for minutes, and a raw socket
            // parked that long trips the server's stalled-peer write
            // timeout. Spooling drains every body eagerly, surfaces
            // transport errors here (as transient Io, not mid-merge decode
            // errors), and hands the merge a local file. Ranged reads are
            // small and stay streaming.
            200 if offset == 0 && size == 0 => {
                let spool = spool_body(Self::body_reader(&headers, stream), &self.cancel)?;
                Ok(Box::new(spool))
            }
            200 => Ok(Self::body_reader(&headers, stream)),
            404 => Err(StorageError::NotFound { level, min_txid, max_txid }),
            _ => Err(self.status_error(
                &format!("open L{level} {min_txid}-{max_txid}"),
                status,
                false,
                &headers,
                stream,
            )),
        }
    }

    /// Pushes one LTX file to the server (reversed-role replication: this
    /// process writes, the listening liters receives). Requires a server
    /// with `writable: true`; read-only servers answer 403. Atomicity and
    /// idempotency are the receiving backend's (tmp+rename on dir), so a
    /// connection cut mid-push leaves nothing behind and a re-push of the
    /// same key is harmless — exactly the retry semantics `Writer` assumes.
    fn write_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        rd: &mut dyn Read,
    ) -> Result<FileInfo> {
        let target =
            format!("{}/ltx/{}/{}", self.base, level, format_filename(min_txid, max_txid));
        let ctx = format!("push L{level} {min_txid}-{max_txid}");
        let (status, headers, stream) = self.put(&target, rd)?;
        match status {
            200 => {
                let mut body = String::new();
                Self::body_reader(&headers, stream)
                    .take(4096)
                    .read_to_string(&mut body)
                    .map_err(|e| transport_err(&ctx, e))?;
                parse_listing_line(body.lines().next().unwrap_or(""), level).ok_or_else(|| {
                    StorageError::Other(format!("push L{level}: bad put response {body:?}"))
                })
            }
            _ => Err(self.status_error(&ctx, status, true, &headers, stream)),
        }
    }

    fn delete_ltx_files(&self, infos: &[FileInfo]) -> Result<()> {
        for info in infos {
            let target = format!(
                "{}/ltx/{}/{}",
                self.base,
                info.level,
                format_filename(info.min_txid, info.max_txid)
            );
            let (status, headers, stream) = self.request("DELETE", &target)?;
            // Missing files are not an error (trait contract); the server
            // answers 200 for those too.
            if status != 200 {
                return Err(self.status_error(
                    &format!("delete L{} {}-{}", info.level, info.min_txid, info.max_txid),
                    status,
                    true,
                    &headers,
                    stream,
                ));
            }
        }
        Ok(())
    }

    fn delete_all(&self) -> Result<()> {
        let (status, headers, stream) = self.request("DELETE", &format!("{}/all", self.base))?;
        if status != 200 {
            return Err(self.status_error("delete all", status, true, &headers, stream));
        }
        Ok(())
    }

    fn open_ltx_stream(&self, seek: Txid) -> Result<Option<Box<dyn LtxStream>>> {
        let (status, headers, stream) = self.get(&format!("{}/stream?seek={}", self.base, seek))?;
        if status != 200 {
            return Err(self.status_error("open stream", status, false, &headers, stream));
        }
        // Short read timeout from here on: each expiry becomes one Idle tick
        // (at frame boundaries) so followers stay cancel-responsive.
        stream.set_read_timeout(Some(STREAM_TICK))?;
        Ok(Some(Box::new(HttpLtxStream {
            body: ChunkedReader::new(stream),
            line: LineBuf::default(),
            state: HttpStreamState::Preamble,
            last_byte: Instant::now(),
            deadman: self.opts.stream_deadman,
            cancel: Arc::clone(&self.cancel),
        })))
    }

    fn set_cancel(&self, token: CancelToken) {
        *self.cancel.lock().unwrap() = token;
    }
}

/// Checks the client's *current* token (the cell contents can be replaced
/// by `set_cancel` mid-operation; the newest token wins).
fn check_token(cancel: &Mutex<CancelToken>) -> Result<()> {
    cancel.lock().unwrap().check()
}

/// Socket-side failure → `Unavailable` (the retry-later signal), with
/// context. Local-file I/O keeps `StorageError::Io`; this is only for
/// transport errors.
fn transport_err(ctx: &str, e: std::io::Error) -> StorageError {
    StorageError::Unavailable(format!("{ctx}: {e}"))
}

/// Writes all of `buf` to the socket, checking the cancellation token on
/// every zero-progress send-timeout tick (the socket's write timeout is
/// `WRITE_TICK`) and giving up with `Unavailable` after `io_timeout` of
/// cumulative zero progress. Any progress resets the stall budget, so a
/// slow-but-flowing uplink is never killed.
///
/// Soundness of retrying the same slice after a timeout: with SO_SNDTIMEO
/// on a *blocking* TCP socket, a `write` call that errors with
/// `WouldBlock`/`TimedOut` has transferred **zero** bytes of that call —
/// partial progress is returned as `Ok(n)` instead. Linux implements this
/// in `tcp_sendmsg` (on a send-buffer wait timeout it returns the copied
/// count if any bytes were queued, and errors only at zero progress);
/// macOS/XNU's `dofilewrite` converts EWOULDBLOCK-with-partial-uio-progress
/// into a partial-count success. Verified empirically on macOS (loopback
/// pair, unread peer, 200ms SO_SNDTIMEO, 4KiB–32MiB writes: bytes received
/// always equaled the sum of `Ok(n)` returns). This holds ONLY per raw
/// `write` call — `write_all`'s internal progress is invisible after an
/// error — which is why this loop tracks its own offset and nothing here
/// wraps `write_all`.
fn write_full(
    stream: &TcpStream,
    buf: &[u8],
    cancel: &Mutex<CancelToken>,
    io_timeout: Duration,
) -> Result<()> {
    let mut w: &TcpStream = stream;
    let mut pos = 0;
    let mut stalled = Instant::now();
    while pos < buf.len() {
        check_token(cancel)?;
        match w.write(&buf[pos..]) {
            Ok(0) => return Err(StorageError::Unavailable("socket write returned 0".into())),
            Ok(n) => {
                pos += n;
                stalled = Instant::now();
            }
            Err(e) if is_timeout(&e) => {
                if stalled.elapsed() >= io_timeout {
                    return Err(StorageError::Unavailable(format!(
                        "write stalled: no progress for {}s",
                        io_timeout.as_secs()
                    )));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => {
                // Prefer Cancelled when a cancel landed mid-write.
                check_token(cancel)?;
                return Err(transport_err("write", e));
            }
        }
    }
    Ok(())
}

/// Parses one `{name} {size} {created_ms|-}` listing line (also the body of
/// a successful PUT response). `None` for anything malformed.
fn parse_listing_line(line: &str, level: u8) -> Option<FileInfo> {
    let mut parts = line.split_whitespace();
    let (name, size, created) = (parts.next()?, parts.next()?, parts.next()?);
    let (min_txid, max_txid) = parse_filename(name)?;
    let size = size.parse::<u64>().ok()?;
    let created_at = match created {
        "-" => None,
        ms => match ms.parse::<u64>() {
            Ok(ms) => Some(UNIX_EPOCH + Duration::from_millis(ms)),
            Err(_) => None,
        },
    };
    Some(FileInfo { level, min_txid, max_txid, size, created_at, ..Default::default() })
}

/// Drains `body` into an anonymous (created then immediately unlinked) temp
/// file and returns it positioned at the start. The fd keeps the data alive;
/// nothing is left on disk regardless of how the reader is dropped.
fn spool_body(mut body: Box<dyn Read + Send>, cancel: &Mutex<CancelToken>) -> Result<std::fs::File> {
    use std::io::{Seek, SeekFrom};

    let mut file = super::unlinked_temp_file()?;
    // Manual copy loop: socket-side read failures are transport errors
    // (Unavailable — the caller re-plans), local spool writes stay Io, and
    // the token is checked per chunk so a cancelled restore stops pulling
    // promptly.
    let mut buf = vec![0u8; 64 << 10];
    loop {
        check_token(cancel)?;
        match body.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => file.write_all(&buf[..n])?,
            Err(e) => {
                // Prefer Cancelled when a cancel landed mid-read.
                check_token(cancel)?;
                return Err(transport_err("read file body", e));
            }
        }
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

enum HttpStreamState {
    /// Expecting the `liters-stream 1` preamble line.
    Preamble,
    /// Expecting a frame line.
    Frame,
    /// Copying a frame body into the caller's sink.
    Body { info: FileInfo, remaining: u64 },
}

struct HttpLtxStream {
    body: ChunkedReader<TcpStream>,
    line: LineBuf,
    state: HttpStreamState,
    last_byte: Instant,
    /// Dead-man bound (`HttpClientOptions::stream_deadman`): zero received
    /// bytes for this long means the stream is dead.
    deadman: Duration,
    /// The owning client's current-token cell: a cancel installed via
    /// `set_cancel` interrupts the stream within one STREAM_TICK.
    cancel: Arc<Mutex<CancelToken>>,
}

impl HttpLtxStream {
    fn deadman(&self) -> Result<()> {
        if self.last_byte.elapsed() >= self.deadman {
            return Err(StorageError::Unavailable(format!(
                "ltx stream stalled: no data for {}s",
                self.deadman.as_secs()
            )));
        }
        Ok(())
    }
}

/// In-stream I/O taxonomy: malformed framing (bad chunk sizes, non-utf8 or
/// oversized lines) is a protocol error (`Other`); everything else — EOF
/// mid-body, resets, broken pipes — is transport (`Unavailable`).
fn stream_io_err(e: std::io::Error) -> StorageError {
    match e.kind() {
        std::io::ErrorKind::InvalidData => StorageError::Other(format!("ltx stream: {e}")),
        _ => StorageError::Unavailable(format!("ltx stream: {e}")),
    }
}

fn parse_frame_line(line: &str) -> Result<HttpStreamState> {
    let mut parts = line.split_whitespace();
    let protocol_err = || StorageError::Other(format!("bad stream frame {line:?}"));
    match parts.next() {
        Some("ltx") => {
            let (Some(level), Some(min), Some(max), Some(size), None) =
                (parts.next(), parts.next(), parts.next(), parts.next(), parts.next())
            else {
                return Err(protocol_err());
            };
            let level: u8 = level.parse().map_err(|_| protocol_err())?;
            let min_txid = Txid::parse(min).ok_or_else(protocol_err)?;
            let max_txid = Txid::parse(max).ok_or_else(protocol_err)?;
            let size: u64 = size.parse().map_err(|_| protocol_err())?;
            let info =
                FileInfo { level, min_txid, max_txid, size, ..Default::default() };
            Ok(HttpStreamState::Body { info, remaining: size })
        }
        _ => Err(protocol_err()),
    }
}

impl LtxStream for HttpLtxStream {
    fn next(&mut self, sink: &mut dyn Write) -> Result<StreamEvent> {
        loop {
            // Every pass — idle ticks, frame lines, and body chunks —
            // observes the current token, so a cancel interrupts within one
            // STREAM_TICK even while bytes are flowing.
            check_token(&self.cancel)?;
            match &mut self.state {
                HttpStreamState::Preamble | HttpStreamState::Frame => {
                    let line = match self.line.read_line(&mut self.body) {
                        Ok(l) => l,
                        Err(e) if is_timeout(&e) => {
                            // Between frames (a partial line survives in the
                            // LineBuf), so an Idle tick is safe.
                            self.deadman()?;
                            return Ok(StreamEvent::Idle { bucket_max: None });
                        }
                        Err(e) => return Err(stream_io_err(e)),
                    };
                    self.last_byte = Instant::now();
                    let Some(line) = line else {
                        // Clean chunked EOF.
                        return match self.state {
                            HttpStreamState::Frame => Ok(StreamEvent::Closed),
                            _ => Err(StorageError::Unavailable(
                                "stream closed before preamble".into(),
                            )),
                        };
                    };

                    if matches!(self.state, HttpStreamState::Preamble) {
                        let expected = format!("liters-stream {PROTOCOL_VERSION}");
                        if line != expected {
                            return Err(StorageError::Other(format!(
                                "bad stream preamble {line:?} (expected {expected:?})"
                            )));
                        }
                        self.state = HttpStreamState::Frame;
                        continue;
                    }

                    // Frame line.
                    if let Some(rest) = line.strip_prefix("ping ") {
                        let bucket_max = Txid::parse(rest.trim()).ok_or_else(|| {
                            StorageError::Other(format!("bad ping frame {line:?}"))
                        })?;
                        return Ok(StreamEvent::Idle { bucket_max: Some(bucket_max) });
                    }
                    if let Some(rest) = line.strip_prefix("gap ") {
                        let next = Txid::parse(rest.trim())
                            .ok_or_else(|| StorageError::Other(format!("bad gap frame {line:?}")))?;
                        return Ok(StreamEvent::Gap { next });
                    }
                    if let Some(rest) = line.strip_prefix("reset ") {
                        let bucket_max = Txid::parse(rest.trim())
                            .ok_or_else(|| StorageError::Other(format!("bad reset frame {line:?}")))?;
                        return Ok(StreamEvent::Reset { bucket_max });
                    }
                    self.state = parse_frame_line(&line)?;
                }
                HttpStreamState::Body { info, remaining } => {
                    if *remaining == 0 {
                        let info = info.clone();
                        self.state = HttpStreamState::Frame;
                        return Ok(StreamEvent::Ltx(info));
                    }
                    let mut buf = [0u8; 8192];
                    let want = (*remaining).min(buf.len() as u64) as usize;
                    match self.body.read(&mut buf[..want]) {
                        Ok(0) => {
                            return Err(StorageError::Unavailable(
                                "stream ended mid-frame".into(),
                            ))
                        }
                        Ok(n) => {
                            self.last_byte = Instant::now();
                            // The sink is the caller's local spool: its
                            // failures stay Io.
                            sink.write_all(&buf[..n])?;
                            *remaining -= n as u64;
                        }
                        // Mid-body timeouts are absorbed (never Idle: the
                        // caller may truncate its sink between next() calls);
                        // the dead-man timer is the stall bound.
                        Err(e) if is_timeout(&e) => self.deadman()?,
                        Err(e) => return Err(stream_io_err(e)),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_parsing() {
        let c = HttpReplicaClient::new("http://example.com:8080/some/base/").unwrap();
        assert_eq!(c.host, "example.com");
        assert_eq!(c.port, 8080);
        assert_eq!(c.base, "/some/base");

        let c = HttpReplicaClient::new("http://10.0.0.1:9999").unwrap();
        assert_eq!((c.host.as_str(), c.port, c.base.as_str()), ("10.0.0.1", 9999, ""));

        let c = HttpReplicaClient::new("http://[::1]:8080").unwrap();
        assert_eq!((c.host.as_str(), c.port), ("::1", 8080));

        let c = HttpReplicaClient::new("http://localhost").unwrap();
        assert_eq!(c.port, 80);

        assert!(HttpReplicaClient::new("https://example.com").is_err());
        assert!(HttpReplicaClient::new("file:///tmp/x").is_err());
        assert!(HttpReplicaClient::new("http://user@host").is_err());
        assert!(HttpReplicaClient::new("http://host:notaport").is_err());
    }

    #[test]
    fn frame_line_parsing() {
        match parse_frame_line("ltx 0 0000000000000005 0000000000000005 1234").unwrap() {
            HttpStreamState::Body { info, remaining } => {
                assert_eq!(info.level, 0);
                assert_eq!(info.min_txid, Txid(5));
                assert_eq!(info.max_txid, Txid(5));
                assert_eq!(info.size, 1234);
                assert_eq!(remaining, 1234);
            }
            _ => panic!("expected body state"),
        }
        assert!(parse_frame_line("ltx 0 xyz 0000000000000005 10").is_err());
        assert!(parse_frame_line("ltx 0 0000000000000005 0000000000000005").is_err());
        assert!(parse_frame_line("ltx 0 0000000000000005 0000000000000005 10 extra").is_err());
        assert!(parse_frame_line("frobnicate").is_err());
    }

    /// The address sweep must observe the cancellation token *between*
    /// attempts, not only on entry and after a successful connect —
    /// otherwise a multi-address blackholed host stretches cancellation
    /// latency to addrs.len() x connect_timeout.
    #[test]
    fn connect_sweep_checks_cancel_between_addresses() {
        // A just-freed loopback port: connect attempts fail fast (refused).
        let refused = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            addr
        };
        let client = HttpReplicaClient::new("http://127.0.0.1:1").unwrap();

        // Live token: the sweep runs to exhaustion and reports transport
        // failure.
        let err = client.try_addrs(vec![refused, refused]).unwrap_err();
        assert!(matches!(err, StorageError::Unavailable(_)), "live token: {err:?}");

        // Cancelled token: the sweep must return Cancelled instead of
        // dialing through the address list.
        let token = CancelToken::new();
        token.cancel();
        client.set_cancel(token);
        let err = client.try_addrs(vec![refused, refused]).unwrap_err();
        assert!(matches!(err, StorageError::Cancelled), "cancelled token: {err:?}");
    }
}
