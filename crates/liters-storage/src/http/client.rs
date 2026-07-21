//! HTTP replica client for a bucket served by a liters [`HttpServer`] (see
//! docs/http-protocol.md for the liters HTTP replication protocol — liters'
//! own, not litestream's). Reads always work; writes
//! (`write_ltx_file`/deletes — i.e. using this client as a `Writer`
//! destination to *push* replication to a listening liters) require the
//! server to run with `writable: true`, otherwise they fail with a clear
//! read-only error.
//!
//! This type owns only the *protocol* — building request targets, validating
//! the `x-liters-protocol` header on every response, parsing listing lines and
//! the `liters-stream` frame grammar. It never touches a socket: every request
//! is handed to an [`HttpTransport`]. The default
//! [`StdNetTransport`](super::StdNetTransport) reproduces the v1 wire behavior
//! (one `Connection: close` socket per request, `http://` only). An embedder
//! (the mobile FFI layer) can inject a foreign transport — one backed by the
//! platform HTTP client — via [`HttpReplicaClient::with_transport`], so that
//! many followers pointed at one authority coalesce onto a single (HTTP/2)
//! connection with the platform owning TLS and keepalive. The protocol, and
//! its version, are identical across transports.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use ltx::{format_filename, parse_filename, FileInfo, Txid};

use crate::{CancelToken, LtxStream, ReplicaClient, Result, StorageError, StreamEvent};

use super::transport::{
    BodyReader, Cancel, HttpTransport, StdNetTransport, TransportRequest, TransportResponse,
};
use super::wire::{header, is_timeout, LineBuf, PROTOCOL_HEADER, PROTOCOL_VERSION};

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
    /// TCP connect timeout, per resolved address. Default 10s. Applies to the
    /// built-in [`StdNetTransport`]; a foreign transport owns its own timeouts.
    pub connect_timeout: Duration,
    /// Socket read timeout, and the cumulative zero-progress budget for
    /// socket writes. Default 30s. Applies to the built-in transport.
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
/// With the default transport, `url` is `http://host:port[/base]` — https is
/// not supported (put a reverse proxy in front for TLS, and disable response
/// buffering for `/stream`, see docs/http-protocol.md). A foreign transport
/// injected via [`HttpReplicaClient::with_transport`] may accept `https://`
/// and owns TLS itself. A multi-DB server's buckets are addressed through the
/// base path: `http://host:port/db/{name}`.
pub struct HttpReplicaClient {
    /// Absolute request base (`scheme://authority[/prefix]`, no trailing
    /// slash). Every request target is built by appending to it.
    base_url: String,
    opts: HttpClientOptions,
    /// The transport that actually moves bytes. Shared (`Arc`) so many
    /// clients on one authority can share a foreign connection pool.
    transport: Arc<dyn HttpTransport>,
    /// Current cancellation token ([`ReplicaClient::set_cancel`]). Blocking
    /// loops — uploads, body reads, stream ticks — observe the *current*
    /// token through this shared cell, so a token installed before or
    /// during an operation interrupts it.
    cancel: Arc<Mutex<CancelToken>>,
}

impl HttpReplicaClient {
    /// A client using the built-in [`StdNetTransport`] (`http://` only).
    pub fn new(url: impl AsRef<str>) -> Result<HttpReplicaClient> {
        Self::with_options(url, HttpClientOptions::default())
    }

    /// A client using the built-in [`StdNetTransport`] with explicit options.
    pub fn with_options(
        url: impl AsRef<str>,
        options: HttpClientOptions,
    ) -> Result<HttpReplicaClient> {
        options.validate()?;
        let base_url = normalize_base_url(url.as_ref(), false)?;
        let transport =
            Arc::new(StdNetTransport::new(options.connect_timeout, options.io_timeout));
        Ok(HttpReplicaClient { base_url, opts: options, transport, cancel: Arc::default() })
    }

    /// A client whose requests are executed by a caller-supplied
    /// [`HttpTransport`] rather than the built-in socket code. This is how an
    /// embedder hands replication to the platform HTTP client (e.g. OkHttp on
    /// Android): pass **one** shared transport to every follower on a host and
    /// they coalesce onto a single connection. `https://` urls are accepted —
    /// the transport owns TLS.
    pub fn with_transport(
        url: impl AsRef<str>,
        options: HttpClientOptions,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<HttpReplicaClient> {
        options.validate()?;
        let base_url = normalize_base_url(url.as_ref(), true)?;
        Ok(HttpReplicaClient { base_url, opts: options, transport, cancel: Arc::default() })
    }

    fn check_cancel(&self) -> Result<()> {
        self.cancel.lock().unwrap().check()
    }

    fn cancel_handle(&self) -> Cancel {
        Cancel::new(Arc::clone(&self.cancel))
    }

    /// Optional request headers: `authorization` on every request, the
    /// fencing headers (`x-liters-writer-id`/`x-liters-writer-takeover`) on
    /// write ops only. Values were validated at construction.
    fn extra_headers(&self, write_op: bool) -> Vec<(String, String)> {
        let mut h = Vec::new();
        if let Some(token) = &self.opts.auth_token {
            h.push(("authorization".into(), format!("Bearer {token}")));
        }
        if write_op {
            if let Some(id) = &self.opts.writer_id {
                h.push(("x-liters-writer-id".into(), id.clone()));
            }
            if self.opts.takeover {
                h.push(("x-liters-writer-takeover".into(), "1".into()));
            }
        }
        h
    }

    /// Executes one request through the transport and validates the liters
    /// protocol header — every response must carry it, so proxies and foreign
    /// servers fail loudly rather than being misparsed.
    fn exec(
        &self,
        method: &str,
        url: &str,
        long_lived: bool,
        body: Option<&mut (dyn Read + Send)>,
    ) -> Result<TransportResponse> {
        self.check_cancel()?;
        let write_op = method != "GET";
        let resp = self.transport.execute(TransportRequest {
            method,
            url,
            headers: self.extra_headers(write_op),
            body,
            long_lived,
            cancel: self.cancel_handle(),
        })?;
        match header(&resp.headers, PROTOCOL_HEADER) {
            Some(PROTOCOL_VERSION) => Ok(resp),
            Some(v) => Err(StorageError::Other(format!(
                "server speaks liters protocol {v:?}, this client speaks {PROTOCOL_VERSION}"
            ))),
            None => Err(StorageError::Other(format!(
                "{url} is not a liters server (status {}, no {PROTOCOL_HEADER} header)",
                resp.status
            ))),
        }
    }

    /// Short error-body excerpt for diagnostics (e.g. the server's
    /// "read-only" message).
    fn error_detail(resp: TransportResponse) -> String {
        let mut buf = String::new();
        let _ = BodyReader(resp.body).take(512).read_to_string(&mut buf);
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
    fn status_error(&self, ctx: &str, write_op: bool, resp: TransportResponse) -> StorageError {
        let status = resp.status;
        let msg = format!("{ctx}: http status {status}{}", Self::error_detail(resp));
        match status {
            401 => StorageError::Unauthorized(msg),
            // 403 on a *read* is a proxy/misconfig, not "server is
            // read-only" — that stays Other.
            403 if write_op => StorageError::ReadOnly(msg),
            409 => StorageError::Conflict(msg),
            _ => StorageError::Other(msg),
        }
    }
}

impl ReplicaClient for HttpReplicaClient {
    fn client_type(&self) -> &'static str {
        "http"
    }

    fn ltx_files(&self, level: u8, seek: Txid, use_metadata: bool) -> Result<Vec<FileInfo>> {
        let mut url = format!("{}/ltx/{}?seek={}", self.base_url, level, seek);
        if use_metadata {
            url.push_str("&meta=1");
        }
        let ctx = format!("list level {level}");
        let resp = self.exec("GET", &url, false, None)?;
        if resp.status != 200 {
            return Err(self.status_error(&ctx, false, resp));
        }

        // Listings always carry content-length (a close-delimited body cannot
        // distinguish completion from a cut connection, and a silently
        // truncated listing could masquerade as divergence).
        let declared: u64 = header(&resp.headers, "content-length")
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| {
                StorageError::Other(format!("list level {level}: missing content-length"))
            })?;
        if declared > 64 << 20 {
            return Err(StorageError::Other(format!(
                "list level {level}: implausible listing size {declared}"
            )));
        }
        // Manual read loop: socket errors are transport (Unavailable), and the
        // token is checked per chunk.
        let mut raw = Vec::new();
        let mut rd = BodyReader(resp.body);
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
        let body = String::from_utf8(raw)
            .map_err(|_| StorageError::Other(format!("list level {level}: non-utf8 listing")))?;

        let mut infos = Vec::new();
        for line in body.lines() {
            // Unparseable lines are skipped, mirroring the dir backend's
            // stray-file stance — and keeping old clients tolerant of future
            // additive columns is NOT allowed by protocol v1 (any format
            // change bumps the version), so skipping is purely defensive.
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
        let mut url =
            format!("{}/ltx/{}/{}", self.base_url, level, format_filename(min_txid, max_txid));
        if offset > 0 || size > 0 {
            url.push_str(&format!("?offset={offset}&size={size}"));
        }
        // The request is sent and the status validated here (eager open): a
        // 404 surfaces as NotFound at open time, exactly like the dir backend,
        // preserving the caller's list/open race handling.
        let resp = self.exec("GET", &url, false, None)?;
        match resp.status {
            // Whole-file reads (the restore/apply paths) are spooled to an
            // unlinked temp file before returning: the k-way restore merge can
            // leave a reader untouched for minutes, and a raw socket parked
            // that long trips the server's stalled-peer write timeout.
            // Spooling drains every body eagerly, surfaces transport errors
            // here (as transient Io, not mid-merge decode errors), and hands
            // the merge a local file. Ranged reads are small and stay
            // streaming.
            200 if offset == 0 && size == 0 => {
                let spool =
                    spool_body(Box::new(BodyReader(resp.body)), &self.cancel_handle())?;
                Ok(Box::new(spool))
            }
            200 => Ok(Box::new(BodyReader(resp.body))),
            404 => Err(StorageError::NotFound { level, min_txid, max_txid }),
            _ => Err(self.status_error(
                &format!("open L{level} {min_txid}-{max_txid}"),
                false,
                resp,
            )),
        }
    }

    /// Pushes one LTX file to the server (reversed-role replication: this
    /// process writes, the listening liters receives). Requires a server with
    /// `writable: true`; read-only servers answer 403. Atomicity and
    /// idempotency are the receiving backend's (tmp+rename on dir), so a
    /// connection cut mid-push leaves nothing behind and a re-push of the same
    /// key is harmless — exactly the retry semantics `Writer` assumes.
    fn write_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        rd: &mut (dyn Read + Send),
    ) -> Result<FileInfo> {
        let url =
            format!("{}/ltx/{}/{}", self.base_url, level, format_filename(min_txid, max_txid));
        let ctx = format!("push L{level} {min_txid}-{max_txid}");
        let resp = self.exec("PUT", &url, false, Some(rd))?;
        match resp.status {
            200 => {
                let mut body = String::new();
                BodyReader(resp.body)
                    .take(4096)
                    .read_to_string(&mut body)
                    .map_err(|e| transport_err(&ctx, e))?;
                parse_listing_line(body.lines().next().unwrap_or(""), level).ok_or_else(|| {
                    StorageError::Other(format!("push L{level}: bad put response {body:?}"))
                })
            }
            _ => Err(self.status_error(&ctx, true, resp)),
        }
    }

    fn delete_ltx_files(&self, infos: &[FileInfo]) -> Result<()> {
        for info in infos {
            let url = format!(
                "{}/ltx/{}/{}",
                self.base_url,
                info.level,
                format_filename(info.min_txid, info.max_txid)
            );
            let resp = self.exec("DELETE", &url, false, None)?;
            // Missing files are not an error (trait contract); the server
            // answers 200 for those too.
            if resp.status != 200 {
                return Err(self.status_error(
                    &format!("delete L{} {}-{}", info.level, info.min_txid, info.max_txid),
                    true,
                    resp,
                ));
            }
        }
        Ok(())
    }

    fn delete_all(&self) -> Result<()> {
        let resp = self.exec("DELETE", &format!("{}/all", self.base_url), false, None)?;
        if resp.status != 200 {
            return Err(self.status_error("delete all", true, resp));
        }
        Ok(())
    }

    fn open_ltx_stream(&self, seek: Txid) -> Result<Option<Box<dyn LtxStream>>> {
        let url = format!("{}/stream?seek={}", self.base_url, seek);
        let resp = self.exec("GET", &url, true, None)?;
        if resp.status != 200 {
            return Err(self.status_error("open stream", false, resp));
        }
        Ok(Some(Box::new(HttpLtxStream {
            body: BodyReader(resp.body),
            line: LineBuf::default(),
            state: HttpStreamState::Preamble,
            last_byte: Instant::now(),
            deadman: self.opts.stream_deadman,
            cancel: self.cancel_handle(),
        })))
    }

    fn set_cancel(&self, token: CancelToken) {
        *self.cancel.lock().unwrap() = token;
    }
}

/// Normalizes a client base url to `scheme://authority[/prefix]` (no trailing
/// slash). Rejects userinfo and (unless `allow_https`) `https://`. The
/// authority's host/port validity is checked per-request by the transport.
fn normalize_base_url(url: &str, allow_https: bool) -> Result<String> {
    let (scheme, rest) = if let Some(rest) = url.strip_prefix("http://") {
        ("http", rest)
    } else if let Some(rest) = url.strip_prefix("https://") {
        if !allow_https {
            return Err(StorageError::Other(
                "https is not supported; terminate TLS with a reverse proxy and use http://, \
                 or inject a transport that owns TLS"
                    .into(),
            ));
        }
        ("https", rest)
    } else {
        return Err(StorageError::Other(format!(
            "invalid liters url {url:?}: expected http://host:port[/path]"
        )));
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].trim_end_matches('/')),
        None => (rest, ""),
    };
    if authority.contains('@') {
        return Err(StorageError::Other("userinfo in url is not supported".into()));
    }
    if authority.is_empty() {
        return Err(StorageError::Other(format!("missing host in url {url:?}")));
    }
    Ok(format!("{scheme}://{authority}{path}"))
}

/// Socket-side failure → `Unavailable` (the retry-later signal), with context.
/// Local-file I/O keeps `StorageError::Io`; this is only for transport errors.
fn transport_err(ctx: &str, e: std::io::Error) -> StorageError {
    StorageError::Unavailable(format!("{ctx}: {e}"))
}

/// Parses one `{name} {size} {created_ms|-}` listing line (also the body of a
/// successful PUT response). `None` for anything malformed.
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
fn spool_body(mut body: Box<dyn Read + Send>, cancel: &Cancel) -> Result<std::fs::File> {
    use std::io::{Seek, SeekFrom};

    let mut file = super::unlinked_temp_file()?;
    // Manual copy loop: socket-side read failures are transport errors
    // (Unavailable — the caller re-plans), local spool writes stay Io, and the
    // token is checked per chunk so a cancelled restore stops pulling promptly.
    let mut buf = vec![0u8; 64 << 10];
    loop {
        cancel.check()?;
        match body.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => file.write_all(&buf[..n])?,
            Err(e) => {
                // Prefer Cancelled when a cancel landed mid-read.
                cancel.check()?;
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
    /// The transport's response body, adapted so an idle tick reads as a
    /// `WouldBlock` timeout (see [`BodyReader`]) — so the frame loop below
    /// handles idle ticks with the same `is_timeout` path the old raw socket
    /// used.
    body: BodyReader,
    line: LineBuf,
    state: HttpStreamState,
    last_byte: Instant,
    /// Dead-man bound (`HttpClientOptions::stream_deadman`): zero received
    /// bytes for this long means the stream is dead.
    deadman: Duration,
    /// The owning client's current-token handle: a cancel installed via
    /// `set_cancel` interrupts the stream within one idle tick.
    cancel: Cancel,
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
            let info = FileInfo { level, min_txid, max_txid, size, ..Default::default() };
            Ok(HttpStreamState::Body { info, remaining: size })
        }
        _ => Err(protocol_err()),
    }
}

impl LtxStream for HttpLtxStream {
    fn next(&mut self, sink: &mut dyn Write) -> Result<StreamEvent> {
        loop {
            // Every pass — idle ticks, frame lines, and body chunks — observes
            // the current token, so a cancel interrupts within one idle tick
            // even while bytes are flowing.
            self.cancel.check()?;
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
                        // Clean EOF.
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
                        let bucket_max = Txid::parse(rest.trim()).ok_or_else(|| {
                            StorageError::Other(format!("bad reset frame {line:?}"))
                        })?;
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
                            return Err(StorageError::Unavailable("stream ended mid-frame".into()))
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
    fn base_url_normalization() {
        let c = HttpReplicaClient::new("http://example.com:8080/some/base/").unwrap();
        assert_eq!(c.base_url, "http://example.com:8080/some/base");

        let c = HttpReplicaClient::new("http://10.0.0.1:9999").unwrap();
        assert_eq!(c.base_url, "http://10.0.0.1:9999");

        let c = HttpReplicaClient::new("http://[::1]:8080").unwrap();
        assert_eq!(c.base_url, "http://[::1]:8080");

        // https is rejected on the built-in transport, accepted with a foreign
        // one.
        assert!(HttpReplicaClient::new("https://example.com").is_err());
        assert!(HttpReplicaClient::new("file:///tmp/x").is_err());
        assert!(HttpReplicaClient::new("http://user@host").is_err());

        let transport = Arc::new(StdNetTransport::new(
            Duration::from_secs(10),
            Duration::from_secs(30),
        ));
        let c = HttpReplicaClient::with_transport(
            "https://example.com/db/x/",
            HttpClientOptions::default(),
            transport,
        )
        .unwrap();
        assert_eq!(c.base_url, "https://example.com/db/x");
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
}
