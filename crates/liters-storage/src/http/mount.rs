//! Transport-agnostic liters replication handler. [`Mount`] is everything the
//! HTTP protocol does to *one* database's bucket — listings, file bytes, the
//! `/stream` follow feed, and pushed writes with fencing — expressed as a
//! pure request-in / response-out function ([`Mount::handle`]). It owns the
//! bucket, its `/stream` wake signal, and its writer lease; it never touches a
//! socket, a listener, or a port.
//!
//! The batteries-included [`HttpServer`](super::HttpServer) is a thin
//! `TcpListener` driver over a `Mount`. An external Rust HTTP server embeds one
//! the same way: build a [`Request`] from its own framework's request (with the
//! mount's URL prefix already stripped by its router), call [`Mount::handle`],
//! and write the [`Response`] back — status, headers, and a [`Body`] it either
//! sends whole, copies from a reader, or pumps from a [`StreamBody`]. All
//! wire-format knowledge (listing lines, the frame grammar, status codes, the
//! `x-liters-protocol` header) stays here; the host only supplies parsed
//! request parts and pumps bytes. Wire protocol: docs/http-protocol.md.

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use ltx::{parse_filename, FileInfo, Txid};

use crate::{CancelToken, LtxStream, ReplicaClient, Result, StorageError, SNAPSHOT_LEVEL};

use super::wire::{header, query_param, PROTOCOL_VERSION};

/// Options for one [`Mount`]. The transport-level knobs (socket timeouts, the
/// listener) live on [`HttpServerOptions`](super::HttpServerOptions); these are
/// the ones the protocol itself depends on.
#[derive(Clone, Debug)]
pub struct MountOptions {
    /// Accept pushed replication (`PUT`/`DELETE`, `DELETE /all`) written
    /// through to the backing client. Default `false` (read-only). An
    /// unauthenticated writable mount accepts `DELETE /all` from anyone who
    /// can reach it — set `auth_token` or keep it on a private interface.
    pub writable: bool,
    /// When set, every route requires `authorization: Bearer <token>`
    /// (case-insensitive scheme, exact token). The bare-root health check is
    /// a [`HttpServer`](super::HttpServer) concern and never reaches here.
    pub auth_token: Option<String>,
    /// How long a writer lease is held after the owner's last accepted write
    /// before another writer id may claim the bucket without a takeover
    /// header (docs/http-protocol.md "Fencing"). In-memory; resets when the
    /// `Mount` is dropped.
    pub lease_ttl: Duration,
    /// How often a `/stream` re-lists level 0 when idle — the change-latency
    /// bound for buckets written without the notifying tee, and the upper
    /// bound on how promptly a parked stream notices its request's
    /// [`CancelToken`].
    pub poll_interval: Duration,
    /// Keepalive cadence on `/stream`. Pings carry the bucket-wide max TXID
    /// (divergence evidence for idle followers).
    pub ping_interval: Duration,
}

impl Default for MountOptions {
    fn default() -> Self {
        MountOptions {
            writable: false,
            auth_token: None,
            lease_ttl: Duration::from_secs(24 * 60 * 60),
            poll_interval: Duration::from_secs(1),
            ping_interval: Duration::from_secs(15),
        }
    }
}

/// A parsed request handed to [`Mount::handle`]. The host fills this from its
/// own HTTP framework; `path` is the request path with any mount prefix
/// already stripped (`"ltx/0"`, `"stream"`, `""` for the mount root), and
/// `body` is the decoded request body (de-chunked / length-bounded) — only
/// read for `PUT`.
pub struct Request<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub headers: &'a [(String, String)],
    pub body: &'a mut dyn Read,
    /// Cancels a long-lived `/stream`. Cancelling ends the stream within one
    /// `poll_interval` (the built-in server also wakes it immediately).
    pub cancel: CancelToken,
}

/// What [`Mount::handle`] returns for the host to write back. `headers`
/// carries the semantic headers (`x-liters-protocol`, `content-type`, and the
/// `/stream` no-buffering hints); the host adds transport framing
/// (`content-length` for [`Body::Bytes`], chunked for the streaming variants)
/// and `connection: close`.
pub struct Response {
    pub status: u16,
    pub headers: Vec<(&'static str, String)>,
    pub body: Body,
}

/// The response payload. All three are *unframed* — the host applies HTTP
/// transfer-encoding.
pub enum Body {
    /// A complete in-memory body (listings, errors, the `PUT` reply line).
    Bytes(Vec<u8>),
    /// A finite reader the host streams out (a file `GET`). The backing FD is
    /// held open for the whole transfer; a read error mid-body means the host
    /// must abort the connection (truncated body), never send a short file.
    Reader(Box<dyn Read + Send>),
    /// The long-lived `/stream` feed. The host hands liters a `Write` sink and
    /// liters pushes frames into it until it returns (see [`StreamBody`]).
    Stream(Box<dyn StreamBody>),
}

/// A server-push body: liters writes the frame stream into the host's sink,
/// flushing at frame boundaries, until it ends. `Ok(())` is a clean end (the
/// host terminates the body normally); `Err` is an abort (the host drops the
/// connection so the follower sees a truncated body and resyncs). Honors the
/// request's [`CancelToken`].
pub trait StreamBody: Send {
    fn write_to(self: Box<Self>, out: &mut dyn Write) -> std::io::Result<()>;
}

impl Response {
    /// A `text/plain` response with an in-memory body.
    pub fn text(status: u16, body: impl Into<String>) -> Response {
        Response {
            status,
            headers: vec![("content-type", "text/plain; charset=utf-8".to_string())],
            body: Body::Bytes(body.into().into_bytes()),
        }
    }

    /// A one-line error body (`{msg}\n`), the shape every error path uses.
    pub(crate) fn error(status: u16, msg: &str) -> Response {
        Response::text(status, format!("{msg}\n"))
    }
}

/// The `GET /` / mount-root version line.
pub(crate) fn version_line() -> String {
    format!("liters {}\n", env!("CARGO_PKG_VERSION"))
}

/// Per-bucket change signal: a generation counter under a mutex plus a
/// condvar. Bumped by the notifying tee and the write endpoints on every
/// mutation; a `/stream` waits on it (with a poll_interval timeout) when
/// caught up. The built-in server also bumps it on shutdown to wake parked
/// waiters — the request's cancel flag is part of the wait predicate.
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

    /// Waits until the generation moves past `seen`, `cancel` fires, or
    /// `timeout` elapses.
    fn wait(&self, seen: u64, timeout: Duration, cancel: &CancelToken) {
        let g = self.generation.lock().unwrap();
        let _unused = self
            .cond
            .wait_timeout_while(g, timeout, |g| *g == seen && !cancel.is_cancelled())
            .unwrap();
    }

    fn generation(&self) -> u64 {
        *self.generation.lock().unwrap()
    }
}

/// The most recent writer to touch a bucket with an `x-liters-writer-id`
/// header. In-memory only, so fencing is a dual-writer *detector*, not a
/// distributed lock; the TXID monotonicity rule protects integrity across
/// restarts.
struct WriterLease {
    id: String,
    last_seen: Instant,
}

/// One database served over HTTP: its backing client, its `/stream` wake
/// signal, its writer lease, and the write gate that serializes mutations.
/// See the module docs; [`handle`](Mount::handle) is the whole protocol.
pub struct Mount {
    client: Arc<dyn ReplicaClient>,
    notify: Arc<Notify>,
    lease: Mutex<Option<WriterLease>>,
    /// Serializes bucket mutations so the fence decision and the backend
    /// commit are atomic — without it two concurrent same-TXID PUTs could
    /// both pass the fence and splice divergent lineage. Never held across a
    /// network read: PUT bodies are spooled locally before the gate is taken.
    write_gate: Mutex<()>,
    opts: MountOptions,
}

impl Mount {
    /// Creates a mount over `client`. The intended backing is a
    /// [`DirReplicaClient`](crate::DirReplicaClient) over the same local
    /// bucket a `Writer` pushes to.
    pub fn new(client: Arc<dyn ReplicaClient>, opts: MountOptions) -> Mount {
        Mount {
            client,
            notify: Arc::new(Notify::new()),
            lease: Mutex::new(None),
            write_gate: Mutex::new(()),
            opts,
        }
    }

    /// Wraps a [`ReplicaClient`] so every successful mutation wakes this
    /// mount's `/stream` followers. Hand the wrapped client to the local
    /// `Writer` and pushes reach followers with no poll latency.
    pub fn notifying_client(&self, inner: Box<dyn ReplicaClient>) -> Box<dyn ReplicaClient> {
        Box::new(NotifyingClient { inner, notify: Arc::clone(&self.notify) })
    }

    /// Wakes every `/stream` parked on this mount (used by the built-in
    /// server's shutdown so a parked stream doesn't sleep out its poll
    /// interval).
    pub(crate) fn wake(&self) {
        self.notify.notify();
    }

    /// Handles one request against this mount and returns the response. Pure:
    /// no sockets, no listener. Auth (if configured) gates everything; the
    /// caller has already stripped any mount prefix from `req.path`.
    pub fn handle(&self, req: Request) -> Response {
        let Request { method, path, query, headers, body, cancel } = req;

        if let Some(expected) = &self.opts.auth_token {
            if !authorized(headers, expected) {
                return Response::error(401, "authorization required");
            }
        }

        let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
        match (method, segments.as_slice()) {
            // Mount root: the same version line as the health check — a handy
            // "does this mount resolve" probe (gated by auth, above).
            ("GET", [] | [""]) => Response::text(200, version_line()),
            ("GET", ["ltx", level]) => self.serve_listing(level, query),
            ("GET", ["ltx", level, name]) => self.serve_file(level, name, query),
            ("GET", ["stream"]) => self.serve_stream(query, cancel),
            ("PUT", ["ltx", level, name]) => self.accept_file(level, name, headers, body),
            ("DELETE", ["ltx", level, name]) => self.delete_file(level, name, headers),
            ("DELETE", ["all"]) => self.delete_all(headers),
            ("GET", _) | ("PUT", _) | ("DELETE", _) => Response::error(404, "not found"),
            _ => Response::error(405, "method not allowed"),
        }
    }

    /// `GET /ltx/{level}?seek={txid:016x}&meta=1` — text listing.
    fn serve_listing(&self, level: &str, query: &str) -> Response {
        let Some(level) = parse_level(level) else {
            return Response::error(404, "no such level");
        };
        let seek = match query_param(query, "seek") {
            None => Txid(0),
            Some(s) => match Txid::parse(s) {
                Some(t) => t,
                None => return Response::error(400, "bad seek"),
            },
        };
        let use_metadata = query_param(query, "meta") == Some("1");
        let infos = match self.client.ltx_files(level, seek, use_metadata) {
            Ok(infos) => infos,
            Err(e) => return Response::error(500, &format!("list: {e}")),
        };
        let mut body = String::new();
        for info in infos {
            body.push_str(&listing_line(&info));
        }
        Response::text(200, body)
    }

    /// `GET /ltx/{level}/{name}?offset=N&size=M` — the file bytes. The reader
    /// is opened *here* (eager open: a 404 surfaces at open, not mid-read) and
    /// held by the returned [`Body::Reader`] for the whole transfer,
    /// reproducing the dir backend's FD-held-during-merge safety.
    fn serve_file(&self, level: &str, name: &str, query: &str) -> Response {
        let Some(level) = parse_level(level) else {
            return Response::error(404, "no such level");
        };
        let Some((min_txid, max_txid)) = parse_filename(name) else {
            return Response::error(404, "no such file");
        };
        let parse_u64 = |name: &str| -> Option<u64> {
            match query_param(query, name) {
                None => Some(0),
                Some(v) => v.parse().ok(),
            }
        };
        let (Some(offset), Some(size)) = (parse_u64("offset"), parse_u64("size")) else {
            return Response::error(400, "bad offset/size");
        };
        match self.client.open_ltx_file(level, min_txid, max_txid, offset, size) {
            Ok(rd) => Response {
                status: 200,
                headers: vec![("content-type", "application/octet-stream".to_string())],
                body: Body::Reader(rd),
            },
            Err(StorageError::NotFound { .. }) => Response::error(404, "no such file"),
            Err(e) => Response::error(500, &format!("open: {e}")),
        }
    }

    /// `GET /stream?seek={txid:016x}` — the long-lived follow feed. Returns a
    /// [`Body::Stream`]; the frame loop runs when the host pumps it.
    fn serve_stream(&self, query: &str, cancel: CancelToken) -> Response {
        let seek = match query_param(query, "seek").and_then(Txid::parse) {
            Some(t) if !t.is_zero() => t,
            _ => return Response::error(400, "stream requires seek >= 1"),
        };
        Response {
            status: 200,
            headers: vec![
                ("content-type", "application/x-liters-ltx-stream".to_string()),
                // Reverse proxies must not buffer or transform this response.
                ("cache-control", "no-cache, no-transform".to_string()),
                ("x-accel-buffering", "no".to_string()),
            ],
            body: Body::Stream(Box::new(LtxStreamBody {
                client: Arc::clone(&self.client),
                notify: Arc::clone(&self.notify),
                poll_interval: self.opts.poll_interval,
                ping_interval: self.opts.ping_interval,
                cursor: seek,
                cancel,
            })),
        }
    }

    /// `PUT /ltx/{level}/{name}` — accept one pushed LTX file. Body is spooled
    /// to a local file first (so the write gate is never held across a network
    /// read), then, under the gate, the fence is re-evaluated against fresh
    /// listings — atomic with the commit — and the file is written through.
    fn accept_file(
        &self,
        level: &str,
        name: &str,
        headers: &[(String, String)],
        body: &mut dyn Read,
    ) -> Response {
        if !self.opts.writable {
            return Response::error(403, "server is read-only (writable: false)");
        }
        let Some(level) = parse_level(level) else {
            return Response::error(404, "no such level");
        };
        let Some((min_txid, max_txid)) = parse_filename(name) else {
            return Response::error(404, "bad ltx filename");
        };
        // Advisory pre-body fence: reject clearly-bad pushes before spooling
        // (saves the uplink). It commits nothing — the authoritative fence
        // runs again under the write gate, once the body is fully spooled.
        if let Err(rej) = self.check_fence(headers, Some((level, min_txid, max_txid))) {
            return fence_reject_response(&rej);
        }

        let (mut spool, spool_len) = match spool_put_body(body) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Response::error(400, &format!("short or truncated ltx body: {e}"));
            }
            Err(e) => return Response::error(500, &format!("read body: {e}")),
        };

        // Authoritative fence + commit, atomic per bucket. The body is fully
        // local now, so the gate is held only for local listings and the
        // backend write — never a network wait.
        let _gate = self.write_gate.lock().unwrap();
        let pass = match self.check_fence(headers, Some((level, min_txid, max_txid))) {
            Ok(pass) => pass,
            Err(rej) => return fence_reject_response(&rej),
        };

        if let Some(existing) = &pass.existing_l0 {
            // Idempotent re-push arm: an exact `{min,max}` L0 file already
            // exists. Only a byte-identical body is re-accepted; divergent
            // same-TXID content (two writers racing the same position) is 409.
            let same = existing.size == spool_len
                && match stored_matches_spool(self.client.as_ref(), existing, &mut spool) {
                    Ok(same) => same,
                    Err(e) => return Response::error(500, &format!("fence: {e}")),
                };
            if !same {
                return Response::error(
                    409,
                    &format!(
                        "L0 re-push of {min_txid}-{max_txid} does not match the stored file \
                         (divergent writer?)"
                    ),
                );
            }
        }

        if let Err(e) = spool.seek(SeekFrom::Start(0)) {
            return Response::error(500, &format!("spool: {e}"));
        }
        match self.client.write_ltx_file(level, min_txid, max_txid, &mut spool) {
            Ok(info) => {
                commit_lease(&self.lease, &pass);
                self.notify.notify();
                Response::text(200, listing_line(&info))
            }
            Err(StorageError::Ltx(e)) => Response::error(400, &format!("bad ltx file: {e}")),
            Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Response::error(400, &format!("short or truncated ltx body: {e}"))
            }
            Err(e) => Response::error(500, &format!("write: {e}")),
        }
    }

    /// `DELETE /ltx/{level}/{name}`. Takes the write gate like a PUT so a
    /// fence is never evaluated against a bucket another request is
    /// mid-commit into.
    fn delete_file(&self, level: &str, name: &str, headers: &[(String, String)]) -> Response {
        if !self.opts.writable {
            return Response::error(403, "server is read-only (writable: false)");
        }
        let Some(level) = parse_level(level) else {
            return Response::error(404, "no such level");
        };
        let Some((min_txid, max_txid)) = parse_filename(name) else {
            return Response::error(404, "bad ltx filename");
        };
        let _gate = self.write_gate.lock().unwrap();
        let pass = match self.check_fence(headers, None) {
            Ok(pass) => pass,
            Err(rej) => return fence_reject_response(&rej),
        };
        let info = FileInfo { level, min_txid, max_txid, ..Default::default() };
        match self.client.delete_ltx_files(&[info]) {
            Ok(()) => {
                commit_lease(&self.lease, &pass);
                self.notify.notify();
                Response::text(200, "deleted\n")
            }
            Err(e) => Response::error(500, &format!("delete: {e}")),
        }
    }

    /// `DELETE /all` — wipes the bucket.
    fn delete_all(&self, headers: &[(String, String)]) -> Response {
        if !self.opts.writable {
            return Response::error(403, "server is read-only (writable: false)");
        }
        let _gate = self.write_gate.lock().unwrap();
        let pass = match self.check_fence(headers, None) {
            Ok(pass) => pass,
            Err(rej) => return fence_reject_response(&rej),
        };
        match self.client.delete_all() {
            Ok(()) => {
                commit_lease(&self.lease, &pass);
                self.notify.notify();
                Response::text(200, "deleted\n")
            }
            Err(e) => Response::error(500, &format!("delete all: {e}")),
        }
    }

    /// Write-fencing gate (docs/http-protocol.md "Fencing"): a pure *check*
    /// that never mutates the lease (that is [`commit_lease`], after the
    /// backend write succeeds). `put` carries a PUT's target; DELETEs pass
    /// `None` (lease rules only).
    fn check_fence(
        &self,
        headers: &[(String, String)],
        put: Option<(u8, Txid, Txid)>,
    ) -> std::result::Result<FencePass, FenceReject> {
        let mut pass = FencePass { claim: None, existing_l0: None };
        if let Some(id) = header(headers, "x-liters-writer-id") {
            let takeover = header(headers, "x-liters-writer-takeover") == Some("1");
            let lease = self.lease.lock().unwrap();
            let held_by_other =
                lease.as_ref().filter(|l| l.id != id && l.last_seen.elapsed() < self.opts.lease_ttl);
            if let Some(l) = held_by_other {
                if !takeover {
                    return Err(FenceReject::Conflict(format!("bucket is owned by writer {}", l.id)));
                }
            }
            pass.claim = Some(id.to_string());
        }

        let Some((level, min_txid, max_txid)) = put else { return Ok(pass) };
        if level == 0 {
            let files =
                self.client.ltx_files(0, Txid(0), false).map_err(FenceReject::Backend)?;
            let cur = files.iter().map(|f| f.max_txid).max().unwrap_or(Txid(0));
            let existing = files.iter().find(|f| f.min_txid == min_txid && f.max_txid == max_txid);
            if let Some(existing) = existing {
                pass.existing_l0 = Some(existing.clone());
            } else if !(files.is_empty() || min_txid.0 == cur.0 + 1) {
                return Err(FenceReject::Conflict(format!(
                    "non-monotonic L0 push: {min_txid}-{max_txid} offered, bucket at {cur}"
                )));
            }
        } else {
            let bmax = bucket_max(self.client.as_ref()).map_err(FenceReject::Backend)?;
            if max_txid > bmax {
                return Err(FenceReject::Conflict(format!(
                    "L{level} push beyond bucket max: {min_txid}-{max_txid} offered, bucket max \
                     is {bmax}; upload the L0 backlog before pushing compactions or snapshots"
                )));
            }
        }
        Ok(pass)
    }
}

enum FenceReject {
    /// 409: lease conflict or non-monotonic TXID.
    Conflict(String),
    /// Backend listing failure while evaluating the gate (500).
    Backend(StorageError),
}

/// A passed fence: what the caller may do, and what to commit if the backend
/// mutation succeeds.
struct FencePass {
    /// Writer id to record as the lease holder once the write is accepted
    /// (`None` for headerless v1 pushers).
    claim: Option<String>,
    /// For an L0 push matching an existing exact `{min,max}` file: the stored
    /// entry, so the caller can verify byte-equality before accepting.
    existing_l0: Option<FileInfo>,
}

fn fence_reject_response(rej: &FenceReject) -> Response {
    match rej {
        FenceReject::Conflict(msg) => Response::error(409, msg),
        FenceReject::Backend(e) => Response::error(500, &format!("fence: {e}")),
    }
}

/// Records `pass.claim` as the lease holder. Called only after the backend
/// mutation succeeded, so the lease always reflects the owner's last
/// *accepted* write.
fn commit_lease(lease: &Mutex<Option<WriterLease>>, pass: &FencePass) {
    if let Some(id) = &pass.claim {
        *lease.lock().unwrap() = Some(WriterLease { id: id.clone(), last_seen: Instant::now() });
    }
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

fn parse_level(s: &str) -> Option<u8> {
    // Strict decimal 0..=9, no leading zeros ("00" is not a level).
    if s.len() != 1 {
        return None;
    }
    let level: u8 = s.parse().ok()?;
    (level <= SNAPSHOT_LEVEL).then_some(level)
}

/// `authorization: Bearer <token>` — case-insensitive scheme, exact token.
fn authorized(headers: &[(String, String)], expected: &str) -> bool {
    let Some(value) = header(headers, "authorization") else { return false };
    let Some((scheme, token)) = value.split_once(char::is_whitespace) else { return false };
    scheme.eq_ignore_ascii_case("bearer") && token.trim() == expected
}

/// Receives a PUT body in full into an unlinked local spool file, returned
/// rewound with its length. Bounded only by disk.
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

/// Byte-compares the stored `existing` L0 file against the spooled body (whose
/// size already matched the listing). `Ok(false)` on any difference.
fn stored_matches_spool(
    client: &dyn ReplicaClient,
    existing: &FileInfo,
    spool: &mut std::fs::File,
) -> Result<bool> {
    spool.seek(SeekFrom::Start(0))?;
    let mut stored =
        client.open_ltx_file(existing.level, existing.min_txid, existing.max_txid, 0, 0)?;
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

/// Bucket-wide max TXID across all levels — the divergence evidence
/// `finish_incremental` uses.
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

/// The `/stream` frame producer. Reproduces the follow-feed loop: preamble,
/// then rounds of "list L0, send new frames, wait", with gap/reset/ping
/// framing. `write_to` writes unframed frame bytes to the host's sink
/// (flushing per frame) and returns `Ok(())` on a clean end (gap, reset,
/// cancel) or `Err` to abort.
struct LtxStreamBody {
    client: Arc<dyn ReplicaClient>,
    notify: Arc<Notify>,
    poll_interval: Duration,
    ping_interval: Duration,
    cursor: Txid,
    cancel: CancelToken,
}

impl StreamBody for LtxStreamBody {
    fn write_to(mut self: Box<Self>, out: &mut dyn Write) -> std::io::Result<()> {
        out.write_all(format!("liters-stream {PROTOCOL_VERSION}\n").as_bytes())?;
        out.flush()?;

        // Divergence rule (finish_incremental parity): only positive evidence
        // counts — a non-empty bucket whose max is below the follower's
        // position. An empty bucket is a wipe-then-reseed window.
        let is_reset = |bucket_max: Txid, cursor: Txid| {
            !bucket_max.is_zero() && bucket_max.0 < cursor.0 - 1
        };

        if let Ok(m) = bucket_max(self.client.as_ref()) {
            if is_reset(m, self.cursor) {
                out.write_all(format!("reset {m}\n").as_bytes())?;
                return Ok(());
            }
        }

        let mut last_ping = Instant::now();
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            let generation = self.notify.generation();

            // Full L0 listing each round (L0 is retention-pruned and small):
            // seek-filtered listings would hide multi-TXID files overlapping
            // the cursor.
            let files = match self.client.ltx_files(0, Txid(0), false) {
                Ok(files) => files,
                // Abort: the follower resyncs via listings, where the error
                // surfaces properly.
                Err(_) => return Err(std::io::Error::other("stream: backend list failed")),
            };

            let l0_max = files.iter().map(|f| f.max_txid).max().unwrap_or(Txid(0));
            let mut progressed = false;
            let mut gap: Option<Txid> = None;
            for info in files {
                if info.max_txid.0 < self.cursor.0 {
                    continue; // already applied by the follower
                }
                if info.min_txid.0 > self.cursor.0 {
                    gap = Some(info.min_txid);
                    break;
                }
                match self.send_ltx_frame(out, &info)? {
                    true => {
                        self.cursor = Txid(info.max_txid.0 + 1);
                        progressed = true;
                    }
                    false => break, // 404 race with retention: re-list
                }
            }

            if let Some(next) = gap {
                out.write_all(format!("gap {next}\n").as_bytes())?;
                return Ok(());
            }
            if progressed {
                last_ping = Instant::now(); // frames are liveness for the peer
                continue;
            }

            // Poll-cadence divergence check: the newest L0 always survives
            // retention, so a non-empty L0 whose max trails the follower is
            // reseed evidence. Confirm bucket-wide before declaring it.
            if is_reset(l0_max, self.cursor) {
                if let Ok(m) = bucket_max(self.client.as_ref()) {
                    if is_reset(m, self.cursor) {
                        out.write_all(format!("reset {m}\n").as_bytes())?;
                        return Ok(());
                    }
                }
            }

            // Caught up. Wait for a push notification or the poll tick,
            // whichever is sooner; ping (with a divergence check) on cadence.
            let until_ping = self.ping_interval.saturating_sub(last_ping.elapsed());
            if until_ping.is_zero() {
                let bucket_max = bucket_max(self.client.as_ref()).unwrap_or(Txid(0));
                if is_reset(bucket_max, self.cursor) {
                    out.write_all(format!("reset {bucket_max}\n").as_bytes())?;
                    return Ok(());
                }
                out.write_all(format!("ping {bucket_max}\n").as_bytes())?;
                out.flush()?;
                last_ping = Instant::now();
                continue;
            }
            self.notify.wait(generation, self.poll_interval.min(until_ping), &self.cancel);
        }
    }
}

impl LtxStreamBody {
    /// Sends one `ltx` frame. `Ok(false)` = the file 404ed between list and
    /// open (retention race) — caller re-lists. A short backend read aborts.
    fn send_ltx_frame(&self, out: &mut dyn Write, info: &FileInfo) -> std::io::Result<bool> {
        let rd = match self.client.open_ltx_file(info.level, info.min_txid, info.max_txid, 0, 0) {
            Ok(rd) => rd,
            Err(StorageError::NotFound { .. }) => return Ok(false),
            Err(e) => return Err(std::io::Error::other(e.to_string())),
        };

        out.write_all(
            format!("ltx {} {} {} {}\n", info.level, info.min_txid, info.max_txid, info.size)
                .as_bytes(),
        )?;

        let mut rd = rd.take(info.size);
        let mut buf = vec![0u8; 64 << 10];
        let mut sent: u64 = 0;
        loop {
            if self.cancel.is_cancelled() {
                return Err(std::io::Error::other("stream cancelled"));
            }
            match rd.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    out.write_all(&buf[..n])?;
                    sent += n as u64;
                }
                Err(e) => return Err(e),
            }
        }
        if sent != info.size {
            return Err(std::io::Error::other("ltx file shorter than its listed size"));
        }
        out.flush()?;
        Ok(true)
    }
}

/// Wraps a [`ReplicaClient`]; every successful mutation wakes one mount's
/// `/stream` followers. Reads pass straight through.
struct NotifyingClient {
    inner: Box<dyn ReplicaClient>,
    notify: Arc<Notify>,
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
        self.notify.notify();
        Ok(info)
    }

    fn delete_ltx_files(&self, infos: &[FileInfo]) -> Result<()> {
        self.inner.delete_ltx_files(infos)?;
        self.notify.notify();
        Ok(())
    }

    fn delete_all(&self) -> Result<()> {
        self.inner.delete_all()?;
        self.notify.notify();
        Ok(())
    }

    fn open_ltx_stream(&self, seek: Txid) -> Result<Option<Box<dyn LtxStream>>> {
        self.inner.open_ltx_stream(seek)
    }

    fn set_cancel(&self, token: CancelToken) {
        // The tee must stay transparent: cancellation reaches the backend.
        self.inner.set_cancel(token)
    }
}
