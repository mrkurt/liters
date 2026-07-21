//! The client-side transport seam. [`HttpReplicaClient`](super::HttpReplicaClient)
//! speaks the liters HTTP replication *protocol* (listing lines, the
//! `liters-stream` frame grammar, the `x-liters-protocol` header); it does not
//! own a socket. It drives an [`HttpTransport`]: "execute this request, hand me
//! back the response head and a body I read incrementally". Transfer-encoding
//! (chunked / content-length / close-delimited) is the transport's problem —
//! the body it returns yields already-decoded bytes.
//!
//! Two implementations:
//! * [`StdNetTransport`] — the zero-dependency default: one `Connection: close`
//!   `TcpStream` per request, exactly the v1 behavior. Used by every
//!   Rust-native caller (CLIs, the built-in [`HttpServer`](super::HttpServer)
//!   as a source, the oracle/interop tests). `http://` only.
//! * A *foreign* transport injected by an embedder (the mobile FFI layer hands
//!   in one backed by the platform HTTP client — e.g. Android's OkHttp — so N
//!   followers to one authority coalesce onto a single HTTP/2 connection, TLS
//!   and trust store owned by the platform). That impl lives in the embedding
//!   crate; this module only defines the trait it satisfies.
//!
//! The protocol is transport-agnostic and its version is unchanged either way:
//! the same request heads, the same `liters-stream` framing, ride whatever
//! transport is plugged in.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{CancelToken, Result, StorageError};

use super::wire::{is_timeout, read_head, ChunkedReader};

/// Socket write timeout: each expiry is one cancellation check inside
/// [`write_full`], which owns the cumulative stall budget.
const WRITE_TICK: Duration = Duration::from_secs(2);
/// Socket read timeout while a long-lived (`/stream`) body is idle; each expiry
/// surfaces as one [`BodyRead::Idle`] tick so followers stay cancel-responsive
/// and can run their dead-man timer.
const STREAM_TICK: Duration = Duration::from_secs(1);

/// A cloneable handle onto the owning client's *current* cancellation token.
/// The client keeps the token behind an `Arc<Mutex<..>>` so
/// [`set_cancel`](crate::ReplicaClient::set_cancel) can swap in a fresh token
/// mid-operation (a [`CancelToken`] is sticky and cannot be reset); every
/// transport that observes cancellation reads *through* this handle, so the
/// newest token always wins.
#[derive(Clone)]
pub struct Cancel(Arc<Mutex<CancelToken>>);

impl Cancel {
    pub fn new(cell: Arc<Mutex<CancelToken>>) -> Cancel {
        Cancel(cell)
    }

    /// `Err(StorageError::Cancelled)` once the current token is cancelled.
    pub fn check(&self) -> Result<()> {
        self.0.lock().unwrap().check()
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.lock().unwrap().is_cancelled()
    }
}

/// One request for an [`HttpTransport`] to execute. `headers` are the
/// *semantic* headers only (`authorization`, the fencing headers) — the
/// transport supplies its own `host`, framing, and connection headers. `url`
/// is absolute (`scheme://authority/path?query`); the foreign transport hands
/// it to the platform client verbatim (so `https` works there), while
/// [`StdNetTransport`] parses the authority out of it.
pub struct TransportRequest<'a, 'b> {
    pub method: &'a str,
    pub url: &'a str,
    pub headers: Vec<(String, String)>,
    /// Request body for `PUT` (streamed as it is read); `None` for GET/DELETE.
    /// `Send` so the foreign transport can pump it from a producer thread. Its
    /// borrow is a separate lifetime from the immutable parts: a `&mut` is
    /// invariant, so tying it to `url`'s lifetime would over-constrain callers.
    pub body: Option<&'b mut (dyn Read + Send)>,
    /// `true` only for `/stream`: the body is long-lived, so the transport
    /// ticks and yields [`BodyRead::Idle`] on each idle interval instead of
    /// treating a quiet connection as a stall. `false` bodies never yield
    /// `Idle` — a read that makes no progress within the transport's timeout
    /// is an error.
    pub long_lived: bool,
    pub cancel: Cancel,
}

/// The response head plus an incrementally-read body.
pub struct TransportResponse {
    pub status: u16,
    /// Lowercased header name/value pairs, in received order.
    pub headers: Vec<(String, String)>,
    pub body: Box<dyn TransportBody>,
}

/// The outcome of one [`TransportBody::read`].
pub enum BodyRead {
    /// `n > 0` decoded body bytes were written to the buffer.
    Bytes(usize),
    /// No bytes right now, but the connection is believed alive: an idle tick
    /// on a `long_lived` body. Never produced for a non-`long_lived` body.
    Idle,
    /// The body is complete.
    Eof,
}

/// A response body read incrementally. Errors are raw [`std::io::Error`]s so
/// the protocol layer can apply its own taxonomy (a malformed chunk is a
/// protocol error; a reset connection is transport) — see
/// [`super::client`]. Cancellation is the caller's to check between reads.
pub trait TransportBody: Send {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<BodyRead>;
}

/// A blocking HTTP request executor. One instance may be shared (behind an
/// `Arc`) by every [`HttpReplicaClient`](super::HttpReplicaClient) pointed at
/// the same host, so they share whatever connection pooling the transport
/// provides.
pub trait HttpTransport: Send + Sync {
    fn execute(&self, req: TransportRequest<'_, '_>) -> Result<TransportResponse>;
}

/// A [`Read`] over a [`TransportBody`] that presents an idle tick as a
/// `WouldBlock` [`std::io::Error`] and end-of-body as `Ok(0)`. This lets every
/// byte/line reader that already copes with a socket read-timeout (via
/// [`is_timeout`]) treat a transport idle tick exactly like the old timeout:
/// short bodies (which never tick) read straight through, and the long-lived
/// `/stream` reader maps the `WouldBlock` onto its `StreamEvent::Idle` +
/// dead-man logic unchanged.
pub struct BodyReader(pub Box<dyn TransportBody>);

impl Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.0.read(buf)? {
            BodyRead::Bytes(n) => Ok(n),
            BodyRead::Eof => Ok(0),
            BodyRead::Idle => Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "idle tick")),
        }
    }
}

// ---------------------------------------------------------------------------
// StdNetTransport: the zero-dependency default (one Connection: close socket
// per request). This is the v1 wire behavior, lifted verbatim out of the old
// HttpReplicaClient.

/// The default, zero-dependency transport: a hand-rolled HTTP/1.1 `GET`/`PUT`/
/// `DELETE` client over `std::net::TcpStream`, one `Connection: close` socket
/// per request. `http://` only — there is no TLS here; terminate TLS with a
/// reverse proxy, or inject a foreign transport that owns TLS.
#[derive(Clone, Debug)]
pub struct StdNetTransport {
    /// TCP connect timeout, per resolved address.
    connect_timeout: Duration,
    /// Socket read timeout for non-stream bodies, and the cumulative
    /// zero-progress budget for socket writes.
    io_timeout: Duration,
}

impl StdNetTransport {
    pub fn new(connect_timeout: Duration, io_timeout: Duration) -> StdNetTransport {
        StdNetTransport { connect_timeout, io_timeout }
    }

    fn connect(&self, host: &str, port: u16, cancel: &Cancel) -> Result<TcpStream> {
        cancel.check()?;
        let addrs: Vec<_> = (host, port)
            .to_socket_addrs()
            .map_err(|e| StorageError::Unavailable(format!("resolve {host}:{port}: {e}")))?
            .collect();
        self.try_addrs(host, port, addrs, cancel)
    }

    /// Attempts each resolved address in turn. The token is checked before
    /// every attempt: a multi-address host (dual-stack, multi-homed) whose
    /// SYNs are all blackholed must not multiply worst-case cancellation
    /// latency to `addrs.len() x connect_timeout` — the bound stays a single
    /// `connect_timeout`.
    fn try_addrs(
        &self,
        host: &str,
        port: u16,
        addrs: Vec<SocketAddr>,
        cancel: &Cancel,
    ) -> Result<TcpStream> {
        let mut last_err = None;
        for addr in addrs {
            cancel.check()?;
            match TcpStream::connect_timeout(&addr, self.connect_timeout) {
                Ok(s) => {
                    // A cancel during the connect itself resolves within
                    // connect_timeout; that granularity is accepted.
                    cancel.check()?;
                    s.set_nodelay(true)?;
                    s.set_read_timeout(Some(self.io_timeout))?;
                    s.set_write_timeout(Some(self.io_timeout.min(WRITE_TICK)))?;
                    return Ok(s);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(match last_err {
            Some(e) => StorageError::Unavailable(format!("connect {host}:{port}: {e}")),
            None => StorageError::Unavailable(format!("no addresses for {host}:{port}")),
        })
    }
}

impl HttpTransport for StdNetTransport {
    fn execute(&self, req: TransportRequest<'_, '_>) -> Result<TransportResponse> {
        let (host, port, target) = parse_http_url(req.url)?;
        let stream = self.connect(&host, port, &req.cancel)?;

        let mut head = format!("{} {target} HTTP/1.1\r\nhost: {host}:{port}\r\n", req.method);
        for (name, value) in &req.headers {
            head.push_str(name);
            head.push_str(": ");
            head.push_str(value);
            head.push_str("\r\n");
        }

        if let Some(body) = req.body {
            head.push_str("transfer-encoding: chunked\r\nconnection: close\r\n\r\n");
            // The chunk framing is inlined rather than using `ChunkedWriter`:
            // retried writes are only sound around a single raw `write` call
            // whose consumed count is tracked (see `write_full`), and the token
            // must be checked per chunk.
            let upload = (|| -> Result<()> {
                write_full(&stream, head.as_bytes(), &req.cancel, self.io_timeout)?;
                let mut buf = vec![0u8; 64 << 10];
                loop {
                    req.cancel.check()?;
                    let n = match body.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => n,
                        // The body source is local (writer's file, compactor
                        // spool): its failures stay Io, not Unavailable.
                        Err(e) => return Err(e.into()),
                    };
                    write_full(&stream, format!("{n:x}\r\n").as_bytes(), &req.cancel, self.io_timeout)?;
                    write_full(&stream, &buf[..n], &req.cancel, self.io_timeout)?;
                    write_full(&stream, b"\r\n", &req.cancel, self.io_timeout)?;
                }
                write_full(&stream, b"0\r\n\r\n", &req.cancel, self.io_timeout)?;
                Ok(())
            })();
            if let Err(upload_err) = upload {
                // Cancellation must return promptly — never wait out a response
                // read.
                if matches!(upload_err, StorageError::Cancelled) {
                    return Err(upload_err);
                }
                // The server may have rejected early (401/403/409, 400 bad
                // file) and stopped reading; its response usually still
                // arrived. Prefer that diagnostic over a raw broken-pipe error.
                if let Ok(resp) = read_response(stream, req.long_lived, &req.cancel) {
                    return Ok(resp);
                }
                return Err(upload_err);
            }
        } else {
            head.push_str("connection: close\r\n\r\n");
            write_full(&stream, head.as_bytes(), &req.cancel, self.io_timeout)?;
        }

        read_response(stream, req.long_lived, &req.cancel)
    }
}

/// Reads and parses a response head off `stream`, then wraps the socket into a
/// [`TransportBody`] framed per the response headers. For a `long_lived` body
/// the read timeout is lowered to [`STREAM_TICK`] so each quiet second becomes
/// a [`BodyRead::Idle`] tick.
fn read_response(
    mut stream: TcpStream,
    long_lived: bool,
    cancel: &Cancel,
) -> Result<TransportResponse> {
    let (status_line, headers) = match read_head(&mut stream) {
        Ok(head) => head,
        Err(e) => {
            // A cancel that lands during a blocked head read surfaces as the
            // read's timeout error; report Cancelled, not Unavailable.
            cancel.check()?;
            return Err(StorageError::Unavailable(format!("read response: {e}")));
        }
    };
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| StorageError::Other(format!("bad status line {status_line:?}")))?;

    if long_lived {
        stream.set_read_timeout(Some(STREAM_TICK))?;
    }
    let reader = body_reader(&headers, stream);
    Ok(TransportResponse {
        status,
        headers,
        body: Box::new(StdNetBody { reader, long_lived }),
    })
}

/// Frames the socket into a decoded byte reader per the response headers:
/// chunked, content-length bounded, or close-delimited.
fn body_reader(headers: &[(String, String)], stream: TcpStream) -> Box<dyn Read + Send> {
    if super::wire::header(headers, "transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
    {
        Box::new(ChunkedReader::new(stream))
    } else if let Some(n) =
        super::wire::header(headers, "content-length").and_then(|v| v.parse::<u64>().ok())
    {
        Box::new(stream.take(n))
    } else {
        // Connection: close delimited.
        Box::new(stream)
    }
}

struct StdNetBody {
    reader: Box<dyn Read + Send>,
    long_lived: bool,
}

impl TransportBody for StdNetBody {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<BodyRead> {
        match self.reader.read(buf) {
            Ok(0) => Ok(BodyRead::Eof),
            Ok(n) => Ok(BodyRead::Bytes(n)),
            // A quiet long-lived body is an idle tick, not a stall; the
            // protocol layer owns the dead-man bound. A quiet non-long-lived
            // body is a real timeout — propagate it so the caller maps it to
            // `Unavailable`.
            Err(e) if is_timeout(&e) && self.long_lived => Ok(BodyRead::Idle),
            Err(e) => Err(e),
        }
    }
}

/// Splits an absolute `http://host[:port]/path?query` into `(host, port,
/// target)` where `target` is `/path?query` (at least `/`). Supports `[v6]`
/// bracketed authorities. `https` is rejected — [`StdNetTransport`] has no TLS.
fn parse_http_url(url: &str) -> Result<(String, u16, String)> {
    let rest = url.strip_prefix("http://").ok_or_else(|| {
        if url.starts_with("https://") {
            StorageError::Other(
                "https is not supported by the built-in transport; terminate TLS with a \
                 reverse proxy, or inject a transport that owns TLS"
                    .into(),
            )
        } else {
            StorageError::Other(format!("invalid liters url {url:?}: expected http://host:port[/path]"))
        }
    })?;

    let (authority, target) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };

    let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
        let (host, rest) = v6
            .split_once(']')
            .ok_or_else(|| StorageError::Other(format!("bad ipv6 authority {authority:?}")))?;
        let port = match rest {
            "" => 80,
            rest => rest
                .strip_prefix(':')
                .and_then(|p| p.parse::<u16>().ok())
                .ok_or_else(|| StorageError::Other(format!("bad port in {authority:?}")))?,
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
    Ok((host, port, target))
}

/// Writes all of `buf` to the socket, checking the cancellation token on every
/// zero-progress send-timeout tick (the socket's write timeout is `WRITE_TICK`)
/// and giving up with `Unavailable` after `io_timeout` of cumulative zero
/// progress. Any progress resets the stall budget, so a slow-but-flowing uplink
/// is never killed.
///
/// Soundness of retrying the same slice after a timeout: with SO_SNDTIMEO on a
/// *blocking* TCP socket, a `write` call that errors with `WouldBlock`/
/// `TimedOut` has transferred **zero** bytes of that call — partial progress is
/// returned as `Ok(n)` instead. Linux implements this in `tcp_sendmsg` (on a
/// send-buffer wait timeout it returns the copied count if any bytes were
/// queued, and errors only at zero progress); macOS/XNU's `dofilewrite`
/// converts EWOULDBLOCK-with-partial-uio-progress into a partial-count success.
/// Verified empirically on macOS (loopback pair, unread peer, 200ms SO_SNDTIMEO,
/// 4KiB–32MiB writes: bytes received always equaled the sum of `Ok(n)`
/// returns). This holds ONLY per raw `write` call — `write_all`'s internal
/// progress is invisible after an error — which is why this loop tracks its own
/// offset and nothing here wraps `write_all`.
fn write_full(
    stream: &TcpStream,
    buf: &[u8],
    cancel: &Cancel,
    io_timeout: Duration,
) -> Result<()> {
    let mut w: &TcpStream = stream;
    let mut pos = 0;
    let mut stalled = Instant::now();
    while pos < buf.len() {
        cancel.check()?;
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
                cancel.check()?;
                return Err(StorageError::Unavailable(format!("write: {e}")));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_parsing() {
        assert_eq!(
            parse_http_url("http://example.com:8080/some/base/ltx/0?seek=1").unwrap(),
            ("example.com".into(), 8080, "/some/base/ltx/0?seek=1".into())
        );
        assert_eq!(
            parse_http_url("http://10.0.0.1:9999").unwrap(),
            ("10.0.0.1".into(), 9999, "/".into())
        );
        assert_eq!(parse_http_url("http://[::1]:8080/x").unwrap(), ("::1".into(), 8080, "/x".into()));
        assert_eq!(parse_http_url("http://localhost/").unwrap(), ("localhost".into(), 80, "/".into()));

        assert!(parse_http_url("https://example.com").is_err());
        assert!(parse_http_url("file:///tmp/x").is_err());
        assert!(parse_http_url("http://host:notaport").is_err());
    }

    /// The address sweep must observe the cancellation token *between*
    /// attempts, not only on entry and after a successful connect — otherwise
    /// a multi-address blackholed host stretches cancellation latency to
    /// addrs.len() x connect_timeout.
    #[test]
    fn connect_sweep_checks_cancel_between_addresses() {
        // A just-freed loopback port: connect attempts fail fast (refused).
        let refused = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            addr
        };
        let transport = StdNetTransport::new(Duration::from_secs(10), Duration::from_secs(30));

        // Live token: the sweep runs to exhaustion and reports transport
        // failure.
        let cell = Arc::new(Mutex::new(CancelToken::new()));
        let cancel = Cancel::new(cell.clone());
        let err = transport.try_addrs("127.0.0.1", 1, vec![refused, refused], &cancel).unwrap_err();
        assert!(matches!(err, StorageError::Unavailable(_)), "live token: {err:?}");

        // Cancelled token: the sweep must return Cancelled instead of dialing
        // through the address list.
        cell.lock().unwrap().cancel();
        let err = transport.try_addrs("127.0.0.1", 1, vec![refused, refused], &cancel).unwrap_err();
        assert!(matches!(err, StorageError::Cancelled), "cancelled token: {err:?}");
    }
}
