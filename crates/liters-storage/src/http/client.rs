//! HTTP replica client for a bucket served by a liters [`HttpServer`] (see
//! docs/http-protocol.md for the wire protocol). Reads always work; writes
//! (`write_ltx_file`/deletes — i.e. using this client as a `Writer`
//! destination to *push* replication to a listening liters) require the
//! server to run with `writable: true`, otherwise they fail with a clear
//! read-only error. Every request is its own `Connection: close` socket, so
//! the restore path's N concurrently open plan files are N sockets.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant, UNIX_EPOCH};

use ltx::{format_filename, parse_filename, FileInfo, Txid};

use crate::{LtxStream, ReplicaClient, Result, StorageError, StreamEvent};

use super::wire::{
    header, is_timeout, read_head, ChunkedReader, ChunkedWriter, LineBuf, PROTOCOL_HEADER,
    PROTOCOL_VERSION,
};

/// (status, headers, socket positioned at the body).
type Response = (u16, Vec<(String, String)>, TcpStream);

/// Socket read timeout while a stream is idle; each expiry surfaces as one
/// [`StreamEvent::Idle`] tick so callers can check their stop flag.
const STREAM_TICK: Duration = Duration::from_secs(1);
/// A stream that has produced no bytes at all (not even a ping) for this
/// long is declared dead. Must comfortably exceed the server's ping interval.
const STREAM_DEADMAN: Duration = Duration::from_secs(45);

/// Blocking client for a bucket served over HTTP by another liters instance.
///
/// `url` is `http://host:port[/base]` — https is not supported in v1; put a
/// reverse proxy in front for TLS/auth (and disable response buffering for
/// `/stream`, see docs/http-protocol.md).
pub struct HttpReplicaClient {
    host: String,
    port: u16,
    /// Path prefix, `""` or `/prefix` (no trailing slash).
    base: String,
    connect_timeout: Duration,
    read_timeout: Duration,
}

impl HttpReplicaClient {
    pub fn new(url: impl AsRef<str>) -> Result<HttpReplicaClient> {
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
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
        })
    }

    fn connect(&self) -> Result<TcpStream> {
        let addrs: Vec<_> = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|e| StorageError::Other(format!("resolve {}:{}: {e}", self.host, self.port)))?
            .collect();
        let mut last_err = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, self.connect_timeout) {
                Ok(s) => {
                    s.set_nodelay(true)?;
                    s.set_read_timeout(Some(self.read_timeout))?;
                    s.set_write_timeout(Some(self.read_timeout))?;
                    return Ok(s);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(match last_err {
            Some(e) => StorageError::Io(e),
            None => StorageError::Other(format!("no addresses for {}:{}", self.host, self.port)),
        })
    }

    /// One bodyless request (GET/DELETE): connect, send, parse the response
    /// head, and validate the liters protocol header — every response must
    /// carry it, so proxies and foreign servers fail loudly rather than
    /// being misparsed.
    fn request(&self, method: &str, target: &str) -> Result<Response> {
        let mut stream = self.connect()?;
        write!(
            stream,
            "{method} {target} HTTP/1.1\r\nhost: {}:{}\r\nconnection: close\r\n\r\n",
            self.host, self.port
        )?;
        stream.flush()?;
        self.read_response(target, stream)
    }

    fn get(&self, target: &str) -> Result<Response> {
        self.request("GET", target)
    }

    /// One PUT streaming `body` as a chunked request body (its length is
    /// unknown — it comes straight off the writer's local file or the
    /// compactor's pipe).
    fn put(&self, target: &str, body: &mut dyn Read) -> Result<Response> {
        let mut stream = self.connect()?;
        write!(
            stream,
            "PUT {target} HTTP/1.1\r\nhost: {}:{}\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
            self.host, self.port
        )?;
        let upload = (|| -> Result<()> {
            let mut out = ChunkedWriter::new(&mut stream);
            let mut buf = vec![0u8; 64 << 10];
            loop {
                match body.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => out.chunk(&buf[..n])?,
                    Err(e) => return Err(e.into()),
                }
            }
            out.finish()?;
            Ok(())
        })();
        if let Err(upload_err) = upload {
            // The server may have rejected early (403 read-only, 400 bad
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
        let (status_line, headers) = read_head(&mut stream)?;
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
        let (status, headers, stream) = self.get(&target)?;
        if status != 200 {
            return Err(StorageError::Other(format!("list level {level}: http status {status}")));
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
        let mut body = String::new();
        Self::body_reader(&headers, stream).read_to_string(&mut body)?;
        if body.len() as u64 != declared {
            return Err(StorageError::Other(format!(
                "list level {level}: truncated listing ({} of {declared} bytes)",
                body.len()
            )));
        }

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
                let spool = spool_body(Self::body_reader(&headers, stream))?;
                Ok(Box::new(spool))
            }
            200 => Ok(Self::body_reader(&headers, stream)),
            404 => Err(StorageError::NotFound { level, min_txid, max_txid }),
            _ => Err(StorageError::Other(format!(
                "open L{level} {min_txid}-{max_txid}: http status {status}"
            ))),
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
        let (status, headers, stream) = self.put(&target, rd)?;
        match status {
            200 => {
                let mut body = String::new();
                Self::body_reader(&headers, stream).take(4096).read_to_string(&mut body)?;
                parse_listing_line(body.lines().next().unwrap_or(""), level).ok_or_else(|| {
                    StorageError::Other(format!("push L{level}: bad put response {body:?}"))
                })
            }
            403 => Err(StorageError::Other(format!(
                "push L{level} {min_txid}-{max_txid}: server is read-only{}",
                Self::error_detail(&headers, stream)
            ))),
            _ => Err(StorageError::Other(format!(
                "push L{level} {min_txid}-{max_txid}: http status {status}{}",
                Self::error_detail(&headers, stream)
            ))),
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
                return Err(StorageError::Other(format!(
                    "delete L{} {}-{}: http status {status}{}",
                    info.level,
                    info.min_txid,
                    info.max_txid,
                    Self::error_detail(&headers, stream)
                )));
            }
        }
        Ok(())
    }

    fn delete_all(&self) -> Result<()> {
        let (status, headers, stream) = self.request("DELETE", &format!("{}/all", self.base))?;
        if status != 200 {
            return Err(StorageError::Other(format!(
                "delete all: http status {status}{}",
                Self::error_detail(&headers, stream)
            )));
        }
        Ok(())
    }

    fn open_ltx_stream(&self, seek: Txid) -> Result<Option<Box<dyn LtxStream>>> {
        let (status, _headers, stream) = self.get(&format!("{}/stream?seek={}", self.base, seek))?;
        if status != 200 {
            return Err(StorageError::Other(format!("open stream: http status {status}")));
        }
        // Short read timeout from here on: each expiry becomes one Idle tick
        // (at frame boundaries) so followers stay stop-responsive.
        stream.set_read_timeout(Some(STREAM_TICK))?;
        Ok(Some(Box::new(HttpLtxStream {
            body: ChunkedReader::new(stream),
            line: LineBuf::default(),
            state: HttpStreamState::Preamble,
            last_byte: Instant::now(),
        })))
    }
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
fn spool_body(mut body: Box<dyn Read + Send>) -> Result<std::fs::File> {
    use std::io::{Seek, SeekFrom};

    let dir = std::env::temp_dir();
    let mut file = None;
    for attempt in 0u32..16 {
        let nanos = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let path = dir.join(format!(
            "liters-http-{}-{nanos:x}-{attempt}.spool",
            std::process::id()
        ));
        match std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&path) {
            Ok(f) => {
                // Unlink immediately; the open fd keeps it readable (unix).
                let _ = std::fs::remove_file(&path);
                file = Some(f);
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    }
    let mut file =
        file.ok_or_else(|| StorageError::Other("could not create spool file".into()))?;
    std::io::copy(&mut body, &mut file)?;
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
}

impl HttpLtxStream {
    fn deadman(&self) -> Result<()> {
        if self.last_byte.elapsed() >= STREAM_DEADMAN {
            return Err(StorageError::Other(format!(
                "ltx stream stalled: no data for {}s",
                STREAM_DEADMAN.as_secs()
            )));
        }
        Ok(())
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
                        Err(e) => return Err(e.into()),
                    };
                    self.last_byte = Instant::now();
                    let Some(line) = line else {
                        // Clean chunked EOF.
                        return match self.state {
                            HttpStreamState::Frame => Ok(StreamEvent::Closed),
                            _ => Err(StorageError::Other(
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
                            return Err(StorageError::Other("stream ended mid-frame".into()))
                        }
                        Ok(n) => {
                            self.last_byte = Instant::now();
                            sink.write_all(&buf[..n])?;
                            *remaining -= n as u64;
                        }
                        // Mid-body timeouts are absorbed (never Idle: the
                        // caller may truncate its sink between next() calls);
                        // the dead-man timer is the stall bound.
                        Err(e) if is_timeout(&e) => self.deadman()?,
                        Err(e) => return Err(e.into()),
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
}
