//! Minimal HTTP/1.1 wire primitives shared by the liters client and server.
//! Deliberately tiny: `GET` plus the write endpoints' `PUT`/`DELETE`,
//! `Connection: close`, chunked or content-length bodies, no
//! percent-encoding (the protocol's paths and query values are plain
//! hex/decimal — see docs/http-protocol.md).

use std::io::{self, Read, Write};

/// Protocol version spoken by this build. Any frame-type or format change
/// bumps this.
pub const PROTOCOL_VERSION: &str = "1";
/// Header carried on every liters response; clients validate it on every
/// response so foreign servers (proxies, wrong ports) fail loudly instead of
/// being misparsed.
pub const PROTOCOL_HEADER: &str = "x-liters-protocol";

/// Max length of any protocol line (request line, header, listing line,
/// stream frame line, chunk-size line).
pub const MAX_LINE: usize = 8192;
/// Max number of request/response header lines.
pub const MAX_HEADERS: usize = 100;

pub fn is_timeout(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
}

/// Byte-at-a-time line accumulator. Reading byte-wise keeps the consumer
/// positioned exactly at the start of what follows the line (no over-read
/// buffering), and the partial line survives read timeouts so streaming
/// callers can resume mid-line.
#[derive(Default)]
pub struct LineBuf {
    buf: Vec<u8>,
}

impl LineBuf {
    /// Reads until `\n`. `Ok(Some(line))` strips the trailing `\n`/`\r\n`.
    /// `Ok(None)` = clean EOF at a line boundary; EOF mid-line is an error.
    /// Timeouts propagate as errors with the partial line preserved.
    pub fn read_line(&mut self, r: &mut dyn Read) -> io::Result<Option<String>> {
        let mut byte = [0u8; 1];
        loop {
            match r.read(&mut byte) {
                Ok(0) => {
                    if self.buf.is_empty() {
                        return Ok(None);
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "eof mid-line",
                    ));
                }
                Ok(_) => {
                    if byte[0] == b'\n' {
                        let mut line = std::mem::take(&mut self.buf);
                        if line.last() == Some(&b'\r') {
                            line.pop();
                        }
                        return String::from_utf8(line).map(Some).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "non-utf8 line")
                        });
                    }
                    if self.buf.len() >= MAX_LINE {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "line exceeds max length",
                        ));
                    }
                    self.buf.push(byte[0]);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Reads a request/response head: the start line plus header lines up to the
/// blank separator. Returns (start_line, headers as lowercase-name pairs).
pub fn read_head(r: &mut dyn Read) -> io::Result<(String, Vec<(String, String)>)> {
    let mut lines = LineBuf::default();
    let start = match lines.read_line(r)? {
        Some(l) if !l.is_empty() => l,
        _ => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "empty request head")),
    };
    let mut headers = Vec::new();
    loop {
        let line = lines
            .read_line(r)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "eof in headers"))?;
        if line.is_empty() {
            return Ok((start, headers));
        }
        if headers.len() >= MAX_HEADERS {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "too many headers"));
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
        // Malformed header lines are skipped, mirroring the listing parser's
        // skip-strays stance.
    }
}

pub fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
}

/// `k=v&k=v` query parsing; no percent-decoding by design.
pub fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
}

/// Chunked transfer-encoding writer. Each `chunk()` is flushed by the caller
/// when timeliness matters (stream frames, pings).
pub struct ChunkedWriter<W: Write> {
    inner: W,
}

impl<W: Write> ChunkedWriter<W> {
    pub fn new(inner: W) -> ChunkedWriter<W> {
        ChunkedWriter { inner }
    }

    pub fn chunk(&mut self, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(()); // an empty chunk would terminate the body
        }
        write!(self.inner, "{:x}\r\n", data.len())?;
        self.inner.write_all(data)?;
        self.inner.write_all(b"\r\n")
    }

    /// Terminates the body cleanly. Dropping without `finish` leaves the
    /// peer with an unexpected EOF — that is the deliberate abort signal.
    pub fn finish(mut self) -> io::Result<()> {
        self.inner.write_all(b"0\r\n\r\n")?;
        self.inner.flush()
    }
}

/// Each `write` emits one chunk, so a `StreamBody` (or any producer) can
/// target `&mut dyn Write` and have its writes framed. `flush` reaches the
/// wrapped writer; `finish` is still explicit (call it on the concrete type,
/// or drop to abort).
impl<W: Write> Write for ChunkedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.chunk(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

enum ChunkState {
    /// Next: a chunk-size line.
    Size,
    /// Mid-chunk with this many data bytes left.
    Data(u64),
    /// Chunk data done; the trailing CRLF still unread.
    DataEnd,
    /// Final 0-chunk seen; trailer lines until the blank line.
    Trailer,
    Eof,
}

/// Chunked transfer-encoding reader. All state advances only on successful
/// reads, so a read timeout at any byte position is resumable: propagate the
/// error, call `read` again later.
pub struct ChunkedReader<R: Read> {
    inner: R,
    state: ChunkState,
    line: LineBuf,
}

impl<R: Read> ChunkedReader<R> {
    pub fn new(inner: R) -> ChunkedReader<R> {
        ChunkedReader { inner, state: ChunkState::Size, line: LineBuf::default() }
    }
}

impl<R: Read> Read for ChunkedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.state {
                ChunkState::Size => {
                    let line = self.line.read_line(&mut self.inner)?.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "eof before chunk size")
                    })?;
                    let size_str = line.split(';').next().unwrap_or("").trim();
                    let size = u64::from_str_radix(size_str, 16).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "bad chunk size")
                    })?;
                    self.state = if size == 0 { ChunkState::Trailer } else { ChunkState::Data(size) };
                }
                ChunkState::Data(remaining) => {
                    if buf.is_empty() {
                        return Ok(0);
                    }
                    let want = buf.len().min(remaining.min(usize::MAX as u64) as usize);
                    let n = self.inner.read(&mut buf[..want])?;
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "eof mid-chunk",
                        ));
                    }
                    self.state = if remaining - n as u64 == 0 {
                        ChunkState::DataEnd
                    } else {
                        ChunkState::Data(remaining - n as u64)
                    };
                    return Ok(n);
                }
                ChunkState::DataEnd => {
                    let line = self.line.read_line(&mut self.inner)?.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "eof after chunk")
                    })?;
                    if !line.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "missing chunk terminator",
                        ));
                    }
                    self.state = ChunkState::Size;
                }
                ChunkState::Trailer => {
                    let line = self.line.read_line(&mut self.inner)?.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "eof in trailer")
                    })?;
                    if line.is_empty() {
                        self.state = ChunkState::Eof;
                    }
                }
                ChunkState::Eof => return Ok(0),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked_roundtrip() {
        let mut out = Vec::new();
        {
            let mut w = ChunkedWriter::new(&mut out);
            w.chunk(b"hello ").unwrap();
            w.chunk(b"world").unwrap();
            w.finish().unwrap();
        }
        let mut r = ChunkedReader::new(&out[..]);
        let mut s = String::new();
        r.read_to_string(&mut s).unwrap();
        assert_eq!(s, "hello world");
    }

    #[test]
    fn chunked_rejects_garbage_size() {
        let mut r = ChunkedReader::new(&b"zz\r\ndata"[..]);
        let mut s = String::new();
        assert!(r.read_to_string(&mut s).is_err());
    }

    #[test]
    fn line_buf_strips_crlf_and_caps_length() {
        let mut lb = LineBuf::default();
        assert_eq!(lb.read_line(&mut &b"abc\r\n"[..]).unwrap(), Some("abc".into()));
        let long = vec![b'a'; MAX_LINE + 1];
        let mut lb = LineBuf::default();
        assert!(lb.read_line(&mut &long[..]).is_err());
    }

    #[test]
    fn query_param_basics() {
        assert_eq!(query_param("seek=00000000000000ff&meta=1", "seek"), Some("00000000000000ff"));
        assert_eq!(query_param("seek=1", "meta"), None);
    }
}
