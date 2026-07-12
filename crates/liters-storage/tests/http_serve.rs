//! Storage-level integration tests for the liters HTTP replication protocol
//! (docs/http-protocol.md): an `HttpServer` fronting a `DirReplicaClient`
//! bucket, read and followed by `HttpReplicaClient`, plus raw-socket
//! protocol-defense cases against a canned non-liters server.
#![cfg(feature = "http")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use liters_storage::{
    DirReplicaClient, HttpReplicaClient, HttpServer, HttpServerOptions, LtxStream, ReplicaClient,
    StorageError, StreamEvent,
};
use ltx::{format_filename, Txid};

/// Small intervals so no test waits on the 1s/15s production defaults.
fn test_opts() -> HttpServerOptions {
    HttpServerOptions {
        poll_interval: Duration::from_millis(50),
        ping_interval: Duration::from_millis(200),
        ..HttpServerOptions::default()
    }
}

/// Generous bound for eventual conditions; never a latency assertion.
const DEADLINE: Duration = Duration::from_secs(30);

/// Seeds `{root}/ltx/{level}/{min:016x}-{max:016x}.ltx` with arbitrary bytes.
/// Write-then-rename so a concurrently polling server never lists a
/// half-written size.
fn seed_file(root: &Path, level: u8, min: u64, max: u64, bytes: &[u8]) {
    let dir = root.join("ltx").join(level.to_string());
    std::fs::create_dir_all(&dir).unwrap();
    let name = format_filename(Txid(min), Txid(max));
    let tmp = dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, bytes).unwrap();
    std::fs::rename(&tmp, dir.join(name)).unwrap();
}

/// Deterministic, seed-distinguishable filler bytes.
fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

/// Serves `root` on a loopback port and returns a client pointed at it.
fn serve(root: &Path) -> (HttpServer, HttpReplicaClient) {
    let srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(root)), test_opts())
            .unwrap();
    let client = HttpReplicaClient::new(format!("http://{}", srv.local_addr())).unwrap();
    (srv, client)
}

fn read_all(mut r: Box<dyn Read + Send>) -> Vec<u8> {
    let mut buf = Vec::new();
    r.read_to_end(&mut buf).unwrap();
    buf
}

/// The server's listing timestamps are milliseconds since the epoch, so the
/// HTTP client's `created_at` is the dir client's value truncated to ms.
fn trunc_ms(t: SystemTime) -> SystemTime {
    let d = t.duration_since(UNIX_EPOCH).unwrap();
    UNIX_EPOCH + Duration::from_millis(d.as_millis() as u64)
}

/// Pumps the stream past Idle ticks until a substantive event arrives.
/// The sink is cleared before every call, so on return it holds exactly the
/// returned event's bytes.
fn next_non_idle(s: &mut dyn LtxStream, sink: &mut Vec<u8>) -> StreamEvent {
    let start = Instant::now();
    loop {
        sink.clear();
        match s.next(sink).unwrap() {
            StreamEvent::Idle { .. } => {
                assert!(start.elapsed() < DEADLINE, "no non-idle stream event within {DEADLINE:?}")
            }
            ev => return ev,
        }
    }
}

/// One-shot canned HTTP server: accepts a single connection, drains the
/// request head, writes `response` verbatim, and closes. Join the handle
/// after the client call completes.
fn canned_server(response: Vec<u8>) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        // Drain the request head so the eventual close is a clean FIN.
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") && head.len() < 8192 {
            match stream.read(&mut byte) {
                Ok(0) | Err(_) => break,
                Ok(_) => head.push(byte[0]),
            }
        }
        stream.write_all(&response).unwrap();
        let _ = stream.flush();
        // Drop closes the socket; buffered bytes still reach the peer.
    });
    (url, handle)
}

/// One chunk in HTTP chunked transfer encoding.
fn chunk(data: &[u8]) -> Vec<u8> {
    let mut out = format!("{:x}\r\n", data.len()).into_bytes();
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

// ---------------------------------------------------------------------------

#[test]
fn listing_matches_dir_backend() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    seed_file(root, 0, 1, 1, &pattern(1, 100));
    seed_file(root, 0, 2, 2, &pattern(2, 257));
    seed_file(root, 0, 3, 4, &pattern(3, 7));
    seed_file(root, 1, 1, 4, &pattern(4, 1024));
    seed_file(root, 9, 1, 4, &pattern(5, 4096));

    let dir = DirReplicaClient::new(root);
    let (mut srv, http) = serve(root);

    for (level, seek) in [(0u8, 0u64), (0, 2), (1, 0), (9, 0), (0, 99)] {
        let want = dir.ltx_files(level, Txid(seek), false).unwrap();
        let got = http.ltx_files(level, Txid(seek), false).unwrap();
        assert_eq!(got.len(), want.len(), "level {level} seek {seek}");
        for (g, w) in got.iter().zip(&want) {
            assert_eq!(
                (g.level, g.min_txid, g.max_txid, g.size),
                (w.level, w.min_txid, w.max_txid, w.size),
                "level {level} seek {seek}"
            );
            // http timestamps are the dir mtimes truncated to milliseconds.
            assert_eq!(g.created_at, w.created_at.map(trunc_ms), "level {level} seek {seek}");
        }
    }

    // Seek-filter shape, independent of the dir client.
    let seeked = http.ltx_files(0, Txid(2), false).unwrap();
    assert_eq!(
        seeked.iter().map(|f| (f.min_txid.0, f.max_txid.0)).collect::<Vec<_>>(),
        vec![(2, 2), (3, 4)]
    );

    // A missing/empty level is Ok(empty), never an error.
    assert!(http.ltx_files(5, Txid(0), false).unwrap().is_empty());
    assert!(dir.ltx_files(5, Txid(0), false).unwrap().is_empty());

    // meta=1 path (use_metadata) serves the same listing.
    assert_eq!(http.ltx_files(0, Txid(0), true).unwrap().len(), 3);

    srv.shutdown();
}

#[test]
fn file_reads_match_dir_backend() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let body = pattern(7, 3000);
    seed_file(root, 0, 1, 1, &body);

    let dir = DirReplicaClient::new(root);
    let (mut srv, http) = serve(root);

    // (offset, size): whole file, offset-only, offset+size, offset past EOF.
    for (offset, size) in [(0u64, 0u64), (100, 0), (100, 500), (10_000, 0)] {
        let want = read_all(dir.open_ltx_file(0, Txid(1), Txid(1), offset, size).unwrap());
        let got = read_all(http.open_ltx_file(0, Txid(1), Txid(1), offset, size).unwrap());
        assert_eq!(got, want, "offset {offset} size {size}");
    }

    // Shape assertions on top of dir-equality.
    assert_eq!(read_all(http.open_ltx_file(0, Txid(1), Txid(1), 0, 0).unwrap()), body);
    assert_eq!(
        read_all(http.open_ltx_file(0, Txid(1), Txid(1), 100, 500).unwrap()),
        &body[100..600]
    );
    // Offset beyond EOF: dir semantics are an empty read, not an error.
    assert!(read_all(http.open_ltx_file(0, Txid(1), Txid(1), 10_000, 0).unwrap()).is_empty());

    srv.shutdown();
}

#[test]
fn missing_file_maps_to_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    seed_file(tmp.path(), 0, 1, 1, b"x"); // non-empty bucket; the file below is absent
    let (mut srv, http) = serve(tmp.path());

    match http.open_ltx_file(3, Txid(0x0a), Txid(0x0b), 0, 0) {
        Err(StorageError::NotFound { level, min_txid, max_txid }) => {
            assert_eq!((level, min_txid, max_txid), (3, Txid(0x0a), Txid(0x0b)));
        }
        other => panic!("expected NotFound, got {:?}", other.map(|_| "reader")),
    }
    srv.shutdown();
}

// (write/delete against a read-only server are covered by
// read_only_server_rejects_writes below — the client's write half is real
// now that push replication exists.)

#[test]
fn stream_delivers_files_then_idles() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let bodies: Vec<Vec<u8>> = (1..=4u8).map(|i| pattern(i, 1000 + i as usize * 100)).collect();
    for txid in 1..=3u64 {
        seed_file(root, 0, txid, txid, &bodies[(txid - 1) as usize]);
    }
    let (mut srv, http) = serve(root);

    let mut stream = http.open_ltx_stream(Txid(1)).unwrap().expect("http backend streams");
    let mut sink = Vec::new();

    for txid in 1..=3u64 {
        match next_non_idle(stream.as_mut(), &mut sink) {
            StreamEvent::Ltx(info) => {
                assert_eq!(
                    (info.level, info.min_txid, info.max_txid),
                    (0, Txid(txid), Txid(txid))
                );
                assert_eq!(info.size, sink.len() as u64);
                assert_eq!(sink, bodies[(txid - 1) as usize], "body of txid {txid}");
            }
            other => panic!("expected Ltx {txid}, got {other:?}"),
        }
    }

    // A file written behind the server's back (no notify tee) is picked up
    // by the server's poll loop within the deadline.
    seed_file(root, 0, 4, 4, &bodies[3]);
    match next_non_idle(stream.as_mut(), &mut sink) {
        StreamEvent::Ltx(info) => {
            assert_eq!((info.min_txid, info.max_txid), (Txid(4), Txid(4)));
            assert_eq!(sink, bodies[3]);
        }
        other => panic!("expected Ltx 4, got {other:?}"),
    }

    // Quiet bucket: idle ticks keep flowing (bounded: three next() calls,
    // each capped by the client's ~1s tick).
    for _ in 0..3 {
        sink.clear();
        match stream.next(&mut sink).unwrap() {
            StreamEvent::Idle { .. } => {}
            other => panic!("expected Idle on quiet bucket, got {other:?}"),
        }
    }

    drop(stream);
    srv.shutdown();
}

#[test]
fn stream_gap_when_seek_pruned() {
    let tmp = tempfile::tempdir().unwrap();
    seed_file(tmp.path(), 0, 5, 5, &pattern(9, 64));
    let (mut srv, http) = serve(tmp.path());

    // Oldest L0 min is 5; seek 1 was pruned -> Gap with the next available min.
    let mut stream = http.open_ltx_stream(Txid(1)).unwrap().unwrap();
    let mut sink = Vec::new();
    match next_non_idle(stream.as_mut(), &mut sink) {
        StreamEvent::Gap { next } => assert_eq!(next, Txid(5)),
        other => panic!("expected Gap, got {other:?}"),
    }
    // After gap the server ends the stream cleanly: the next event is Closed.
    match next_non_idle(stream.as_mut(), &mut sink) {
        StreamEvent::Closed => {}
        other => panic!("expected Closed after Gap, got {other:?}"),
    }

    drop(stream);
    srv.shutdown();
}

#[test]
fn stream_reset_on_divergence() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    // Bucket max across levels is 3 (L0); the L1 file keeps the bucket
    // non-empty after the L0 deletions below.
    seed_file(root, 0, 1, 1, &pattern(1, 128));
    seed_file(root, 0, 2, 2, &pattern(2, 128));
    seed_file(root, 0, 3, 3, &pattern(3, 128));
    seed_file(root, 1, 1, 2, &pattern(4, 256));
    let (mut srv, http) = serve(root);
    let mut sink = Vec::new();

    // Divergent at open: seek far beyond bucket max -> immediate Reset.
    let mut diverged = http.open_ltx_stream(Txid(10)).unwrap().unwrap();
    match next_non_idle(diverged.as_mut(), &mut sink) {
        StreamEvent::Reset { bucket_max } => assert_eq!(bucket_max, Txid(3)),
        other => panic!("expected Reset at open, got {other:?}"),
    }
    drop(diverged);

    // Non-divergent seek: files flow, no Reset at open.
    let mut stream = http.open_ltx_stream(Txid(2)).unwrap().unwrap();
    for txid in 2..=3u64 {
        match next_non_idle(stream.as_mut(), &mut sink) {
            StreamEvent::Ltx(info) => {
                assert_eq!((info.min_txid, info.max_txid), (Txid(txid), Txid(txid)));
            }
            other => panic!("expected Ltx {txid}, got {other:?}"),
        }
    }

    // Drop the bucket max (now 2, from the L1 file) below the follower's
    // position (3): a subsequent ping tick surfaces Reset.
    let l0 = root.join("ltx").join("0");
    std::fs::remove_file(l0.join(format_filename(Txid(2), Txid(2)))).unwrap();
    std::fs::remove_file(l0.join(format_filename(Txid(3), Txid(3)))).unwrap();
    match next_non_idle(stream.as_mut(), &mut sink) {
        StreamEvent::Reset { bucket_max } => assert_eq!(bucket_max, Txid(2)),
        other => panic!("expected Reset after deletion, got {other:?}"),
    }

    drop(stream);
    srv.shutdown();
}

#[test]
fn idle_pings_carry_bucket_max() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    seed_file(root, 0, 1, 1, &pattern(1, 64));
    seed_file(root, 0, 2, 3, &pattern(2, 64));
    seed_file(root, 1, 1, 2, &pattern(3, 64));
    let (mut srv, http) = serve(root);

    // Caught up (position 3 == bucket max), so the stream only pings.
    // With a 200ms ping interval, three Some-pings land in well under a
    // second; the loop is bounded by the deadline either way.
    let mut stream = http.open_ltx_stream(Txid(4)).unwrap().unwrap();
    let mut sink = Vec::new();
    let start = Instant::now();
    let mut ping_maxes = Vec::new();
    while ping_maxes.len() < 3 {
        assert!(start.elapsed() < DEADLINE, "got only {ping_maxes:?} within {DEADLINE:?}");
        match stream.next(&mut sink).unwrap() {
            StreamEvent::Idle { bucket_max: Some(m) } => ping_maxes.push(m),
            StreamEvent::Idle { bucket_max: None } => {} // client-side tick
            other => panic!("unexpected event on idle stream: {other:?}"),
        }
    }
    assert!(ping_maxes.iter().all(|m| *m == Txid(3)), "pings carried {ping_maxes:?}");

    drop(stream);
    srv.shutdown();
}

#[test]
fn client_rejects_bad_protocol_responses() {
    // (a) No x-liters-protocol header: not a liters server.
    let (url, h) = canned_server(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".to_vec());
    let err = HttpReplicaClient::new(&url).unwrap().ltx_files(0, Txid(0), false).unwrap_err();
    assert!(err.to_string().contains("not a liters server"), "unexpected error: {err}");
    h.join().unwrap();

    // (b) Wrong protocol version.
    let (url, h) = canned_server(
        b"HTTP/1.1 200 OK\r\nx-liters-protocol: 2\r\ncontent-length: 0\r\n\r\n".to_vec(),
    );
    let err = HttpReplicaClient::new(&url).unwrap().ltx_files(0, Txid(0), false).unwrap_err();
    assert!(err.to_string().contains("protocol"), "unexpected error: {err}");
    h.join().unwrap();

    // (c) Malformed listing lines are skipped; valid ones parse.
    let body = "0000000000000001-0000000000000002.ltx 100 1783804999123\n\
                utter garbage\n\
                0000000000000005-0000000000000005.ltx notasize 5\n\
                badname.ltx 10 20\n\
                \n\
                0000000000000003-0000000000000003.ltx 50 -\n";
    let (url, h) = canned_server(
        format!(
            "HTTP/1.1 200 OK\r\nx-liters-protocol: 1\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes(),
    );
    let infos = HttpReplicaClient::new(&url).unwrap().ltx_files(0, Txid(0), false).unwrap();
    assert_eq!(infos.len(), 2, "parsed {infos:?}");
    assert_eq!(
        (infos[0].min_txid, infos[0].max_txid, infos[0].size),
        (Txid(1), Txid(2), 100)
    );
    assert_eq!(
        infos[0].created_at,
        Some(UNIX_EPOCH + Duration::from_millis(1_783_804_999_123))
    );
    assert_eq!(
        (infos[1].min_txid, infos[1].max_txid, infos[1].size, infos[1].created_at),
        (Txid(3), Txid(3), 50, None)
    );
    h.join().unwrap();

    // (d) Stream with a bad preamble: the first next() errors.
    let mut resp =
        b"HTTP/1.1 200 OK\r\nx-liters-protocol: 1\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
    resp.extend(chunk(b"bogus-preamble 1\n"));
    resp.extend_from_slice(b"0\r\n\r\n");
    let (url, h) = canned_server(resp);
    let client = HttpReplicaClient::new(&url).unwrap();
    let mut stream = client.open_ltx_stream(Txid(1)).unwrap().unwrap();
    let mut sink = Vec::new();
    let err = stream.next(&mut sink).unwrap_err();
    assert!(err.to_string().contains("preamble"), "unexpected error: {err}");
    drop(stream);
    h.join().unwrap();

    // (e) An ltx frame declaring 100 bytes but delivering 10, then the
    // connection closes: next() errors within the deadline, never hangs.
    let mut resp =
        b"HTTP/1.1 200 OK\r\nx-liters-protocol: 1\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
    resp.extend(chunk(b"liters-stream 1\n"));
    resp.extend(chunk(b"ltx 0 0000000000000001 0000000000000001 100\n"));
    resp.extend(chunk(&[0xab; 10])); // truncated body; no terminating 0-chunk
    let (url, h) = canned_server(resp);
    let client = HttpReplicaClient::new(&url).unwrap();
    let mut stream = client.open_ltx_stream(Txid(1)).unwrap().unwrap();
    let start = Instant::now();
    loop {
        sink.clear();
        match stream.next(&mut sink) {
            Err(_) => break,
            Ok(StreamEvent::Idle { .. }) => {
                assert!(start.elapsed() < DEADLINE, "truncated frame never errored")
            }
            Ok(other) => panic!("expected error on truncated frame, got {other:?}"),
        }
    }
    drop(stream);
    h.join().unwrap();

    // (f) A listing cut short of its declared content-length is an error,
    // never a silently shorter listing (which could look like divergence).
    let body = "0000000000000001-0000000000000001.ltx 100 -\n";
    let (url, h) = canned_server(
        format!(
            "HTTP/1.1 200 OK\r\nx-liters-protocol: 1\r\ncontent-length: {}\r\n\r\n{}",
            body.len() + 40, // claims more than it delivers
            body
        )
        .into_bytes(),
    );
    let err = HttpReplicaClient::new(&url).unwrap().ltx_files(0, Txid(0), false).unwrap_err();
    assert!(err.to_string().contains("truncated listing"), "unexpected error: {err}");
    h.join().unwrap();

    // (g) A listing without content-length is rejected outright.
    let (url, h) = canned_server(
        b"HTTP/1.1 200 OK\r\nx-liters-protocol: 1\r\n\r\n".to_vec(),
    );
    let err = HttpReplicaClient::new(&url).unwrap().ltx_files(0, Txid(0), false).unwrap_err();
    assert!(err.to_string().contains("content-length"), "unexpected error: {err}");
    h.join().unwrap();

    // (h) A ping frame whose payload is not a 16-hex TXID is a protocol
    // error (it carries divergence evidence; a lenient parse could mask a
    // reseed).
    let mut resp =
        b"HTTP/1.1 200 OK\r\nx-liters-protocol: 1\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
    resp.extend(chunk(b"liters-stream 1\n"));
    resp.extend(chunk(b"ping bogus\n"));
    resp.extend_from_slice(b"0\r\n\r\n");
    let (url, h) = canned_server(resp);
    let client = HttpReplicaClient::new(&url).unwrap();
    let mut stream = client.open_ltx_stream(Txid(1)).unwrap().unwrap();
    sink.clear();
    let err = stream.next(&mut sink).unwrap_err();
    assert!(err.to_string().contains("ping"), "unexpected error: {err}");
    drop(stream);
    h.join().unwrap();
}

#[test]
fn shutdown_unblocks_stalled_stream_writer() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    // Bounded load: 20 x 1 MiB, far more than loopback socket buffers hold,
    // so the server's stream writer ends up blocked on the stalled peer.
    let body = vec![0x5a; 1 << 20];
    for txid in 1..=20u64 {
        seed_file(root, 0, txid, txid, &body);
    }
    let (mut srv, _http) = serve(root);

    let mut sock = TcpStream::connect(srv.local_addr()).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    sock.write_all(
        b"GET /stream?seek=0000000000000001 HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n",
    )
    .unwrap();

    // Read ~1 KiB of the response, then stop reading but keep the socket
    // open: a stalled follower, not a disconnected one.
    let mut got = 0usize;
    let mut buf = [0u8; 256];
    while got < 1024 {
        let n = sock.read(&mut buf).unwrap();
        assert!(n > 0, "server closed the stream early");
        got += n;
    }
    // Give the server time to fill the socket buffers and block in write.
    std::thread::sleep(Duration::from_secs(1));

    let start = Instant::now();
    srv.shutdown();
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(10), "shutdown took {elapsed:?}");

    drop(sock);
}

#[test]
fn health_check_root_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut srv, _http) = serve(tmp.path());

    let mut sock = TcpStream::connect(srv.local_addr()).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    sock.write_all(b"GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n").unwrap();
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).unwrap();
    let resp = String::from_utf8(resp).unwrap();

    let (head, body) = resp.split_once("\r\n\r\n").expect("no header/body separator");
    assert!(head.starts_with("HTTP/1.1 200"), "head: {head:?}");
    assert!(
        head.to_ascii_lowercase().contains("x-liters-protocol: 1"),
        "missing protocol header: {head:?}"
    );
    assert!(body.starts_with("liters "), "body: {body:?}");

    drop(sock);
    srv.shutdown();
}

// ---------------------------------------------------------------------------
// Push (reversed roles): the server accepts replication, a remote client
// pushes. docs/http-protocol.md "Push".

/// A minimal valid LTX file: real 100-byte header (write_ltx_file decodes
/// it for the timestamp) + arbitrary body bytes.
fn ltx_bytes(min: u64, max: u64, body_len: usize) -> Vec<u8> {
    let hdr = ltx::Header {
        flags: ltx::HEADER_FLAG_NO_CHECKSUM,
        page_size: 4096,
        commit: 1,
        min_txid: Txid(min),
        max_txid: Txid(max),
        timestamp: 1_783_800_000_123,
        ..Default::default()
    };
    let mut v = hdr.encode().to_vec();
    v.extend(pattern(min as u8, body_len));
    v
}

fn serve_writable(root: &Path) -> (HttpServer, HttpReplicaClient) {
    let opts = HttpServerOptions { writable: true, ..test_opts() };
    let srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(root)), opts).unwrap();
    let client = HttpReplicaClient::new(format!("http://{}", srv.local_addr())).unwrap();
    (srv, client)
}

#[test]
fn push_roundtrip_matches_dir_backend() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let dir = DirReplicaClient::new(root);
    let (mut srv, http) = serve_writable(root);

    let bytes = ltx_bytes(1, 1, 500);
    let info = http.write_ltx_file(0, Txid(1), Txid(1), &mut &bytes[..]).unwrap();
    assert_eq!((info.level, info.min_txid, info.max_txid), (0, Txid(1), Txid(1)));
    assert_eq!(info.size, bytes.len() as u64);
    // created_at comes from the LTX header timestamp, echoed in the PUT
    // response's listing line.
    assert_eq!(
        info.created_at,
        Some(UNIX_EPOCH + Duration::from_millis(1_783_800_000_123))
    );

    // The dir backend sees exactly what a local write would have produced.
    let listed = dir.ltx_files(0, Txid(0), false).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].size, bytes.len() as u64);
    assert_eq!(read_all(dir.open_ltx_file(0, Txid(1), Txid(1), 0, 0).unwrap()), bytes);

    // Idempotent re-push of the same key is harmless (Writer retry
    // semantics).
    http.write_ltx_file(0, Txid(1), Txid(1), &mut &bytes[..]).unwrap();
    assert_eq!(dir.ltx_files(0, Txid(0), false).unwrap().len(), 1);
    assert_eq!(read_all(dir.open_ltx_file(0, Txid(1), Txid(1), 0, 0).unwrap()), bytes);

    // Delete over HTTP; deleting a missing file is not an error.
    let target = listed[0].clone();
    http.delete_ltx_files(std::slice::from_ref(&target)).unwrap();
    assert!(dir.ltx_files(0, Txid(0), false).unwrap().is_empty());
    http.delete_ltx_files(&[target]).unwrap();

    // delete_all wipes the bucket.
    let more = ltx_bytes(2, 2, 40);
    http.write_ltx_file(0, Txid(2), Txid(2), &mut &more[..]).unwrap();
    http.write_ltx_file(9, Txid(1), Txid(2), &mut &ltx_bytes(1, 2, 80)[..]).unwrap();
    http.delete_all().unwrap();
    assert!(dir.ltx_files(0, Txid(0), false).unwrap().is_empty());
    assert!(dir.ltx_files(9, Txid(0), false).unwrap().is_empty());

    srv.shutdown();
}

#[test]
fn read_only_server_rejects_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut srv, http) = serve(tmp.path()); // default opts: writable false

    let bytes = ltx_bytes(1, 1, 10);
    let err = http.write_ltx_file(0, Txid(1), Txid(1), &mut &bytes[..]).unwrap_err();
    assert!(err.to_string().contains("read-only"), "unexpected error: {err}");

    let info = ltx::FileInfo { level: 0, min_txid: Txid(1), max_txid: Txid(1), ..Default::default() };
    let err = http.delete_ltx_files(&[info]).unwrap_err();
    assert!(err.to_string().contains("403"), "unexpected error: {err}");

    let err = http.delete_all().unwrap_err();
    assert!(err.to_string().contains("403"), "unexpected error: {err}");

    // And nothing landed.
    assert!(DirReplicaClient::new(tmp.path()).ltx_files(0, Txid(0), false).unwrap().is_empty());
    srv.shutdown();
}

#[test]
fn push_with_garbage_header_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut srv, http) = serve_writable(tmp.path());

    let garbage = pattern(9, 300); // no LTX1 magic
    let err = http.write_ltx_file(0, Txid(1), Txid(1), &mut &garbage[..]).unwrap_err();
    assert!(err.to_string().contains("400"), "unexpected error: {err}");
    assert!(DirReplicaClient::new(tmp.path()).ltx_files(0, Txid(0), false).unwrap().is_empty());

    srv.shutdown();
}

#[test]
fn accepted_push_wakes_stream_followers() {
    // Relay: one client pushes in, a follower streaming from the same
    // server receives the file. Poll and ping intervals are set far beyond
    // the deadline so ONLY the PUT-handler notify can deliver in time — a
    // broken notify() fails this test instead of hiding behind the poll.
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions {
        poll_interval: Duration::from_secs(120),
        ping_interval: Duration::from_secs(120),
        writable: true,
    };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    let http = HttpReplicaClient::new(format!("http://{}", srv.local_addr())).unwrap();

    let mut stream = http.open_ltx_stream(Txid(1)).unwrap().unwrap();
    let mut sink = Vec::new();

    let bytes = ltx_bytes(1, 1, 200);
    http.write_ltx_file(0, Txid(1), Txid(1), &mut &bytes[..]).unwrap();

    match next_non_idle(stream.as_mut(), &mut sink) {
        StreamEvent::Ltx(info) => {
            assert_eq!((info.min_txid, info.max_txid), (Txid(1), Txid(1)));
            assert_eq!(sink, bytes);
        }
        other => panic!("expected pushed file on stream, got {other:?}"),
    }

    drop(stream);
    srv.shutdown();
}

#[test]
fn truncated_content_length_push_commits_nothing() {
    // The critical case: a content-length body cut by a clean FIN must be
    // an error, never a shorter file — a truncated file committed with 200
    // would sit at the bucket's max TXID and the pusher would never
    // re-upload it.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (mut srv, http) = serve_writable(root);

    let full = ltx_bytes(3, 3, 400);
    let mut sock = TcpStream::connect(srv.local_addr()).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    write!(
        sock,
        "PUT /ltx/0/{} HTTP/1.1\r\nhost: x\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        format_filename(Txid(3), Txid(3)),
        full.len()
    )
    .unwrap();
    sock.write_all(&full[..250]).unwrap(); // 250 of 500 bytes
    sock.flush().unwrap();
    sock.shutdown(std::net::Shutdown::Write).unwrap(); // clean FIN, not RST

    // The server must reject (400 short body) — and above all commit
    // nothing.
    let mut resp = String::new();
    let _ = sock.read_to_string(&mut resp);
    assert!(
        resp.starts_with("HTTP/1.1 400"),
        "expected 400 for truncated content-length body, got: {resp:?}"
    );
    assert!(http.ltx_files(0, Txid(0), false).unwrap().is_empty());
    let leftovers: Vec<_> = match std::fs::read_dir(root.join("ltx").join("0")) {
        Ok(rd) => rd.map(|e| e.unwrap().file_name()).collect(),
        Err(_) => Vec::new(),
    };
    assert!(leftovers.is_empty(), "leftovers after truncated push: {leftovers:?}");

    drop(sock);
    srv.shutdown();
}

#[test]
fn truncated_push_leaves_no_file() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (mut srv, http) = serve_writable(root);
    let addr = srv.local_addr();

    // Raw PUT that sends a valid header + partial body, then cuts the
    // connection mid-chunk: the backend's tmp+rename atomicity means no
    // file (and no stray tmp) may appear.
    let full = ltx_bytes(5, 5, 400);
    let mut sock = TcpStream::connect(addr).unwrap();
    write!(
        sock,
        "PUT /ltx/0/{} HTTP/1.1\r\nhost: x\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
        format_filename(Txid(5), Txid(5))
    )
    .unwrap();
    // One complete chunk (header + some body), then a declared-but-cut one.
    sock.write_all(&chunk(&full[..150])).unwrap();
    sock.write_all(b"1f4\r\n").unwrap();
    sock.write_all(&full[150..160]).unwrap();
    sock.flush().unwrap();
    drop(sock); // FIN mid-chunk

    // The failed write must never surface a file; give the server a moment
    // to process the aborted request, then verify emptiness and liveness.
    let deadline = Instant::now() + DEADLINE;
    loop {
        let level_dir = root.join("ltx").join("0");
        let leftovers: Vec<_> = match std::fs::read_dir(&level_dir) {
            Ok(rd) => rd.map(|e| e.unwrap().file_name()).collect(),
            Err(_) => Vec::new(), // dir not created at all: fine
        };
        if leftovers.is_empty() && http.ltx_files(0, Txid(0), false).unwrap().is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "leftover files after aborted push: {leftovers:?}");
        std::thread::sleep(Duration::from_millis(50));
    }

    // Server still healthy after the aborted request.
    assert!(http.ltx_files(0, Txid(0), false).unwrap().is_empty());
    srv.shutdown();
}
