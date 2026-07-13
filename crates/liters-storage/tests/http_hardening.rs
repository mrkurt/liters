//! Storage-level tests for the HTTP hardening features
//! (docs/http-protocol.md): bearer-token auth, multi-DB serving under
//! `/db/{name}`, per-bucket stream notification, writer fencing (lease +
//! TXID monotonicity), client-side cancellation, and the client error
//! taxonomy. Conventions follow http_serve.rs: bounded loads, loopback
//! port 0, eventual-convergence waits under a generous deadline.
#![cfg(feature = "http")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use liters_storage::{
    CancelToken, DirReplicaClient, HttpClientOptions, HttpReplicaClient, HttpServer,
    HttpServerOptions, LtxStream, ReplicaClient, StorageError, StreamEvent,
};
use ltx::{format_filename, FileInfo, Txid};

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

/// Deterministic, seed-distinguishable filler bytes.
fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

/// A minimal valid LTX file: real 100-byte header (write_ltx_file decodes
/// it for the timestamp) + arbitrary body bytes.
fn ltx_bytes(min: u64, max: u64, body_len: usize) -> Vec<u8> {
    ltx_bytes_with(min, max, body_len, min as u8)
}

/// Like [`ltx_bytes`] but with an explicit body seed, so two files with the
/// same TXID range (and the same length) can carry different bytes.
fn ltx_bytes_with(min: u64, max: u64, body_len: usize, seed: u8) -> Vec<u8> {
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
    v.extend(pattern(seed, body_len));
    v
}

/// Seeds `{root}/ltx/{level}/{min:016x}-{max:016x}.ltx` behind the server's
/// back (no notify). Write-then-rename so a concurrently polling server
/// never lists a half-written size.
fn seed_file(root: &Path, level: u8, min: u64, max: u64, bytes: &[u8]) {
    let dir = root.join("ltx").join(level.to_string());
    std::fs::create_dir_all(&dir).unwrap();
    let name = format_filename(Txid(min), Txid(max));
    let tmp = dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, bytes).unwrap();
    std::fs::rename(&tmp, dir.join(name)).unwrap();
}

fn push(client: &dyn ReplicaClient, level: u8, min: u64, max: u64) -> Result<(), StorageError> {
    let bytes = ltx_bytes(min, max, 200);
    client.write_ltx_file(level, Txid(min), Txid(max), &mut &bytes[..]).map(|_| ())
}

fn client_with(url: &str, opts: HttpClientOptions) -> HttpReplicaClient {
    HttpReplicaClient::with_options(url, opts).unwrap()
}

/// Pumps the stream past Idle ticks until a substantive event arrives.
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
/// request head, writes `response` verbatim, and closes.
fn canned_server(response: Vec<u8>) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
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
    });
    (url, handle)
}

/// Raw request against the server; returns the full response as a string.
fn raw_request(addr: std::net::SocketAddr, request: &str) -> String {
    let mut sock = TcpStream::connect(addr).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    sock.write_all(request.as_bytes()).unwrap();
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).unwrap();
    String::from_utf8(resp).unwrap()
}

// ---------------------------------------------------------------------------
// Auth

#[test]
fn auth_gates_every_route_class() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions {
        writable: true,
        auth_token: Some("sekrit".into()),
        ..test_opts()
    };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    let url = format!("http://{}", srv.local_addr());

    let expect_unauthorized = |err: StorageError, what: &str| {
        assert!(
            matches!(err, StorageError::Unauthorized(_)),
            "{what}: expected Unauthorized, got {err:?}"
        );
    };

    // Every route class rejects a tokenless client...
    let bare = HttpReplicaClient::new(&url).unwrap();
    expect_unauthorized(bare.ltx_files(0, Txid(0), false).unwrap_err(), "list");
    expect_unauthorized(
        bare.open_ltx_file(0, Txid(1), Txid(1), 0, 0).map(|_| ()).unwrap_err(),
        "get",
    );
    expect_unauthorized(bare.open_ltx_stream(Txid(1)).map(|_| ()).unwrap_err(), "stream");
    expect_unauthorized(push(&bare, 0, 1, 1).unwrap_err(), "put");
    let info = ltx::FileInfo { level: 0, min_txid: Txid(1), max_txid: Txid(1), ..Default::default() };
    expect_unauthorized(bare.delete_ltx_files(&[info]).unwrap_err(), "delete");
    expect_unauthorized(bare.delete_all().unwrap_err(), "delete all");

    // ...and a wrong-token client; the server's body excerpt is preserved.
    let wrong = client_with(&url, HttpClientOptions {
        auth_token: Some("wrong".into()),
        ..HttpClientOptions::default()
    });
    let err = wrong.ltx_files(0, Txid(0), false).unwrap_err();
    assert!(matches!(err, StorageError::Unauthorized(_)), "wrong token: {err:?}");
    assert!(err.to_string().contains("authorization required"), "no body excerpt: {err}");

    // The right token passes on every route class.
    let good = client_with(&url, HttpClientOptions {
        auth_token: Some("sekrit".into()),
        ..HttpClientOptions::default()
    });
    push(&good, 0, 1, 1).unwrap();
    let listed = good.ltx_files(0, Txid(0), false).unwrap();
    assert_eq!(listed.len(), 1);
    let mut body = Vec::new();
    good.open_ltx_file(0, Txid(1), Txid(1), 0, 0).unwrap().read_to_end(&mut body).unwrap();
    assert_eq!(body, ltx_bytes(1, 1, 200));
    let mut stream = good.open_ltx_stream(Txid(1)).unwrap().unwrap();
    let mut sink = Vec::new();
    match next_non_idle(stream.as_mut(), &mut sink) {
        StreamEvent::Ltx(info) => assert_eq!(info.min_txid, Txid(1)),
        other => panic!("expected Ltx on authed stream, got {other:?}"),
    }
    drop(stream);
    good.delete_ltx_files(&listed).unwrap();
    good.delete_all().unwrap();

    // GET / stays open — liveness probes need no secret.
    let resp = raw_request(srv.local_addr(), "GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n");
    assert!(resp.starts_with("HTTP/1.1 200"), "unauthenticated health check failed: {resp:?}");

    srv.shutdown();
}

// ---------------------------------------------------------------------------
// Multi-DB

#[test]
fn separate_servers_are_isolated() {
    // Multiple databases are served by multiple single-DB servers (one bucket
    // each). A push to one never appears in the other — physical isolation.
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_b = tempfile::tempdir().unwrap();
    let opts = || HttpServerOptions { writable: true, ..test_opts() };
    let srv_a =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp_a.path())), opts())
            .unwrap();
    let srv_b =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp_b.path())), opts())
            .unwrap();

    let ca = HttpReplicaClient::new(format!("http://{}", srv_a.local_addr())).unwrap();
    let cb = HttpReplicaClient::new(format!("http://{}", srv_b.local_addr())).unwrap();
    push(&ca, 0, 1, 1).unwrap();
    assert_eq!(ca.ltx_files(0, Txid(0), false).unwrap().len(), 1);
    assert!(cb.ltx_files(0, Txid(0), false).unwrap().is_empty(), "b sees a's push");
    assert!(DirReplicaClient::new(tmp_b.path()).ltx_files(0, Txid(0), false).unwrap().is_empty());

    let mut body = Vec::new();
    ca.open_ltx_file(0, Txid(1), Txid(1), 0, 0).unwrap().read_to_end(&mut body).unwrap();
    assert_eq!(body, ltx_bytes(1, 1, 200));

    // Health probe answers on each server's root.
    let resp = raw_request(srv_a.local_addr(), "GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n");
    assert!(resp.contains("liters "), "health: {resp:?}");

    let (mut srv_a, mut srv_b) = (srv_a, srv_b);
    srv_a.shutdown();
    srv_b.shutdown();
}

/// Delegating [`ReplicaClient`] wrapper that counts level-0 listing calls:
/// a deterministic, test-side window into a stream handler's progress (each
/// handler round lists L0 exactly once).
struct CountingClient {
    inner: DirReplicaClient,
    l0_lists: Arc<AtomicU64>,
}

impl ReplicaClient for CountingClient {
    fn client_type(&self) -> &'static str {
        self.inner.client_type()
    }

    fn ltx_files(
        &self,
        level: u8,
        seek: Txid,
        use_metadata: bool,
    ) -> liters_storage::Result<Vec<FileInfo>> {
        let r = self.inner.ltx_files(level, seek, use_metadata);
        if level == 0 {
            // Incremented AFTER the listing returns: an observed count
            // proves that listing's snapshot is complete.
            self.l0_lists.fetch_add(1, Ordering::SeqCst);
        }
        r
    }

    fn open_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        offset: u64,
        size: u64,
    ) -> liters_storage::Result<Box<dyn Read + Send>> {
        self.inner.open_ltx_file(level, min_txid, max_txid, offset, size)
    }

    fn write_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        rd: &mut dyn Read,
    ) -> liters_storage::Result<FileInfo> {
        self.inner.write_ltx_file(level, min_txid, max_txid, rd)
    }

    fn delete_ltx_files(&self, infos: &[FileInfo]) -> liters_storage::Result<()> {
        self.inner.delete_ltx_files(infos)
    }

    fn delete_all(&self) -> liters_storage::Result<()> {
        self.inner.delete_all()
    }
}

#[test]
fn per_server_notify_wakes_only_its_stream() {
    // Poll and ping are pushed far beyond the deadline, so an `ltx` frame
    // can only be delivered by a notify wake — and a wake from the WRONG
    // server would be visible because a file seeded behind the server's
    // back gets delivered by any spurious re-list.
    let tmp_a = tempfile::tempdir().unwrap();
    let tmp_b = tempfile::tempdir().unwrap();
    let opts = || HttpServerOptions {
        poll_interval: Duration::from_secs(120),
        ping_interval: Duration::from_secs(120),
        ..HttpServerOptions::default()
    };
    let l0_lists = Arc::new(AtomicU64::new(0));
    let srv_a = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(CountingClient {
            inner: DirReplicaClient::new(tmp_a.path()),
            l0_lists: Arc::clone(&l0_lists),
        }),
        opts(),
    )
    .unwrap();
    let srv_b =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp_b.path())), opts())
            .unwrap();
    let addr_a = srv_a.local_addr();
    let na = srv_a.notifying_client(Box::new(DirReplicaClient::new(tmp_a.path())));
    let nb = srv_b.notifying_client(Box::new(DirReplicaClient::new(tmp_b.path())));

    // (1,1) exists before the stream opens: the handler's initial listing
    // round delivers it without any notify.
    push(na.as_ref(), 0, 1, 1).unwrap();
    let ca = HttpReplicaClient::new(format!("http://{addr_a}")).unwrap();
    let mut stream = ca.open_ltx_stream(Txid(1)).unwrap().unwrap();
    let mut sink = Vec::new();
    match next_non_idle(stream.as_mut(), &mut sink) {
        StreamEvent::Ltx(info) => assert_eq!(info.min_txid, Txid(1)),
        other => panic!("expected initial Ltx, got {other:?}"),
    }

    // Deterministic barrier (not wall-clock ticks): `a`'s handler lists L0
    // exactly three times before parking in its 120s wait — once inside
    // the open-time bucket_max scan, once for the round that delivered
    // (1,1), and once for the follow-up round that finds nothing new. Once
    // the third listing has returned, a file seeded WITHOUT a notify can
    // only be delivered by a generation bump (a wake) or the 120s poll —
    // no in-flight listing round can race the bait in.
    let deadline = Instant::now() + DEADLINE;
    while l0_lists.load(Ordering::SeqCst) < 3 {
        assert!(
            Instant::now() < deadline,
            "handler never finished its post-frame round (L0 listings: {})",
            l0_lists.load(Ordering::SeqCst)
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    // Bait: (2,2) lands in server `a`'s bucket with NO notify. Only a wake
    // can deliver it before the 120s poll.
    seed_file(tmp_a.path(), 0, 2, 2, &ltx_bytes(2, 2, 200));

    // A push to `b` must not wake `a`'s stream: the two servers have
    // independent notify signals.
    push(nb.as_ref(), 0, 1, 1).unwrap();
    for _ in 0..3 {
        sink.clear();
        match stream.next(&mut sink).unwrap() {
            StreamEvent::Idle { .. } => {}
            other => panic!("b's push woke a's stream: got {other:?}"),
        }
    }
    // Server-side confirmation: b's push triggered no listing round on a.
    assert_eq!(
        l0_lists.load(Ordering::SeqCst),
        3,
        "b's push triggered a listing round on a's bucket"
    );

    // A push to `a` wakes it: the bait and the new file both arrive.
    push(na.as_ref(), 0, 3, 3).unwrap();
    for want in [2u64, 3] {
        match next_non_idle(stream.as_mut(), &mut sink) {
            StreamEvent::Ltx(info) => {
                assert_eq!((info.min_txid, info.max_txid), (Txid(want), Txid(want)));
                assert_eq!(sink, ltx_bytes(want, want, 200), "body of txid {want}");
            }
            other => panic!("expected Ltx {want} after a's notify, got {other:?}"),
        }
    }

    drop(stream);
    let (mut srv_a, mut srv_b) = (srv_a, srv_b);
    srv_a.shutdown();
    srv_b.shutdown();
}

// ---------------------------------------------------------------------------
// Fencing

#[test]
fn fencing_monotonicity_rules() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions { writable: true, ..test_opts() };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    let url = format!("http://{}", srv.local_addr());
    let plain = HttpReplicaClient::new(&url).unwrap();

    let expect_conflict = |r: Result<(), StorageError>, what: &str| {
        let err = r.unwrap_err();
        assert!(matches!(err, StorageError::Conflict(_)), "{what}: got {err:?}");
        err.to_string()
    };

    // Appends into an empty then growing L0.
    push(&plain, 0, 1, 1).unwrap();
    push(&plain, 0, 2, 3).unwrap(); // multi-TXID file, min == cur+1
    // Gap and non-exact overlap are rejected with the offered range and
    // the bucket's position, so the pusher can tell WHY.
    let msg = expect_conflict(push(&plain, 0, 5, 5), "gap push");
    assert!(msg.contains("non-monotonic"), "gap message: {msg}");
    assert!(
        msg.contains("0000000000000005-0000000000000005") && msg.contains("0000000000000003"),
        "L0 message must carry the offered range and bucket position: {msg}"
    );
    expect_conflict(push(&plain, 0, 3, 3), "backwards push");
    // Idempotent exact re-push is accepted (writer crash-retry semantics).
    push(&plain, 0, 2, 3).unwrap();
    push(&plain, 0, 4, 4).unwrap();

    // Higher levels may only summarize uploaded history (max <= bucket
    // max), and exact re-pushes trivially satisfy that.
    push(&plain, 1, 1, 3).unwrap();
    push(&plain, 1, 1, 3).unwrap();
    push(&plain, 9, 1, 4).unwrap();
    let msg = expect_conflict(push(&plain, 1, 1, 9), "L1 beyond bucket max");
    assert!(msg.contains("beyond bucket max"), "L1 message: {msg}");
    // The 409 names the offered range, the bucket max, and the required
    // ordering (L0 backlog before compactions/snapshots).
    assert!(
        msg.contains("0000000000000001-0000000000000009") && msg.contains("0000000000000004"),
        "L1 message must carry the offered range and bucket max: {msg}"
    );
    assert!(msg.contains("L0 backlog"), "L1 message must state the ordering contract: {msg}");

    srv.shutdown();
}

#[test]
fn l0_repush_requires_identical_content() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions { writable: true, ..test_opts() };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    let url = format!("http://{}", srv.local_addr());
    let client = HttpReplicaClient::new(&url).unwrap();

    let original = ltx_bytes(1, 1, 300);
    client.write_ltx_file(0, Txid(1), Txid(1), &mut &original[..]).unwrap();

    // Byte-identical re-push: idempotent, accepted (writer crash-retry).
    client.write_ltx_file(0, Txid(1), Txid(1), &mut &original[..]).unwrap();

    // Same TXID range, same length, different bytes: the dual-writer
    // splice — must be 409, never silently accepted.
    let same_len = ltx_bytes_with(1, 1, 300, 0x99);
    assert_ne!(same_len, original);
    let err = client.write_ltx_file(0, Txid(1), Txid(1), &mut &same_len[..]).unwrap_err();
    assert!(matches!(err, StorageError::Conflict(_)), "same-length divergent: {err:?}");
    assert!(err.to_string().contains("does not match"), "message: {err}");

    // Same TXID range, different length: also 409 (cheap size gate).
    let longer = ltx_bytes(1, 1, 301);
    let err = client.write_ltx_file(0, Txid(1), Txid(1), &mut &longer[..]).unwrap_err();
    assert!(matches!(err, StorageError::Conflict(_)), "different-length divergent: {err:?}");

    // The stored file is still byte-for-byte the original.
    let mut stored = Vec::new();
    client
        .open_ltx_file(0, Txid(1), Txid(1), 0, 0)
        .unwrap()
        .read_to_end(&mut stored)
        .unwrap();
    assert_eq!(stored, original);

    srv.shutdown();
}

#[test]
fn concurrent_same_txid_pushes_one_wins() {
    // The fence must be atomic with the commit: two writers racing the
    // SAME next TXID with different bodies must produce exactly one 200
    // and one 409, and the bucket must hold exactly the winner's bytes.
    // Bodies have identical length, so a size check alone cannot tell them
    // apart. Several rounds widen the scheduling interleavings; every
    // round is a fresh TXID (bounded: 5 rounds x 2 threads x ~4 KiB).
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions { writable: true, ..test_opts() };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    let url = format!("http://{}", srv.local_addr());

    // Seed txid 1 so the racing pushes are appends, not first-files.
    let observer = HttpReplicaClient::new(&url).unwrap();
    push(&observer, 0, 1, 1).unwrap();

    for round in 0..5u64 {
        let txid = 2 + round;
        let body_a = ltx_bytes_with(txid, txid, 4096, 0xa0 + round as u8);
        let body_b = ltx_bytes_with(txid, txid, 4096, 0x50 + round as u8);
        assert_ne!(body_a, body_b);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = [body_a.clone(), body_b.clone()]
            .into_iter()
            .map(|bytes| {
                let url = url.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let c = HttpReplicaClient::new(&url).unwrap();
                    barrier.wait();
                    c.write_ltx_file(0, Txid(txid), Txid(txid), &mut &bytes[..]).map(|_| ())
                })
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let winners = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(winners, 1, "round {round}: exactly one push must win: {results:?}");
        let loser = results.iter().find_map(|r| r.as_ref().err()).unwrap();
        assert!(
            matches!(loser, StorageError::Conflict(_)),
            "round {round}: loser must see 409/Conflict, got {loser:?}"
        );

        // The bucket serves exactly the winner's bytes.
        let mut stored = Vec::new();
        observer
            .open_ltx_file(0, Txid(txid), Txid(txid), 0, 0)
            .unwrap()
            .read_to_end(&mut stored)
            .unwrap();
        let expect = if results[0].is_ok() { &body_a } else { &body_b };
        assert_eq!(&stored, expect, "round {round}: stored bytes must match the 200");
    }

    srv.shutdown();
}

#[test]
fn fencing_writer_lease() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions { writable: true, ..test_opts() };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    let url = format!("http://{}", srv.local_addr());

    let with_id = |id: &str, takeover: bool| {
        client_with(&url, HttpClientOptions {
            writer_id: Some(id.into()),
            takeover,
            ..HttpClientOptions::default()
        })
    };
    let ca = with_id("aaa", false);
    let cb = with_id("bbb", false);
    let cb_takeover = with_id("bbb", true);
    let plain = HttpReplicaClient::new(&url).unwrap();

    // First identified writer takes the lease.
    push(&ca, 0, 1, 1).unwrap();

    // A different id is fenced out — of PUTs and DELETEs — and the 409
    // reveals only the owner's id.
    let err = push(&cb, 0, 2, 2).unwrap_err();
    assert!(matches!(err, StorageError::Conflict(_)), "lease conflict: {err:?}");
    assert!(err.to_string().contains("owned by writer aaa"), "lease message: {err}");
    let info = ltx::FileInfo { level: 0, min_txid: Txid(1), max_txid: Txid(1), ..Default::default() };
    let err = cb.delete_ltx_files(std::slice::from_ref(&info)).unwrap_err();
    assert!(matches!(err, StorageError::Conflict(_)), "delete under lease: {err:?}");
    let err = cb.delete_all().unwrap_err();
    assert!(matches!(err, StorageError::Conflict(_)), "delete_all under lease: {err:?}");

    // A headerless pusher skips lease logic entirely (plain v1
    // back-compat) but still faces monotonicity.
    push(&plain, 0, 2, 2).unwrap();
    let err = push(&plain, 0, 9, 9).unwrap_err();
    assert!(matches!(err, StorageError::Conflict(_)), "headerless monotonicity: {err:?}");

    // Takeover wins and flips the lease; the previous owner is now fenced.
    push(&cb_takeover, 0, 3, 3).unwrap();
    let err = push(&ca, 0, 4, 4).unwrap_err();
    assert!(err.to_string().contains("owned by writer bbb"), "flipped lease: {err}");
    // The new owner keeps writing without the takeover header.
    push(&cb, 0, 4, 4).unwrap();

    srv.shutdown();
}

#[test]
fn fencing_lease_expires_after_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions {
        writable: true,
        lease_ttl: Duration::from_millis(250),
        ..test_opts()
    };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    let url = format!("http://{}", srv.local_addr());

    let ca = client_with(&url, HttpClientOptions {
        writer_id: Some("aaa".into()),
        ..HttpClientOptions::default()
    });
    let cb = client_with(&url, HttpClientOptions {
        writer_id: Some("bbb".into()),
        ..HttpClientOptions::default()
    });

    push(&ca, 0, 1, 1).unwrap();
    // After the TTL lapses another writer may claim without a takeover.
    // (That the lease HOLDS within its TTL is proven by
    // fencing_writer_lease under the 24h default — asserting it here with
    // a millisecond TTL would race the HTTP roundtrip.)
    std::thread::sleep(Duration::from_millis(500));
    push(&cb, 0, 2, 2).unwrap();

    srv.shutdown();
}

#[test]
fn rejected_push_never_claims_lease() {
    // The lease reflects the last ACCEPTED write. A rejected identified
    // push must not create a lease on an unowned bucket — one stray 409
    // from a stale device must not fence out the legitimate writer for a
    // whole lease_ttl (24h by default).
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions { writable: true, ..test_opts() };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    let url = format!("http://{}", srv.local_addr());

    let with_id = |id: &str| {
        client_with(&url, HttpClientOptions {
            writer_id: Some(id.into()),
            ..HttpClientOptions::default()
        })
    };
    let stale = with_id("stale");
    let good = with_id("good");

    // Seed via a headerless push (takes no lease).
    push(&HttpReplicaClient::new(&url).unwrap(), 0, 1, 1).unwrap();

    // A stale device's identified push is rejected (non-monotonic)...
    let err = push(&stale, 0, 9, 9).unwrap_err();
    assert!(matches!(err, StorageError::Conflict(_)), "stale push: {err:?}");

    // ...and must NOT have taken the lease: the healthy writer's next push
    // is accepted, not 409 "owned by writer stale".
    push(&good, 0, 2, 2).unwrap();

    // Now "good" owns the lease. A rejected push from another id must not
    // steal or disturb it either.
    let err = push(&stale, 0, 9, 9).unwrap_err();
    assert!(err.to_string().contains("owned by writer good"), "lease reveal: {err}");
    push(&good, 0, 3, 3).unwrap();

    srv.shutdown();
}

#[test]
fn rejected_pushes_do_not_refresh_lease_ttl() {
    // A broken client stuck retrying rejected pushes must not keep its own
    // lease alive forever: the TTL runs from the last ACCEPTED write.
    let ttl = Duration::from_millis(300);
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions { writable: true, lease_ttl: ttl, ..test_opts() };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    let url = format!("http://{}", srv.local_addr());

    let ca = client_with(&url, HttpClientOptions {
        writer_id: Some("aaa".into()),
        ..HttpClientOptions::default()
    });
    let cb = client_with(&url, HttpClientOptions {
        writer_id: Some("bbb".into()),
        ..HttpClientOptions::default()
    });

    // aaa's lease starts at its accepted write...
    push(&ca, 0, 1, 1).unwrap();
    // ...then aaa keeps sending REJECTED (non-monotonic) pushes for more
    // than a TTL. None of them may refresh the lease.
    let until = Instant::now() + 2 * ttl + Duration::from_millis(100);
    while Instant::now() < until {
        let err = push(&ca, 0, 9, 9).unwrap_err();
        assert!(matches!(err, StorageError::Conflict(_)), "own rejected push: {err:?}");
        std::thread::sleep(Duration::from_millis(50));
    }

    // The TTL has expired despite aaa's continuous rejected activity, so
    // bbb claims the bucket without a takeover header.
    push(&cb, 0, 2, 2).unwrap();

    srv.shutdown();
}

// ---------------------------------------------------------------------------
// Error-path body draining

#[test]
fn unmatched_put_drains_body_before_404() {
    // A PUT to a path the server does not serve (e.g. a misconfigured base
    // path) must drain the request body (bounded) before responding:
    // closing with unread bytes RSTs the connection and can destroy the
    // 404, making a permanent misconfiguration look transient. The body is
    // far larger than loopback socket buffers, so on a non-draining server
    // the write below would fail instead of completing.
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions { writable: true, ..test_opts() };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();

    let body = vec![0x5a; 2 << 20]; // 2 MiB: > socket buffers, < drain cap
    let mut sock = TcpStream::connect(srv.local_addr()).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    sock.set_write_timeout(Some(Duration::from_secs(30))).unwrap();
    write!(
        sock,
        "PUT /dbs/app/ltx/0/{} HTTP/1.1\r\nhost: x\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        format_filename(Txid(1), Txid(1)),
        body.len()
    )
    .unwrap();
    sock.write_all(&body).expect("server must drain an unmatched PUT's body");
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).unwrap();
    let resp = String::from_utf8(resp).unwrap();
    assert!(resp.starts_with("HTTP/1.1 404"), "expected 404, got: {resp:?}");

    // Same for the 405 arm (foreign method with a body).
    let mut sock = TcpStream::connect(srv.local_addr()).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    sock.set_write_timeout(Some(Duration::from_secs(30))).unwrap();
    write!(
        sock,
        "POST /ltx/0/{} HTTP/1.1\r\nhost: x\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        format_filename(Txid(1), Txid(1)),
        body.len()
    )
    .unwrap();
    sock.write_all(&body).expect("server must drain a 405'd body");
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).unwrap();
    let resp = String::from_utf8(resp).unwrap();
    assert!(resp.starts_with("HTTP/1.1 405"), "expected 405, got: {resp:?}");

    srv.shutdown();
}

// ---------------------------------------------------------------------------
// Shutdown vs removed buckets

#[test]
fn shutdown_wakes_parked_stream() {
    // A /stream handler parked in its poll_interval wait must be woken by
    // shutdown — with a 120s poll, a missed wake would block shutdown's join
    // far beyond any mobile background-task budget.
    let tmp = tempfile::tempdir().unwrap();
    let opts = HttpServerOptions {
        poll_interval: Duration::from_secs(120),
        ping_interval: Duration::from_secs(120),
        ..HttpServerOptions::default()
    };
    let mut srv =
        HttpServer::bind("127.0.0.1:0", Arc::new(DirReplicaClient::new(tmp.path())), opts)
            .unwrap();
    seed_file(tmp.path(), 0, 1, 1, &ltx_bytes(1, 1, 100));

    let client = HttpReplicaClient::new(format!("http://{}", srv.local_addr())).unwrap();
    let mut stream = client.open_ltx_stream(Txid(1)).unwrap().unwrap();
    let mut sink = Vec::new();
    match next_non_idle(stream.as_mut(), &mut sink) {
        StreamEvent::Ltx(info) => assert_eq!(info.min_txid, Txid(1)),
        other => panic!("expected Ltx, got {other:?}"),
    }

    // Let the handler catch up and park in its 120s wait.
    sink.clear();
    match stream.next(&mut sink).unwrap() {
        StreamEvent::Idle { .. } => {} // one ~1s tick: the handler had time to park
        other => panic!("expected Idle on caught-up stream, got {other:?}"),
    }

    let start = Instant::now();
    srv.shutdown();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "shutdown blocked {elapsed:?} joining a parked stream (poll_interval is 120s)"
    );
    drop(stream);
}

// ---------------------------------------------------------------------------
// Cancellation

#[test]
fn cancel_interrupts_stalled_put() {
    // Sink server: accepts one connection, drains the request head, then
    // never reads again — the client's socket buffers fill and its upload
    // stalls in send-timeout ticks. Held open (bounded) until the test
    // finishes.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") && head.len() < 8192 {
            match stream.read(&mut byte) {
                Ok(0) | Err(_) => break,
                Ok(_) => head.push(byte[0]),
            }
        }
        let _ = done_rx.recv_timeout(Duration::from_secs(30));
    });

    // A tighter io_timeout keeps the failure mode bounded even if
    // cancellation were broken (the stall budget would kick in).
    let client = client_with(&url, HttpClientOptions {
        io_timeout: Duration::from_secs(20),
        ..HttpClientOptions::default()
    });
    let token = CancelToken::new();
    client.set_cancel(token.clone());
    let canceller = std::thread::spawn({
        let token = token.clone();
        move || {
            std::thread::sleep(Duration::from_millis(500));
            token.cancel();
        }
    });

    // Body far larger than loopback socket buffers, so the upload is
    // mid-flight when the cancel lands. Never fully sent.
    let start = Instant::now();
    let mut body = std::io::repeat(0x5a).take(64 << 20);
    let err = client.write_ltx_file(0, Txid(1), Txid(1), &mut body).unwrap_err();
    let elapsed = start.elapsed();
    assert!(matches!(err, StorageError::Cancelled), "expected Cancelled, got {err:?}");
    // Bound: one 2s write tick past the cancel, plus scheduling slop —
    // and far below the 20s stall budget it must NOT wait out.
    assert!(elapsed < Duration::from_secs(6), "cancel took {elapsed:?}");

    let _ = done_tx.send(());
    canceller.join().unwrap();
    server.join().unwrap();
}

#[test]
fn cancel_interrupts_stream_and_fresh_token_resumes() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut srv, client) = {
        let srv = HttpServer::bind(
            "127.0.0.1:0",
            Arc::new(DirReplicaClient::new(tmp.path())),
            test_opts(),
        )
        .unwrap();
        let client = HttpReplicaClient::new(format!("http://{}", srv.local_addr())).unwrap();
        (srv, client)
    };

    let token = CancelToken::new();
    client.set_cancel(token.clone());
    let mut stream = client.open_ltx_stream(Txid(1)).unwrap().unwrap();

    // Cancel while next() is parked in its ~1s idle tick.
    let canceller = std::thread::spawn({
        let token = token.clone();
        move || {
            std::thread::sleep(Duration::from_millis(300));
            token.cancel();
        }
    });
    let start = Instant::now();
    let mut sink = Vec::new();
    let err = loop {
        sink.clear();
        match stream.next(&mut sink) {
            Ok(StreamEvent::Idle { .. }) => {
                assert!(start.elapsed() < DEADLINE, "cancel never surfaced")
            }
            Ok(other) => panic!("unexpected event on cancelled stream: {other:?}"),
            Err(e) => break e,
        }
    };
    assert!(matches!(err, StorageError::Cancelled), "expected Cancelled, got {err:?}");
    // Bound: one 1s stream tick past the cancel plus slop — far below the
    // 45s dead-man it must NOT wait out.
    assert!(start.elapsed() < Duration::from_secs(5), "cancel took {:?}", start.elapsed());
    canceller.join().unwrap();
    drop(stream);

    // A cancelled token is sticky: the client stays cancelled...
    let err = client.ltx_files(0, Txid(0), false).unwrap_err();
    assert!(matches!(err, StorageError::Cancelled), "sticky token: {err:?}");
    // ...until a fresh token is installed (resume semantics).
    client.set_cancel(CancelToken::new());
    assert!(client.ltx_files(0, Txid(0), false).unwrap().is_empty());

    srv.shutdown();
}

// ---------------------------------------------------------------------------
// Error taxonomy and options validation

#[test]
fn error_taxonomy_transport_vs_protocol() {
    // Connect refused → Unavailable (transient; the retry signal).
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
        // dropped: nothing listens here now
    };
    let client = HttpReplicaClient::new(format!("http://127.0.0.1:{port}")).unwrap();
    let err = client.ltx_files(0, Txid(0), false).unwrap_err();
    assert!(matches!(err, StorageError::Unavailable(_)), "connect refused: {err:?}");
    assert!(err.is_transient(), "connect refused must be transient");

    // A non-liters server → Other (protocol error; NOT transient).
    let (url, h) = canned_server(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".to_vec());
    let err = HttpReplicaClient::new(&url).unwrap().ltx_files(0, Txid(0), false).unwrap_err();
    assert!(matches!(err, StorageError::Other(_)), "foreign server: {err:?}");
    assert!(!err.is_transient(), "protocol errors must not be transient");
    h.join().unwrap();

    // 403 on a write → ReadOnly.
    let tmp = tempfile::tempdir().unwrap();
    let mut srv = HttpServer::bind(
        "127.0.0.1:0",
        Arc::new(DirReplicaClient::new(tmp.path())),
        test_opts(), // writable: false
    )
    .unwrap();
    let client = HttpReplicaClient::new(format!("http://{}", srv.local_addr())).unwrap();
    let err = push(&client, 0, 1, 1).unwrap_err();
    assert!(matches!(err, StorageError::ReadOnly(_)), "read-only server: {err:?}");
    srv.shutdown();
}

#[test]
fn client_options_reject_unsafe_header_values() {
    let url = "http://127.0.0.1:9736";
    for bad in ["", "has space", "crlf\r\ninjected: x", "tab\there", "ünïcode", "del\x7f"] {
        assert!(
            HttpReplicaClient::with_options(url, HttpClientOptions {
                auth_token: Some(bad.into()),
                ..HttpClientOptions::default()
            })
            .is_err(),
            "auth_token {bad:?} should be rejected"
        );
        assert!(
            HttpReplicaClient::with_options(url, HttpClientOptions {
                writer_id: Some(bad.into()),
                ..HttpClientOptions::default()
            })
            .is_err(),
            "writer_id {bad:?} should be rejected"
        );
    }
    // The full visible-ASCII range is fine.
    HttpReplicaClient::with_options(url, HttpClientOptions {
        auth_token: Some("Abc123._~+/=-".into()),
        writer_id: Some("device-7f3a".into()),
        ..HttpClientOptions::default()
    })
    .unwrap();
}
