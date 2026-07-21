//! The HTTP replica client drives a pluggable `HttpTransport`. These tests
//! exercise the full client protocol layer — listing parse, file open (whole +
//! ranged), the `/stream` frame grammar with idle ticks, the mandatory
//! `x-liters-protocol` validation, and header emission — over a scripted
//! in-memory transport, i.e. exactly the path a foreign (mobile) transport
//! takes, with no sockets involved.
#![cfg(feature = "http")]

use std::collections::VecDeque;
use std::io::Read;
use std::sync::{Arc, Mutex};

use liters_storage::{
    BodyRead, HttpClientOptions, HttpReplicaClient, HttpTransport, ReplicaClient, StorageError,
    StreamEvent, TransportBody, TransportRequest, TransportResponse,
};
use ltx::Txid;

/// One scripted step of a response body.
enum Step {
    Data(Vec<u8>),
    Idle,
    Eof,
}

struct ScriptBody(VecDeque<Step>);

impl TransportBody for ScriptBody {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<BodyRead> {
        match self.0.front_mut() {
            None => Ok(BodyRead::Eof),
            Some(Step::Eof) => {
                self.0.pop_front();
                Ok(BodyRead::Eof)
            }
            Some(Step::Idle) => {
                self.0.pop_front();
                Ok(BodyRead::Idle)
            }
            Some(Step::Data(d)) => {
                let n = d.len().min(buf.len());
                buf[..n].copy_from_slice(&d[..n]);
                d.drain(..n);
                if d.is_empty() {
                    self.0.pop_front();
                }
                Ok(BodyRead::Bytes(n))
            }
        }
    }
}

/// A canned response.
struct Canned {
    status: u16,
    /// Whether to include the `x-liters-protocol: 1` header.
    protocol_header: bool,
    extra_headers: Vec<(&'static str, String)>,
    steps: Vec<Step>,
}

impl Canned {
    fn ok(body: &[u8]) -> Canned {
        Canned {
            status: 200,
            protocol_header: true,
            extra_headers: vec![("content-length", body.len().to_string())],
            steps: vec![Step::Data(body.to_vec())],
        }
    }
}

/// A recorded request: `(method, url, semantic headers)`.
type SeenRequest = (String, String, Vec<(String, String)>);

/// Routes each request to a canned response by a url substring key and records
/// the requests it saw (for header assertions).
struct MockTransport {
    responses: Mutex<Vec<(String, Canned)>>,
    seen: Mutex<Vec<SeenRequest>>,
}

impl MockTransport {
    fn new(responses: Vec<(&str, Canned)>) -> Arc<MockTransport> {
        Arc::new(MockTransport {
            responses: Mutex::new(
                responses.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            ),
            seen: Mutex::new(Vec::new()),
        })
    }
}

impl HttpTransport for MockTransport {
    fn execute(&self, req: TransportRequest<'_, '_>) -> Result<TransportResponse, StorageError> {
        self.seen.lock().unwrap().push((
            req.method.to_string(),
            req.url.to_string(),
            req.headers.clone(),
        ));
        // Match on a substring key so tests can key by endpoint.
        let mut responses = self.responses.lock().unwrap();
        let idx = responses
            .iter()
            .position(|(k, _)| req.url.contains(k.as_str()))
            .unwrap_or_else(|| panic!("no canned response for {} {}", req.method, req.url));
        let (_, canned) = responses.remove(idx);
        let mut headers: Vec<(String, String)> = Vec::new();
        if canned.protocol_header {
            headers.push(("x-liters-protocol".into(), "1".into()));
        }
        for (n, v) in canned.extra_headers {
            headers.push((n.to_string(), v));
        }
        Ok(TransportResponse {
            status: canned.status,
            headers,
            body: Box::new(ScriptBody(canned.steps.into())),
        })
    }
}

fn client(transport: Arc<dyn HttpTransport>) -> HttpReplicaClient {
    HttpReplicaClient::with_transport("http://host:9/db/x", HttpClientOptions::default(), transport)
        .unwrap()
}

#[test]
fn lists_files_through_transport() {
    let listing = "0000000000000001-0000000000000001.ltx 100 -\n\
                   0000000000000002-0000000000000003.ltx 200 1700000000000\n";
    let mock = MockTransport::new(vec![("/ltx/0?seek=", Canned::ok(listing.as_bytes()))]);
    let c = client(mock.clone());

    let files = c.ltx_files(0, Txid(0), false).unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!((files[0].min_txid, files[0].max_txid, files[0].size), (Txid(1), Txid(1), 100));
    assert_eq!((files[1].min_txid, files[1].max_txid, files[1].size), (Txid(2), Txid(3), 200));

    // The request went to the base path's /ltx endpoint.
    let seen = mock.seen.lock().unwrap();
    assert_eq!(seen[0].0, "GET");
    assert!(seen[0].1.starts_with("http://host:9/db/x/ltx/0?seek="), "{}", seen[0].1);
}

#[test]
fn opens_whole_file_through_transport() {
    let bytes = b"an ltx file body";
    let mock = MockTransport::new(vec![("/ltx/0/", Canned::ok(bytes))]);
    let c = client(mock);

    let mut rd = c.open_ltx_file(0, Txid(1), Txid(1), 0, 0).unwrap();
    let mut out = Vec::new();
    rd.read_to_end(&mut out).unwrap();
    assert_eq!(out, bytes);
}

#[test]
fn missing_file_maps_to_not_found() {
    let mut canned = Canned::ok(b"not found\n");
    canned.status = 404;
    let mock = MockTransport::new(vec![("/ltx/0/", canned)]);
    let c = client(mock);

    // `open_ltx_file`'s Ok type (`Box<dyn Read>`) is not `Debug`, so match.
    let err = match c.open_ltx_file(0, Txid(5), Txid(5), 0, 0) {
        Ok(_) => panic!("expected NotFound"),
        Err(e) => e,
    };
    assert!(
        matches!(err, StorageError::NotFound { min_txid: Txid(5), .. }),
        "expected NotFound, got {err:?}"
    );
}

#[test]
fn stream_frames_idle_and_close() {
    // One Data chunk carrying the preamble, one ltx frame + its 5-byte body,
    // then an idle tick, a ping, and a clean end.
    let body = b"liters-stream 1\nltx 0 0000000000000001 0000000000000001 5\nhello";
    let mock = MockTransport::new(vec![(
        "/stream?seek=",
        Canned {
            status: 200,
            protocol_header: true,
            extra_headers: vec![],
            steps: vec![
                Step::Data(body.to_vec()),
                Step::Idle,
                Step::Data(b"ping 0000000000000001\n".to_vec()),
                Step::Eof,
            ],
        },
    )]);
    let c = client(mock);

    let mut stream = c.open_ltx_stream(Txid(1)).unwrap().expect("stream");
    let mut sink = Vec::new();

    match stream.next(&mut sink).unwrap() {
        StreamEvent::Ltx(info) => {
            assert_eq!((info.min_txid, info.max_txid, info.size), (Txid(1), Txid(1), 5));
        }
        e => panic!("expected Ltx, got {e:?}"),
    }
    assert_eq!(sink, b"hello");

    // The idle step surfaces as a timeout tick with no bucket_max.
    assert_eq!(stream.next(&mut sink).unwrap(), StreamEvent::Idle { bucket_max: None });
    // The ping frame carries the bucket max.
    assert_eq!(
        stream.next(&mut sink).unwrap(),
        StreamEvent::Idle { bucket_max: Some(Txid(1)) }
    );
    // Clean end at a frame boundary.
    assert_eq!(stream.next(&mut sink).unwrap(), StreamEvent::Closed);
}

#[test]
fn rejects_response_without_protocol_header() {
    let mut canned = Canned::ok(b"hi\n");
    canned.protocol_header = false;
    let mock = MockTransport::new(vec![("/ltx/0?seek=", canned)]);
    let c = client(mock);

    let err = c.ltx_files(0, Txid(0), false).unwrap_err();
    assert!(
        matches!(err, StorageError::Other(msg) if msg.contains("not a liters server")),
        "expected protocol-header error"
    );
}

#[test]
fn sends_authorization_header() {
    let mock = MockTransport::new(vec![("/ltx/0?seek=", Canned::ok(b""))]);
    let opts = HttpClientOptions { auth_token: Some("s3cr3t".into()), ..Default::default() };
    let c = HttpReplicaClient::with_transport("http://host:9/db/x", opts, mock.clone()).unwrap();

    c.ltx_files(0, Txid(0), false).unwrap();

    let seen = mock.seen.lock().unwrap();
    let (_, _, headers) = &seen[0];
    assert!(
        headers.iter().any(|(n, v)| n == "authorization" && v == "Bearer s3cr3t"),
        "authorization header not sent: {headers:?}"
    );
}
