//! liters-native HTTP replication (feature `http`): serve a bucket to other
//! liters instances ([`HttpServer`]) and read/follow one over HTTP
//! ([`HttpReplicaClient`]). This is the **liters HTTP replication protocol**:
//! liters' own, *not* litestream's — stock litestream has no HTTP source
//! scheme, so nothing about this protocol is litestream-compatible (only the
//! LTX files it moves are). It is specified normatively in
//! docs/http-protocol.md. Zero-dependency by design: std::net + threads on
//! the server, and on the read side a hand-rolled HTTP/1.1 client
//! ([`StdNetTransport`]) behind a transport seam ([`HttpTransport`]). The seam
//! lets an embedder inject a different transport without adding dependencies to
//! this crate: the mobile FFI layer supplies one backed by the platform HTTP
//! client (Android's OkHttp) so that many followers to one authority coalesce
//! onto a single HTTP/2 connection, with the platform owning TLS, the trust
//! store, and keepalive. The protocol — request heads and `liters-stream`
//! framing — is identical across transports; only the bytes' carrier changes.

mod client;
pub mod mount;
mod server;
mod transport;
mod wire;

pub use client::{HttpClientOptions, HttpReplicaClient};
pub use mount::{Body, Mount, MountOptions, Request, Response, StreamBody};
pub use server::{HttpServer, HttpServerOptions};
pub use transport::{
    BodyRead, BodyReader, Cancel, HttpTransport, StdNetTransport, TransportBody, TransportRequest,
    TransportResponse,
};

/// Creates an anonymous (created then immediately unlinked) temp file in the
/// system temp dir, open read+write. The fd keeps the data alive; nothing is
/// left on disk regardless of how the file is dropped (unix). Used for
/// spooling whole bodies on both sides: the client spools file GETs so the
/// restore merge never parks a socket, the server spools PUT bodies so the
/// per-bucket write gate is never held across a network read.
fn unlinked_temp_file() -> std::io::Result<std::fs::File> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir();
    for _ in 0..16 {
        let path = dir.join(format!(
            "liters-http-{}-{:x}.spool",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        match std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&path) {
            Ok(f) => {
                // Unlink immediately; the open fd keeps it readable.
                let _ = std::fs::remove_file(&path);
                return Ok(f);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(std::io::Error::other("could not create spool file"))
}
