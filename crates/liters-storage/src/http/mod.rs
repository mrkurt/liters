//! liters-native HTTP replication (feature `http`): serve a bucket to other
//! liters instances ([`HttpServer`]) and read/follow one over HTTP
//! ([`HttpReplicaClient`]). The wire protocol is liters-proprietary — stock
//! litestream has no HTTP source scheme — and is specified normatively in
//! docs/http-protocol.md. Zero-dependency by design: std::net + threads on
//! the server, a hand-rolled HTTP/1.1 GET client on the read side.

mod client;
mod server;
mod wire;

pub use client::{HttpClientOptions, HttpReplicaClient};
pub use server::{HttpServer, HttpServerOptions};

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
