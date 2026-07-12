//! liters-native HTTP replication (feature `http`): serve a bucket to other
//! liters instances ([`HttpServer`]) and read/follow one over HTTP
//! ([`HttpReplicaClient`]). The wire protocol is liters-proprietary — stock
//! litestream has no HTTP source scheme — and is specified normatively in
//! docs/http-protocol.md. Zero-dependency by design: std::net + threads on
//! the server, a hand-rolled HTTP/1.1 GET client on the read side.

mod client;
mod server;
mod wire;

pub use client::HttpReplicaClient;
pub use server::{HttpServer, HttpServerOptions};
