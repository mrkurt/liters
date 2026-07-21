# liters

A Rust library, embeddable in iOS/Android apps, that reads and writes
[Litestream](https://litestream.io) v0.5.x's LTX file format and bucket
layout:

- **Produce**: replicate a local, app-owned SQLite database to object storage
  as LTX files. Buckets written by liters restore with stock
  `litestream restore`.
- **Consume**: maintain a local read-only replica of any litestream bucket,
  applying new LTX files incrementally.

Replication is driven by explicit calls — `push()` after commits, `sync()` to
pull — because mobile apps own their write timing and background execution
windows. There is no daemon and no file watching. Every operation is short,
resumable, and crash-safe, sized for iOS `BGTaskScheduler` / Android
`WorkManager` budgets.

```rust
use liters::{Writer, WriterOptions, Replica, ReplicaOptions, DirReplicaClient};

// Write side: the app owns app.db and calls push() after commits.
let mut w = Writer::open("app.db", Box::new(DirReplicaClient::new("/bucket")),
                         WriterOptions::default())?;
w.push()?;                                  // WAL → L0 LTX → upload
w.maintain(&Default::default())?;           // compaction/snapshots/retention, when due

// Read side: a live-updating local materialization of a bucket.
let mut r = Replica::open("replica.db", Box::new(DirReplicaClient::new("/bucket")),
                          ReplicaOptions::default());
r.sync()?;                                  // restore on first call, then incremental
// open replica.db read-only with any SQLite
```

## Crates

| crate | contents |
|---|---|
| `ltx` | LTX v0.5.1 codec: encoder, decoder, page index, k-way compactor, CRC-64/GO-ISO checksums |
| `liters-wal` | SQLite WAL reader: salt/checksum-verified frames, committed-transaction page maps |
| `liters-storage` | `ReplicaClient` trait; `dir` backend (litestream `file` layout), `s3` backend (litestream S3 layout, feature `s3`), and liters-native HTTP serving + source with auth, a mountable `Mount` handler, and write fencing (feature `http`) |
| `liters` | `Writer` (push pipeline, checkpointing, device-side compaction/retention), `Replica` (restore + incremental follow), and `Manager` (background replication for N databases with sleep/resume) |
| `liters-ffi` | UniFFI bindings for Swift/Kotlin: `LitersWriter`, `LitersReplica`, `LitersManager` + event listener (`scripts/build-ios.sh`, `scripts/build-android.sh`) |

## HTTP replication (liters-native)

With the `http` feature, a liters process can serve its bucket over HTTP and
other liters instances can restore from it and **follow it live** — new
transactions stream to followers over a single long-lived connection, with
no object store in between. The files on the move are still litestream-format
LTX, but the protocol carrying them is liters' own: stock litestream v0.5.x
has no HTTP scheme, so this is the **liters HTTP replication protocol** —
liters-proprietary, not a litestream protocol — specified normatively in
[docs/http-protocol.md](docs/http-protocol.md).

```rust
use std::sync::Arc;
use liters_storage::{DirReplicaClient, HttpReplicaClient, HttpServer, HttpServerOptions};

// Serving side: writer pushes to a local dir bucket; the server serves it.
// The notifying tee wakes followers the instant a push lands.
let srv = HttpServer::bind("0.0.0.0:9736", Arc::new(DirReplicaClient::new("/bucket")),
                           HttpServerOptions::default())?;
let mut w = Writer::open("app.db", srv.notifying_client(Box::new(DirReplicaClient::new("/bucket"))),
                         WriterOptions::default())?;

// Following side: restore over HTTP, then apply changes as they arrive.
let mut r = Replica::open("replica.db", Box::new(HttpReplicaClient::new("http://host:9736")?),
                          ReplicaOptions::default());
let cancel = CancelToken::new();
r.follow(&cancel, &FollowOptions::default())?;   // blocks; cancel() ends it cleanly
```

`Replica::sync()` polling works over HTTP too (the server is a full
read-side `ReplicaClient`), and the server can serve a bucket some other
process writes — including stock `litestream replicate` — with poll-bounded
latency. TLS is not built in: front with a reverse proxy (auth below).

Roles also reverse (**push replication**): a server started with
`writable: true` *accepts* replication, and a writer whose destination is an
`HttpReplicaClient` dials out and pushes — the shape you want when the
writer is behind NAT or on a mobile network:

```rust
// Receiver: listens, materializes pushed data into a local bucket.
let srv = HttpServer::bind("0.0.0.0:9736", Arc::new(DirReplicaClient::new("/bucket")),
                           HttpServerOptions { writable: true, ..Default::default() })?;

// Pusher (elsewhere): a normal Writer, destination is the receiver.
let mut w = Writer::open("app.db", Box::new(HttpReplicaClient::new("http://receiver:9736")?),
                         WriterOptions::default())?;
w.push()?;      // L0 upload over HTTP; maintain() compacts/retains remotely too
```

Accepted pushes wake the receiver's `/stream` followers, so a writable
server is also a relay: devices push in, downstream replicas stream out,
and the receiver can follow its own server over loopback for a live local
copy. The pushed bucket stays litestream-exact — stock `litestream restore`
works against it.

### Mounting, embedding, auth, fencing

A server serves one database. Mount it under a URL path prefix
(`HttpServerOptions::base_path`) so liters can share an origin with unrelated
apps behind a path-routing reverse proxy — `http://host/db/...` reaches
liters while `http://host/users/...` reaches something else. Requests outside
the prefix are `404`; clients just point at the deeper URL, and the base path
they already parse from the URL lines up with the server's mount.
(Alternatively, strip the prefix at the proxy and leave `base_path` unset —
do one or the other, not both.) With `auth_token` set, every route except the
`GET /` health check requires `authorization: Bearer <token>`.

```rust
let srv = HttpServer::bind("0.0.0.0:9736", Arc::new(DirReplicaClient::new("/bucket")),
                           HttpServerOptions { base_path: Some("/db".into()),
                                               auth_token: Some("secret".into()),
                                               ..Default::default() })?;
let client = HttpReplicaClient::new("http://host:9736/db")?;
```

To serve liters from **your own** Rust HTTP server — one listener shared with
other routes, or several databases dispatched by path — skip `HttpServer` and
embed `Mount`, the transport-agnostic handler that is the protocol for one
database. `HttpServer` is itself just a thin `TcpListener` driver over one:

```rust
let mount = Mount::new(Arc::new(DirReplicaClient::new("/bucket")), MountOptions::default());
// in your router, for a request matched to this mount (prefix already stripped):
let resp = mount.handle(Request { method, path, query, headers,
                                  body: &mut req_body, cancel });
// write resp.status + resp.headers, then the Body (Bytes / Reader / Stream).
```

`Mount::handle` is a pure request-in / response-out function: it owns all
wire-format knowledge; your host owns the socket, the listener, and routing.

Writable servers also *fence* pushers: a writer that sends an
`x-liters-writer-id` header (`HttpClientOptions::writer_id`; the `Manager`
fills it automatically with a per-database persisted id) holds a lease on
its bucket, and pushes from other writer ids are rejected with 409 until
the lease ages out (`HttpServerOptions::lease_ttl`, default 24h) or a
takeover is forced. Leases are in-memory — a dual-writer detector, not a
distributed lock; L0 TXID monotonicity checks protect bucket integrity
across server restarts. Details in
[docs/http-protocol.md](docs/http-protocol.md).

## Manager: many databases, sleep/resume

For apps replicating several databases, `liters::Manager` runs one worker
thread per registered database — pushing on an interval (or only on
`push_now()`) or following live — with transient failures retried on a
jittered exponential `Backoff` and fatal errors parked in a `Failed` state
you can observe and nudge:

```rust
use std::time::Duration;
use liters::{Manager, ManagerOptions, PushConfig, FollowConfig, StorageConfig};

let mgr = Manager::new(ManagerOptions::default());
mgr.register_push("app", "app.db", PushConfig {
    storage: StorageConfig::Http {
        url: "http://sync.example:9736/db".into(),
        options: liters::HttpClientOptions {
            auth_token: Some("secret".into()),
            ..Default::default()
        },
    },
    writer_options: Default::default(),
    push_interval: Some(Duration::from_secs(30)),
    maintenance: Some((Default::default(), Duration::from_secs(3600))),
    backoff: None,
})?;
mgr.register_follow("catalog", "catalog.db", FollowConfig {
    storage: StorageConfig::Dir { path: "/buckets/catalog".into() },
    replica_options: Default::default(),
    follow_options: Default::default(),
})?;

mgr.set_observer(Some(observer));  // state changes, push completions, errors
mgr.statuses();                    // per-DB state, position, last error

// Mobile power management: sleeping cancels in-flight transfers and drops
// the Writer — releasing the WAL read lock and every fd — then parks with
// zero storage traffic; resume schedules an immediate catch-up round.
mgr.sleep_all();                   // app went to background
mgr.resume_all();                  // app returned to foreground
```

Writers open offline: pushes convert the WAL into local L0 files even with
the bucket unreachable, and the first successful push uploads the backlog.
Every long operation also has a `_with` variant taking a `CancelToken`
(`push_with`, `sync_with`, `maintain_with`, `follow`); flipping the token
makes the call return `Error::Cancelled` promptly — mid-transfer on the
HTTP backend — and retrying after a cancel is indistinguishable from
resuming after a crash. Tokens are one-shot: a cancelled token stays
cancelled, and a new session gets a fresh one. This is the mechanism behind
`sleep()`/`resume()` and bounded shutdown.

The same surface ships over FFI: `liters-ffi` exports `LitersWriter` /
`LitersReplica` (with `cancel()` and `close()`) and `LitersManager`
(register/sleep/resume/status plus a `ManagerListener` callback interface —
callbacks arrive on worker threads and must not block). Live follow over
FFI goes through `LitersManager.register_follow`.

## Compatibility

Liters has two compatibility surfaces, and only one is litestream's. The
**LTX file format and bucket layout** are litestream v0.5-compatible,
implemented from the `superfly/ltx` v0.5.1 **source** (the version litestream
v0.5.14 pins) and litestream's replica-client sources — not from the docs,
several of which are stale. (The **liters HTTP replication protocol** above is
the other surface: liters' own, interoperating only with other liters
instances, moving litestream-format LTX files over a protocol litestream does
not define.) Where ltx source and docs disagree, the oracle decides: the test
suite builds the real Go `litestream` and `ltx` binaries (`make oracle`) and
asserts, among others, that

- every push is restorable by `litestream restore` (file and S3/MinIO),
- Rust-encoded LTX files pass Go `ltx verify` and apply byte-identically,
- liters continues seamlessly from a database litestream was replicating
  (same meta-dir layout) and vice versa,
- the reader follows buckets written by live `litestream replicate`,
  surviving compaction races, pruned levels, and bucket reseeds.

Run everything with `make test` (Go toolchain required for the oracle; tests
skip gracefully without it). `docs/research/` holds the format/internals
notes the implementation was built from.

## Design notes

- Files are written with litestream's `HeaderFlagNoChecksum` — the pre/
  post-apply checksum chain is inert in real litestream buckets, and enabling
  it would break Go's restore path (its compactor rejects mixed checksums).
  Continuity is TXID contiguity plus per-file CRC-64, exactly as upstream.
- The writer holds litestream's long-running read transaction, so it **must**
  checkpoint the database itself; thresholds mirror upstream defaults.
- The reader verifies each fetched file's CRC **before** any page touches the
  live replica, falls back to applying a newer snapshot in place when
  levels 0–8 are pruned, and detects bucket reseeds as divergence — three
  deliberate hardenings over upstream's follow mode.
- Compaction/retention run device-side (`Writer::maintain`): each device is
  the sole writer of its prefix, hence also its sole compactor. Retention
  preserves the invariants stock readers rely on (newest snapshot kept, ≥1
  file per level, no L0 gaps).
