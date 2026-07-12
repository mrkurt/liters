# liters HTTP replication protocol — v1 (normative)

liters instances can serve a database's bucket over HTTP so other liters
instances restore from it and follow it live. This protocol is
**liters-proprietary**: stock litestream v0.5.x has no HTTP source scheme
(its supported schemes are file|s3|gs|abs|sftp|webdav|nats|oss), so there is
no Go oracle for it. This document is the spec; the implementation lives in
`crates/liters-storage/src/http/` behind the `http` cargo feature.

The server exposes a `ReplicaClient`-shaped view of one bucket: state lives
entirely in listings and filenames, exactly like every other litestream
backend. The one addition is `/stream`, a long-lived response that pushes new
level-0 LTX files to a follower over a single connection.

## Transport

- HTTP/1.1, `Connection: close` (one request per connection). Read
  endpoints are `GET`; a *writable* server (see Push below) additionally
  accepts `PUT`/`DELETE` on the write endpoints. Anything else is `405`.
- No TLS and no authentication in v1. Bind loopback or a private interface,
  or front with a reverse proxy. Proxies MUST NOT buffer `/stream`
  responses: the server sends `X-Accel-Buffering: no` and
  `Cache-Control: no-cache, no-transform`; for nginx set
  `proxy_buffering off` and `proxy_read_timeout` greater than the ping
  interval (default 15s).
- No percent-encoding anywhere: paths and query values are plain lowercase
  hex and decimal.
- Line-length cap: 8192 bytes for any protocol line (request line, header,
  listing line, frame line) in either direction.

### Versioning

- Every server response carries `x-liters-protocol: 1`. Clients MUST
  validate it on **every** response (not only streams): a missing header
  means "not a liters server", a different value means "incompatible
  protocol". This keeps proxies and foreign servers from being misparsed.
- Any change to the listing format, the frame grammar, the set of frame
  types, or the endpoint/method set bumps the version. The write endpoints
  (Push) are part of v1. The `level` field of `ltx` frames is the only
  in-version extension point (always `0` in v1).
- Servers ignore unknown query parameters, so older servers tolerate newer
  clients.

## Endpoints

TXIDs are always 16 lowercase hex digits (`%016x`), matching LTX filenames.
Level is a single decimal digit `0`–`9` (9 = the snapshot pseudo-level).

### `GET /`

Health check. `200 text/plain`, body `liters <version>\n`.

### `GET /ltx/{level}?seek={txid}&meta=1`

Listing of the level's LTX files, ascending by `(min_txid, max_txid)`,
filtered to `min_txid >= seek` (`seek` defaults to zero; `meta=1` requests
accurate created-at timestamps, which may cost extra backend calls — it maps
to `ReplicaClient::ltx_files(use_metadata: true)`).

`200 text/plain`, one line per file:

```
{min:016x}-{max:016x}.ltx {size} {created_ms}
0000000000000001-000000000000000c.ltx 4196 1783804999123
000000000000000d-000000000000000d.ltx 4196 -
```

`size` is the exact byte size (restore planning requires accurate sizes).
`created_ms` is milliseconds since the Unix epoch, or `-` when unknown.
A missing or empty level is `200` with an empty body, never an error.
Clients skip unparseable lines (the dir backend's stray-file stance).

### `GET /ltx/{level}/{min:016x}-{max:016x}.ltx?offset=N&size=M`

The file bytes, `200` with `Transfer-Encoding: chunked`. `offset`/`size`
(decimal, both default 0) select a byte range with `ReplicaClient` semantics:
`size == 0` means offset-to-EOF. A missing file is `404` — clients map it to
`StorageError::NotFound`, the re-plan-don't-fail signal for readers racing
compaction/retention.

Sequencing requirements (they preserve the dir backend's safety properties):

- The **client** sends the request and validates the status inside
  `open_ltx_file` (eager open): a 404 surfaces at open time, not mid-read.
- The **server** opens the backend reader *before* writing the status line
  and holds it for the whole transfer, so a restore that opened N plan files
  keeps N server-side FDs alive while retention deletes — the POSIX
  unlink-while-open guarantee, reproduced over HTTP.
- If the backend read fails mid-body, the server aborts the connection
  without terminating the chunked body; the client sees a truncated body
  and treats the fetch as failed (never as a short file).

### `GET /stream?seek={txid}`

Long-lived follow stream of level-0 files. `seek` is the first TXID wanted
(the follower's position + 1) and must be >= 1; `400` otherwise. `200` with
`Transfer-Encoding: chunked`; the body is a sequence of frames, flushed
eagerly. Text frame lines end in `\n`; `ltx` frame lines are followed by raw
binary.

```
liters-stream 1\n                                  preamble, exactly once
ltx {level} {min:016x} {max:016x} {size}\n         then exactly {size} raw bytes
ping {bucket_max:016x}\n                           keepalive, every ping interval
gap {next_min:016x}\n                              then clean end of stream
reset {bucket_max:016x}\n                          then clean end of stream
```

- `ltx` — one complete LTX file (level always 0 in v1). Files are sent in
  TXID order; a frame may *overlap* the follower's position (multi-TXID L0
  files written by stock litestream): followers apply any frame satisfying
  `is_contiguous(position, min, max)` (`min <= position+1 && max > position`)
  and skip frames with `max <= position`. The declared size is exact; a
  stream that cannot deliver exactly `size` bytes aborts the connection.
- `ping` — sent when idle. Carries the **bucket-wide max TXID across all
  levels** (the same divergence evidence `Replica`'s poll path uses):
  a follower seeing `0 < bucket_max < position` must drop the stream and
  resync via listings. `ping 0000000000000000` means the bucket is empty —
  a wipe-then-reseed window, *not* divergence.
- `gap` — the requested position was pruned at level 0; `next_min` is the
  oldest available L0 min TXID. Stream ends cleanly; the follower bridges
  through levels 1–8 (or a snapshot) via listings, then reconnects.
- `reset` — the bucket's max TXID is *below* the follower's position: wiped
  or reseeded. Checked at stream open, on every ping tick, and — because the
  newest L0 file always survives retention — on every idle re-list whose L0
  max trails the follower's position (poll-cadence detection, parity with
  the polling read path). Stream ends cleanly; the follower resyncs, where
  divergence handling (auto-reset or error) runs.
- A clean end of the chunked body without `gap`/`reset` means the server
  shut down; reconnect or fall back to polling.

An unparseable frame line — including a `ping`/`gap`/`reset` payload that is
not a valid 16-hex TXID — is a protocol error: drop the connection and
resync. Unknown frame types cannot be skipped (frames carry raw binary), so
new frame types require a version bump.

### Timing (informative defaults)

- Server ping interval: 15s. Server idle re-list interval: 1s (this bounds
  change latency when the bucket is written by an external process and no
  in-process notification tee is wired).
- Client stream tick: 1s (stop-flag responsiveness). Client dead-man: 45s
  without a single received byte — any byte counts as liveness, so a large
  frame that streams slowly but steadily is never killed. Mid-frame read
  timeouts never surface events; idle ticks are only reported between
  frames.
- Server write timeout: 60s per blocked write — a peer that accepts zero
  bytes for 60s is stalled or suspended; slow-but-progressing drains never
  trip it. To stay compatible with this, clients MUST drain file-GET bodies
  eagerly: the reference client spools whole-file reads to a local temp file
  before returning them, because a restore's k-way merge can otherwise park
  a connection unread for minutes.
- Listing responses always carry `Content-Length`, and clients reject short
  bodies — a cut connection must never be mistaken for a shorter listing
  (it could masquerade as divergence).

## Push (reversed roles)

A server started with `writable: true` **receives** replication: the remote
liters writer dials out and pushes, which is the deployment shape when the
writer sits behind NAT or on a mobile network and the receiver has the
stable address. The pusher is simply a `Writer` whose destination client is
an `HttpReplicaClient` — pushes, compaction, retention, and crash-resume all
flow through the same `ReplicaClient` calls, now carried over HTTP. The
sole-writer-per-prefix model is preserved: the pusher remains the only
writer and compactor of the bucket; the receiving server just materializes
it.

Write endpoints (rejected with `403` when the server is read-only, the
default):

- `PUT /ltx/{level}/{min:016x}-{max:016x}.ltx` — body is the complete LTX
  file, sent with `Content-Length` or chunked (the reference client sends
  chunked; the body length isn't known when streaming off the compactor).
  A body not starting with a valid 100-byte LTX header, or ending short of
  its declared length (truncation MUST be an error, never a shorter file),
  is `400`. On success the response body is the file's listing line
  (`{name} {size} {created_ms|-}\n`) so the pusher gets an authoritative
  `FileInfo`. Atomicity and idempotency are the receiving backend's: a
  connection cut mid-body leaves nothing behind, and re-pushing the same
  key is harmless — exactly the retry semantics writers rely on. Requests
  without `Content-Length` or chunked encoding are `411`. On error paths
  the server drains a bounded amount of unread body before closing so the
  rejection is deliverable; the reference client additionally reads the
  response after a mid-upload write failure, so a 403/400 surfaces as its
  message rather than a broken pipe.
- `DELETE /ltx/{level}/{min:016x}-{max:016x}.ltx` — `200`; deleting a
  missing file is also `200` (trait contract: not an error).
- `DELETE /all` — wipes the bucket. `200`.

Every accepted write wakes the server's `/stream` followers, so a writable
server is simultaneously a **relay**: devices push in, downstream replicas
stream out with no polling latency, and the receiving process can follow
its own server over loopback for a live local materialization.

Security: protocol v1 has no auth, and a writable server accepts
`DELETE /all` from anyone who can reach it. Only enable `writable` on
loopback/private interfaces, or behind a reverse proxy that authenticates
`PUT`/`DELETE`.

Server-side body timeout: a pushed body that stops flowing for 30s is
aborted (per-read timeout; slow-but-flowing uplinks are fine). The pusher
re-pushes on its next `push()` — PUTs are idempotent.

## Error mapping (client)

| HTTP | `StorageError` |
|---|---|
| 404 on a file GET | `NotFound { level, min, max }` (re-plan signal) |
| 403 on a write | `Other` with the server's "read-only" message |
| non-200 elsewhere | `Other` (message includes the status and error body) |
| missing/unknown `x-liters-protocol` | `Other` ("not a liters server" / version mismatch) |

## Serving

`HttpServer::bind(addr, Arc<dyn ReplicaClient>, opts)` serves any backend,
but the design target is a `DirReplicaClient` over the same local bucket the
`Writer` pushes to. Wrap the writer's client with
`HttpServer::notifying_client` so pushes wake `/stream` followers with no
poll latency:

```rust
let bucket = "/path/to/bucket";
let srv = HttpServer::bind("0.0.0.0:9736",
                           Arc::new(DirReplicaClient::new(bucket)),
                           HttpServerOptions::default())?;
let mut w = Writer::open("app.db",
                         srv.notifying_client(Box::new(DirReplicaClient::new(bucket))),
                         WriterOptions::default())?;
```

Without the tee (bucket written by another process — including stock Go
`litestream replicate`), streaming still works with up to `poll_interval`
latency. Serving an S3-backed `ReplicaClient` works but is not recommended:
that backend buffers whole objects in memory and serializes calls on a
private runtime.

## Following

```rust
let client = Box::new(HttpReplicaClient::new("http://host:9736")?);
let mut r = Replica::open("replica.db", client, ReplicaOptions::default());
r.sync()?;                       // one-shot restore / catch-up works as usual
r.follow(&stop, &FollowOptions { retry: Some(Duration::from_secs(1)), ..Default::default() })?;
```

`Replica::follow` runs `sync()` to catch up (full restore on first run,
gap-bridging, divergence handling), then applies streamed frames as they
arrive, persisting the position sidecar after every applied file. Every
stream anomaly — gap, reset, non-contiguous frame, CRC failure, divergence —
routes back through `sync()`, which owns the hardened recovery logic. Each
file is spooled and CRC-verified in full before any page touches the
replica, identical to the listing path.
