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
- No TLS in v1. Bind loopback or a private interface, or front with a
  reverse proxy. Proxies MUST NOT buffer `/stream` responses: the server
  sends `X-Accel-Buffering: no` and `Cache-Control: no-cache,
  no-transform`; for nginx set `proxy_buffering off` and
  `proxy_read_timeout` greater than the ping interval (default 15s).
- Optional bearer-token authentication (see Authentication below). Without
  a configured token the server behaves exactly as before auth existed.
- No percent-encoding anywhere: paths and query values are plain lowercase
  hex and decimal (DB names are restricted to a safe charset — see
  Multiple databases).
- Line-length cap: 8192 bytes for any protocol line (request line, header,
  listing line, frame line) in either direction.

### Client transport is pluggable (HTTP/2, TLS, and mobile)

The protocol above is what rides *inside* an HTTP request/response. The client
does not mandate how those requests are carried, and the built-in
`Connection: close`/one-socket-per-request behavior is a property of the
default transport, not of the protocol. The framing — the request heads, the
listing body, and the `liters-stream` frames — is byte-identical regardless of
transport, so the protocol **version is not bumped** by any of this.

- **Default transport** (`StdNetTransport`): the hand-rolled HTTP/1.1 client,
  `http://` only, one `Connection: close` socket per request. This is what
  every Rust-native caller (CLIs, server-to-server replication, the test
  suite) uses.
- **Host-delegated transport** (mobile): an embedder may hand the client an
  `HttpTransport` that executes requests on the platform's own HTTP client
  instead of a socket. The liters-ffi layer exposes this as a Kotlin/Swift
  `HttpClient` the app implements over a **single shared** platform client
  (Android: one `OkHttpClient`). Because the app points every liters follow —
  and, ideally, its unrelated REST calls — at that one client, all requests to
  a given authority **coalesce onto a single HTTP/2 connection**: N followers
  become N concurrent h2 streams over one TCP+TLS connection with one keepalive,
  not N connections. The platform owns ALPN (`h2`, with `http/1.1` fallback),
  TLS and the system trust store (so `https://` URLs work with no Rust TLS
  dependency), connection pooling, keepalive PING, and flow control.
  - The long-lived `/stream` follow is one h2 stream carrying the frame body as
    DATA frames; `/ltx` fetches and listings are concurrent streams on the same
    connection. The host reports an idle read-timeout tick to liters (rather
    than failing) *only* for the follow request, so the client's ping/idle and
    dead-man logic (§Timing) are unchanged; short requests fail a stalled read
    normally.
  - A `PUT` push streams its body: the host pulls request-body bytes from liters
    as it writes them, so a push is never buffered whole.
  - The app must not set `Connection: close` and should let the platform keep
    the connection alive; raising the h2 initial connection/stream windows keeps
    a large `/ltx` transfer from starving the follow stream. A `421 Misdirected
    Request` (a server refusing a coalesced authority) is the platform client's
    to retry on a fresh connection; single-authority path-based mounts
    (`/db/<name>`) avoid it entirely.

Server-side HTTP/2 is independent of this: the built-in server speaks HTTP/1.1,
and a host-delegated client reaching it directly negotiates h1 (still pooled).
End-to-end multiplexing to a directly-connected liters server would require the
server to speak h2 (e.g. front it with an h2 edge/CDN that terminates h2 and
speaks h1 to the mount) — a deployment choice, not a protocol change.

### Versioning

- Every server response carries `x-liters-protocol: 1`. Clients MUST
  validate it on **every** response (not only streams): a missing header
  means "not a liters server", a different value means "incompatible
  protocol". This keeps proxies and foreign servers from being misparsed.
- Any change to the listing format, the frame grammar, the set of frame
  types, or the meaning of an existing endpoint/method bumps the version.
  The write endpoints (Push) are part of v1. The `level` field of `ltx`
  frames is the only in-version extension point (always `0` in v1).
- Purely *additive* surface does not bump the version: optional request
  headers (`authorization`, the fencing headers) and an operator-configured
  mount prefix (see Mount path) are ignored-or-404 on servers that predate
  them, and clients that never send them observe byte-identical behavior.
  Authentication, the mount prefix, and fencing are all such additions — a
  v1 server with none of them configured behaves exactly as before they
  existed.
- Servers ignore unknown query parameters, so older servers tolerate newer
  clients.

## Authentication

A server configured with an auth token (`HttpServerOptions::auth_token`)
requires

```
authorization: Bearer <token>
```

on **every** route except the `GET /` health check (liveness probes carry
no secrets; the mount-root probe `GET /{prefix}` IS gated). The scheme is
matched case-insensitively; the token must match exactly. Anything missing
or wrong is `401` with body `authorization required`; rejected `PUT` bodies
get the same bounded drain treatment as `403` so the response survives the
unread body.

Tokens (and writer ids, see Fencing) must be visible ASCII
(`0x21..=0x7e`); the reference client rejects anything else at
construction, since values are interpolated into request heads verbatim.

Auth without TLS protects against unauthorized *access*, not snooping —
the token crosses the wire in clear text. Front with a TLS-terminating
proxy when the transport is untrusted.

## Mount path

A server may be mounted under a URL path prefix
(`HttpServerOptions::base_path`, e.g. `/db`) so it can share an origin with
unrelated apps behind a path-routing reverse proxy — `http://host/db/...`
reaches liters while `http://host/users/...` reaches something else. Every
endpoint moves under the prefix:

```
/db/ltx/{level}...   /db/stream?seek=...   /db/all   etc.
```

- The prefix is stripped before routing, so the endpoint grammar below is
  unchanged beneath it.
- A request whose path is **not** under the prefix is `404 not found`
  (checked after auth, so the prefix layout is not probeable without the
  token). PUT bodies on such requests get the same bounded drain as any
  other error, so a misdirected pusher receives a real status.
- Leading/trailing slashes are optional and interior `//` is collapsed;
  no prefix, `""`, and `/` all mean "mounted at root", byte-identical to a
  server without the option.
- The bare-root `GET /` health check answers regardless of the prefix
  (liveness probes on the raw listener). `GET /{prefix}` answers the version
  line — a "does this mount resolve" probe, gated by auth when configured.
- Clients need nothing new: the base path in the client URL
  (`http://host:port/db`) is prepended to every request target, so a
  mounted server is addressed exactly like a root-mounted one at a deeper
  URL. If a reverse proxy instead *strips* the prefix before forwarding,
  leave `base_path` unset and give clients the external URL — the two
  approaches are mutually exclusive (don't strip at the proxy *and*
  configure the prefix on the server).

## Serving several databases

A server serves exactly **one** database. To serve several, run several
servers (each `HttpServer::bind` opens its own listener/port), or — to keep
one listener — embed the transport-agnostic handler `Mount` in your own Rust
HTTP server and let its router dispatch by path (see Serving). Each mount is
fully independent: its own writer lease, its own `/stream` change signal (a
push to one never wakes another's followers), its own auth/writable config.
Give each a distinct URL — a distinct port, or a distinct mount prefix
(Mount path) that a reverse proxy or your router maps to the right mount —
and clients address it through the client URL's base path.

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

The client-side values are the `HttpClientOptions` defaults
(`connect_timeout` 10s, `io_timeout` 30s, `stream_deadman` 45s) and are
configurable per client; the wire protocol does not depend on them.

- Server ping interval: 15s. Server idle re-list interval: 1s (this bounds
  change latency when the bucket is written by an external process and no
  in-process notification tee is wired).
- Client stream tick: 1s (cancellation responsiveness). Client dead-man:
  45s without a single received byte — any byte counts as liveness, so a
  large frame that streams slowly but steadily is never killed. Mid-frame
  read timeouts never surface events; idle ticks are only reported between
  frames.
- Client socket writes tick at 2s so an in-flight upload notices
  cancellation promptly; a write making zero progress for a cumulative
  `io_timeout` (default 30s) fails as transient. Any progress resets the
  budget — slow-but-flowing uplinks are never killed.
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
  key with the same bytes is harmless — exactly the retry semantics
  writers rely on. (Re-pushing an existing L0 key with *different* bytes
  is `409`, see Fencing.) The reference server receives the body in full
  into a local spool before committing, so a slow uplink never blocks
  other requests on the same bucket. Requests without `Content-Length` or
  chunked encoding are `411`. On **every** error path — including `401`,
  `409`, and even `404` for an unmatched path or `405` for a foreign
  method — the server drains a bounded amount of unread body before
  closing so the rejection is deliverable (an RST from unread bytes could
  destroy the response and make a permanent error look transient); the
  reference client additionally reads the response after a mid-upload
  write failure, so a 401/403/409/400 surfaces as its message rather than
  a broken pipe.
- `DELETE /ltx/{level}/{min:016x}-{max:016x}.ltx` — `200`; deleting a
  missing file is also `200` (trait contract: not an error).
- `DELETE /all` — wipes the bucket. `200`.

Every accepted write wakes that bucket's `/stream` followers, so a
writable server is simultaneously a **relay**: devices push in, downstream
replicas stream out with no polling latency, and the receiving process can
follow its own server over loopback for a live local materialization.

Security: an unauthenticated writable server accepts `DELETE /all` from
anyone who can reach it. Only enable `writable` on loopback/private
interfaces, behind an authenticating reverse proxy, or with an auth token
configured (see Authentication).

Server-side body timeout: a pushed body that stops flowing for 30s is
aborted (per-read timeout; slow-but-flowing uplinks are fine). The pusher
re-pushes on its next `push()` — PUTs are idempotent.

### Fencing

Writable servers gate every write (`PUT`, `DELETE`, `DELETE /all`) per
bucket, after auth and before the backend is touched. Rejections are `409`
with a one-line reason; rejected `PUT` bodies get the bounded drain
treatment. Two independent rules:

**Writer lease** — applies only to requests carrying

```
x-liters-writer-id: <id>           (visible ASCII, like the auth token)
x-liters-writer-takeover: 1        (optional)
```

The bucket remembers the last identified writer and when it last
**successfully** wrote. An identified write is allowed to proceed iff no
lease is held, the holder is this id, the lease is older than the TTL
(`HttpServerOptions::lease_ttl`, default 24h), or the request carries the
takeover header. Otherwise `409` with body `bucket is owned by writer
<id>` (only the owner's id is revealed). The lease is taken/refreshed
**only when the write is accepted and committed**: a rejected (`409`,
`400`) or failed (`500`) request never creates a lease and never
refreshes one — a stray misdirected push cannot fence out the legitimate
writer, and a client stuck retrying rejected pushes cannot keep its own
lease alive past the TTL. Requests **without** the header skip lease
logic entirely — plain v1 pushers are unaffected — but still face
monotonicity.

The lease is **in-memory and resets on server restart**: fencing is a
dual-writer *detector*, not a distributed lock. The monotonicity rule
below is what protects bucket integrity across restarts.

**TXID monotonicity** — applies to every `PUT` (headerless or not):

- Level 0 is the replication log. With `cur` = the bucket's max L0 TXID,
  a `PUT` is accepted iff L0 is empty, or `min == cur + 1` (an append), or
  the exact `{min,max}` file already exists **and the pushed body is
  byte-identical to the stored file** (idempotent re-push — writer
  crash-retry resends the same local file). A same-key push with
  *different* content is `409`: it is the dual-writer signature (two
  devices restored from the same backup racing the same position), and
  accepting it would silently splice a divergent lineage under history
  already served to followers. Equality is checked size-first (from the
  listing), then byte-wise against the stored file — cheap on the
  directory backing, one extra object read on remote backings, and only
  on the rare re-push path. Any other range is `409` with body
  `non-monotonic L0 push: <min>-<max> offered, bucket at <cur>`.
- Levels 1–9 only ever summarize **uploaded** history (compactions,
  snapshots): a `PUT` is accepted iff `max <=` the bucket-wide max TXID
  across all levels. (An exact re-push trivially satisfies this.)
  Otherwise `409` with a body naming the offered range and the bucket
  max. This imposes an **upload-ordering contract on pushers**: push the
  L0 backlog first, then compactions/snapshots — a snapshot taken at a
  local position ahead of the uploaded L0s must not land before the L0s
  that justify it (the liters `Writer` uploads its L0 backlog before any
  higher-level push). Same-key L1–9 overwrites are permitted within that
  bound — recompaction of the same range is normal.
- `DELETE`s face only the lease rule — deletes are retention/GC, which
  monotonicity does not constrain.

**Atomicity** — the fence decision and the backend commit are atomic per
bucket: mutations of one bucket are serialized by the server, and the
authoritative fence is evaluated against fresh listings inside that
critical section, *after* the request body has been fully received into a
local spool (the write lock is never held across a network read). An
advisory copy of the fence also runs before the body is received, purely
to reject doomed pushes early; passing it commits nothing. Two concurrent
`PUT`s for the same TXID therefore yield exactly one `200` — the other
gets `409` and knows its push was not accepted.

Cost: evaluating the gate takes one listing per fence pass against the
backing client (L0 for L0 pushes; all levels for higher-level pushes);
`PUT`s pay it twice (advisory + authoritative). The design target is a
directory backing where a listing is one readdir.

A fenced-out or non-monotonic writer sees `409` as a non-retryable
conflict; the correct reaction is to re-check its lineage against the
bucket (the liters `Writer` rebaselines when the bucket has moved ahead),
not to retry the same file.

## Error mapping (client)

| condition | `StorageError` |
|---|---|
| 404 on a file GET | `NotFound { level, min, max }` (re-plan signal) |
| 401 | `Unauthorized` (message includes the server's body excerpt) |
| 403 on a write | `ReadOnly` with the server's "read-only" message |
| 409 | `Conflict` (fencing: lease or monotonicity, see Fencing) |
| non-200 elsewhere | `Other` (message includes the status and error body) |
| resolve/connect failure, timeout, reset, broken pipe, EOF mid-body, dead-man, truncated listing | `Unavailable` (transient — safe to retry) |
| missing/unknown `x-liters-protocol`, malformed frames or listings | `Other` (protocol error — not transient) |
| cancelled via `CancelToken` | `Cancelled` (never misreported as `Unavailable`) |

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

`HttpServer` is a thin `TcpListener` driver over `Mount`, the
transport-agnostic handler that *is* the protocol for one database. To serve
liters from your own Rust HTTP server (one listener shared with unrelated
routes, or several databases dispatched by path), build a `Mount` and call
`Mount::handle` per request:

```rust
let mount = Mount::new(Arc::new(DirReplicaClient::new(bucket)), MountOptions::default());
let mut w = Writer::open("app.db",
                         mount.notifying_client(Box::new(DirReplicaClient::new(bucket))),
                         WriterOptions::default())?;
// in your router, for a request the router has matched to this mount:
let resp = mount.handle(Request {
    method, path,      // `path` has your mount prefix already stripped
    query, headers,
    body: &mut req_body,   // decoded (de-chunked / length-bounded); only read for PUT
    cancel,                // cancels a parked /stream
});
// write resp.status + resp.headers (+ your framing), then the Body:
//   Body::Bytes(b)   -> content-length body
//   Body::Reader(r)  -> stream it out (chunked); a read error means abort, not a short file
//   Body::Stream(s)  -> s.write_to(&mut your_chunked_sink); Ok = clean end, Err = abort
```

All wire-format knowledge (status codes, listing lines, the frame grammar,
the `x-liters-protocol` header) lives in `Mount`; the host supplies parsed
request parts and pumps bytes. `/stream` and file `GET`s must egress as
HTTP/1.1 chunked, unbuffered (the reference client requires a chunked
reader). See the `mount` module docs.

## Following

```rust
let client = Box::new(HttpReplicaClient::new("http://host:9736")?);
let mut r = Replica::open("replica.db", client, ReplicaOptions::default());
r.sync()?;                       // one-shot restore / catch-up works as usual
r.follow(&stop, &FollowOptions { retry: Some(Backoff::default()), ..Default::default() })?;
```

Auth tokens, fencing identity, and timeouts are per-client options:

```rust
let client = Box::new(HttpReplicaClient::with_options(
    "http://host:9736/db",   // base path if the server is mounted under /db
    HttpClientOptions { auth_token: Some(token), ..HttpClientOptions::default() },
)?);
```

`Replica::follow` runs `sync()` to catch up (full restore on first run,
gap-bridging, divergence handling), then applies streamed frames as they
arrive, persisting the position sidecar after every applied file. Every
stream anomaly — gap, reset, non-contiguous frame, CRC failure, divergence —
routes back through `sync()`, which owns the hardened recovery logic. Each
file is spooled and CRC-verified in full before any page touches the
replica, identical to the listing path.
