# liters

A Rust library, embeddable in iOS/Android apps, that speaks
[Litestream](https://litestream.io) v0.5.x's wire and bucket format:

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
| `liters-storage` | `ReplicaClient` trait; `dir` backend (litestream `file` layout) and `s3` backend (litestream S3 layout, feature `s3`) |
| `liters` | `Writer` (push pipeline, checkpointing, device-side compaction/retention) and `Replica` (restore + incremental follow) |
| `liters-ffi` | UniFFI bindings for Swift/Kotlin (`scripts/build-ios.sh`, `scripts/build-android.sh`) |

## Compatibility

The wire format is implemented from the `superfly/ltx` v0.5.1 **source** (the
version litestream v0.5.14 pins) and litestream's replica-client sources —
not from the docs, several of which are stale. Where the two disagree, the
oracle decides: the test suite builds the real Go `litestream` and `ltx`
binaries (`make oracle`) and asserts, among others, that

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
