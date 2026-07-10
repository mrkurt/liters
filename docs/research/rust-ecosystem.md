All research complete. Compiling the final report.

# Rust Ecosystem Survey for `liters` (Litestream v0.5.x–compatible embedded replication)

## 0. Ground truth: the wire format liters must match (extracted from local reference checkout + superfly/ltx@main)

Local reference: `/Users/kurt/code/liters/reference/litestream` (litestream `main`, go.mod pins `github.com/superfly/ltx v0.5.1`, `github.com/pierrec/lz4/v4 v4.1.22`, `hash/crc64` ISO).

### 0.1 LTX file format (ltx v0.5.1, `Version = 3`)
Constants (superfly/ltx `ltx.go`): `Magic = "LTX1"`, `HeaderSize = 100`, `PageHeaderSize = 6`, `TrailerSize = 16`, `ChecksumSize = 8`, `MinPageSize = 512`, `MaxPageSize = 65536` (powers of 2). All integers **big-endian**.

Header (100 bytes; offsets):

| off | size | field | type |
|---|---|---|---|
| 0 | 4 | Magic `"LTX1"` | [4]u8 |
| 4 | 4 | Flags | u32 |
| 8 | 4 | PageSize | u32 |
| 12 | 4 | Commit (db size in pages after apply) | u32 |
| 16 | 8 | MinTXID | u64 |
| 24 | 8 | MaxTXID | u64 |
| 32 | 8 | Timestamp (ms since epoch) | i64 |
| 40 | 8 | PreApplyChecksum | u64 |
| 48 | 8 | WALOffset | i64 |
| 56 | 8 | WALSize | i64 |
| 64 | 4 | WALSalt1 | u32 |
| 68 | 4 | WALSalt2 | u32 |
| 72 | 8 | NodeID | u64 |
| 80 | 20 | reserved (zero) | — |

Header flags: `HeaderFlagNoChecksum = 1<<1 (0x2)`; `HeaderFlagMask = 0x2` (the old whole-file `COMPRESS_LZ4 = 0x1` flag from LTX v0.1 is **gone**; a v0.5 reader must reject it). **Litestream always sets `HeaderFlagNoChecksum`** on every LTX it writes: `db.go:1948`, `db.go:2412`, `replica.go:682`, `compactor.go:165`, `vfs.go:706` — so Pre/PostApply checksum enforcement is relaxed (litepages fork exists precisely because upstream litetx rejects such files).

Page block (per page frame):
- PageHeader: `Pgno u32` @0, `Flags u16` @4. `PageHeaderFlagSize = 1<<0` ⇒ a 4-byte BE size field follows the 6-byte header giving the **compressed** payload length.
- Page data is **LZ4 block-compressed unconditionally** by the v0.5 encoder (`enc.compressor.CompressBlock(data, buf)` — pierrec/lz4 raw block format, not frame), `hdr.Flags |= PageHeaderFlagSize`, then 4-byte BE compressed length, then compressed bytes. The running file hash is fed the **uncompressed** page data. Decoder (`DecodePageData`): reads `dataSize = BE u32 at [PageHeaderSize..]`, `lz4.UncompressBlock` into a 65536-byte buffer. VERIFY during impl: behavior when `CompressBlock` returns 0 (incompressible input) — whether frame is stored raw with flag unset.
- Snapshot files (contain lock page? no — lock page excluded): pgnos must start at 1 and be sequential; non-snapshots strictly increasing. Lock page = `(PENDING_BYTE=0x40000000 / pageSize) + 1` is skipped.

Page index (new in v0.5; enables point page reads from object storage):
- After page frames: repeated uvarint triples `(pgno, offset, size)` in ascending pgno order, terminated by a single zero uvarint, followed by **8-byte BE index byte-length**, then the 16-byte trailer. `offset`/`size` are **absolute file offsets** of the whole frame (page header + size field + compressed data): `Offset: offset, Size: enc.n - offset`.
- Reader locates it from the end: `indexSize = BE u64 at [fileLen - TrailerSize - 8 .. fileLen - TrailerSize]`, index bytes at `[fileLen - TrailerSize - 8 - indexSize, ...)` — see `replica_client.go:116-145` (`fetchPageIndexData`, esp. line 134) and `FetchPageIndex` at `replica_client.go:90-97`; `DecodePageIndex(r, level, minTXID, maxTXID) -> map[u32]PageIndexElem{Level, MinTXID, MaxTXID, Offset, Size}`.

Trailer (16 bytes): `PostApplyChecksum u64` @0, `FileChecksum u64` @8 (`TrailerChecksumOffset = 8`). Decoder verifies `ChecksumFlag | crc64_sum == trailer.FileChecksum`.

Checksums:
- `type Checksum = u64`, `ChecksumFlag = 1<<63` OR'd into every stored checksum (guarantees non-zero).
- CRC-64 with **Go's `crc64.ISO` table** (`crc64.MakeTable(crc64.ISO)`, poly `0xD800000000000000` reversed form). Digest params = catalog **CRC-64/GO-ISO**: width=64, poly=0x000000000000001B, init=0xFFFFFFFFFFFFFFFF, refin=true, refout=true, xorout=0xFFFFFFFFFFFFFFFF, check("123456789")=0xB90956C775A41001. Rust: `crc::Crc::<u64>::new(&crc::CRC_64_GO_ISO)` — this is exactly what superfly's own ltx-rs uses, confirming equivalence.
- Per-page: `ChecksumPage(pgno, data) = ChecksumFlag | crc64(BE(pgno as u32) || data)`.
- Rolling DB checksum: XOR-accumulate per-page checksums: `chksum = ChecksumFlag | (chksum ^ ChecksumPage(pgno, data))` (XOR makes it order-independent/incremental). Litestream's full-DB variant: `db.go:2747-2772` (`crc64.New(crc64.MakeTable(crc64.ISO))` over raw db file).
- FileChecksum: running crc64 over encoded stream with uncompressed page data (exact byte coverage of header/trailer fields — VERIFY against `encoder.go`/`decoder.go` when implementing).

Filenames: `FormatFilename(min, max) = "%016x-%016x.ltx"` (e.g. testdata `000000000000000d-000000000000000d.ltx`); parse regex `^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$`.

### 0.2 Bucket layout & client protocol (litestream v0.5.x)
- **S3 key scheme**: `{configured-path}/{level:%04x}/{minTXID:%016x}-{maxTXID:%016x}.ltx` — note level is **4-digit hex** on remote storage (`s3/replica_client.go:655,703,1070`), while the *local* cache dir uses `ltx/{level-decimal}/` (`litestream.go:184-197`). Snapshot level ⇒ `0009/`.
- **Compaction levels** (`compaction_level.go:9-19`): `SnapshotLevel = 9`; defaults L0 raw (interval 0), L1 = 30 s, L2 = 5 min, L3 = 1 h. Level intervals compact lower-level files into wider TXID ranges; snapshots (level 9) are full-db LTX files, not contiguous (`store.go:832`).
- **Object metadata**: on PUT, litestream sets S3 metadata `litestream-timestamp` = header Timestamp as RFC3339Nano (`s3/replica_client.go:55`, WriteLTXFile) — used for timestamp-based restore (`LTXFiles(..., useMetadata bool)` does HEADs when true).
- **Ranged GETs**: `OpenLTXFile(level,min,max,offset,size)` → `Range: bytes={offset}-{offset+size-1}` (or `bytes={offset}-` when size==0). Header peek = range `[0,100)`; page reads = `[elem.Offset, elem.Offset+elem.Size)`. Reader requires: GET, ranged GET, LIST-with-prefix (per level), PUT, DELETE (batch), HEAD (optional metadata). No conditional puts required for basic compat, but litestream HA leasing (`leaser.go`) uses them; liters' single-writer-per-db model can use `If-None-Match: *` to detect split-brain pushes.
- **ReplicaClient interface to reimplement** (`replica_client.go:24-44`): `LTXFiles(level, seekTXID, useMetadata) -> iterator`, `OpenLTXFile(level, min, max, offset, size)`, `WriteLTXFile(level, min, max, reader) -> FileInfo`, `DeleteLTXFiles([]FileInfo)`, `DeleteAll()`.
- **Restore algorithm** (`replica.go:1441` `CalcRestorePlan`): pick latest level-9 snapshot with `MaxTXID <= targetTXID` (or CreatedAt < timestamp); then greedily walk levels 8→0 with per-level cursors, repeatedly choosing the candidate file that extends the longest contiguous TXID range from `currentMax` (`restoreCandidateBetter`), requiring `candidate.MaxTXID > currentMax`; stop at target. Invariant: applied LTX files must form a contiguous TXID chain from the snapshot; apply = write each frame's pages at `(pgno-1)*pageSize`, truncate to `Commit*pageSize`.
- **Read-replica VFS** (`vfs.go`): SQLite VFS backed by ReplicaClient; per-file LRU page cache (`vfs.go:523`, hashicorp/golang-lru), builds a merged pgno→PageIndexElem map by layering page indexes from snapshot upward (`vfs.go:1258,2655`), serves reads via `FetchPage` ranged GETs (`vfs.go:817,1500,1618`). C shim shipped at `src/litestream-vfs.c`. This is the design to mirror for liters' reader.
- Legacy v0.3 layout (`v3.go:98-111`: `generations/…/{index:08x}.snapshot.lz4`, `{index:08x}_{offset:08x}.wal.lz4`, LZ4 **frame** format) — read-only backcompat in litestream; liters can skip it.

---

## 1. Existing LTX/litestream Rust implementations

| crate | version | license | status |
|---|---|---|---|
| **litetx** ([superfly/ltx-rs](https://github.com/superfly/ltx-rs)) | 0.1.0 (Sep 5 2023) | Apache-2.0 | **Abandoned/stale** — 27 commits, last commit Sep 2023, ~1.9k total downloads. Written by Pavel Borzenkov (then Fly.io) for LiteFS-era LTX. |
| **litepages** ([russellromney/walrust](https://github.com/russellromney/walrust)) | 0.1.0 (Mar 22 2026) | Apache-2.0 | Fork of ltx-rs with litestream-compat fixes: adds `NO_CHECKSUM (0x02)` flag and skips checksum validation when set. 164 downloads; single publisher. |
| **walrust** (repo, not on crates.io as such) | — | Apache-2.0 | Litestream-*inspired* Rust replicator, **explicitly NOT wire-compatible** ("walrust writes HADBP changesets, not Litestream's LTX. Neither tool can restore the other's backups"). 14 stars, experimental. Useful design reference (WAL reading, pinned read-lock via `_walrust_seq` table, leveled compaction), not a dependency. |
| **verneuil** | crates.io `verneuil` | Apache-2.0 | Backtrace Labs' Rust VFS that replicates SQLite to S3 — own chunk format, not litestream-compatible; low activity. Prior art only. |
| litesync (litesync.io) | — | commercial/closed C | TCP peer sync built into a SQLite fork; unrelated to litestream wire format. Ignore. |

**Gap analysis — litetx/litepages vs required LTX v0.5**: litetx implements the 100-byte header (matching offsets) and `Crc::<u64>::new(&CRC_64_GO_ISO)` (correct), but: (a) its only header flag is the obsolete `COMPRESS_LZ4 = 0x1` whole-body **LZ4-frame** compression (`lz4_flex` frame feature); (b) page headers are bare 4-byte pgno (v0.5 needs 6-byte header + flags + 4-byte size + per-page **LZ4 block** compression); (c) **no page index section**; (d) no `NoChecksum` (litepages adds only this). Verdict: fork ltx-rs as a starting skeleton (types, header marshal, checksum trait are reusable) but expect to rewrite encoder/decoder for v0.5; or write fresh against the Go `superfly/ltx` v0.5.1 source (Apache-2.0, 249 stars, active — last release Dec 17 2025). No public Fly.io Rust crate implements LTX v0.5 today — **this is greenfield**.

## 2. SQLite embedding

- **rusqlite 0.40.1** (Jun 6 2026, MIT, actively maintained) — recommended. `bundled` feature compiles SQLite **3.53.1** via `cc` (libsqlite3-sys 0.38.0); works on iOS/Android (Android even vendors rusqlite in AOSP). Relevant features: `bundled`, `backup` (sqlite3_backup for restore-into-place), `hooks` (commit/update hooks for dirty tracking), `wal` checkpoint APIs (`Connection::pragma`/`wal_checkpoint(TRUNCATE)`), `load_extension`. rusqlite does **not** expose VFS registration — use libsqlite3-sys raw or sqlite-plugin (below). liters should link one SQLite: expose a cargo feature to either bundle or bind the platform SQLite (iOS system SQLite is fine read-side; Android NDK has no public libsqlite → bundle).
- **libsql 0.9.30** (Mar 19 2026, MIT, Turso) — fork of SQLite C core + Rust wrapper. Embedded replicas sync **frame-based** (WAL frames over HTTP from their `sqld`/Turso server; `Database::open_with_local_sync`, `sync()` returns committed `frame_no`). Protocol requires a stateful sync server — fundamentally different from litestream's dumb-object-storage model; also drags in tokio/reqwest/tower. Not suitable as a base for liters; useful as prior art (§6).
- **sqlite-plugin 0.10.1** (May 16 2026, MIT/Apache-2.0, orbitinghail — the Graft authors) — actively maintained Rust VFS toolkit; `Vfs` trait intercepts all ops at VFS level (simpler shared state than file-handle delegation); registers as dynamic VFS against any linked SQLite (works with rusqlite/libsqlite3-sys; used in production-alpha by Graft, incl. mobile targets). **Recommended if liters offers a live read-replica VFS** (mirror of litestream `vfs.go`).
- **sqlite-vfs 0.2** (rkusa) — unmaintained since Jul 2022; known soundness/lifecycle issues; avoid.
- Simplest reader alternative: skip VFS entirely — restore/apply LTX to a local db file (plan from §0.2), open read-only with rusqlite. VFS is only needed for lazy page-on-demand reads.

## 3. Object storage

| crate | version | license | assessment for liters |
|---|---|---|---|
| **object_store** | 0.14.0 (Jun 22 2026) | MIT/Apache-2.0 | **Recommended.** Apache Arrow project, very active. S3/GCS/Azure/HTTP/local-fs behind one trait; `GetOptions::range` + `get_ranges` (vectored, coalescing) map 1:1 to `OpenLTXFile`; list-with-prefix + paging; `PutMode::Create`/`Update(UpdateVersion{e_tag,version})` → `If-None-Match`/`If-Match` (`S3ConditionalPut::ETag` works on Tigris/R2/minio/AWS). Custom object metadata via `PutOptions::attributes` (needed for `litestream-timestamp`). tokio is **optional** (only `fs`/util features); HTTP transport is pluggable via `HttpConnector` (`*-base` features allow reqwest-free builds) — good escape hatch for mobile TLS. |
| opendal | 0.58.0 (Jul 2026) | Apache-2.0 | Very active, 40+ backends, conditional writes (`if_match`/`if_not_exists`). Heavier dependency graph and churnier API than object_store; overkill for S3-only. Second choice. |
| aws-sdk-s3 | 1.x (May 2026) | Apache-2.0 | What litestream itself uses (Go equivalent). Huge (smithy stack), tokio+hyper mandatory, slow to compile, large binaries. Not for mobile embedding. |
| rust-s3 (`s3`) | maintained (May 2026) | MIT | Has a **sync** (attohttpc) mode — only mainstream crate with no-async S3. Historically spotty maintenance and sigv4 edge cases. Fallback option if a fully-blocking core is required. |
| DIY: `ureq` + `aws-sigv4` (smithy, standalone) | — | Apache-2.0/MIT | ~500 LoC for GET/PUT/LIST/DELETE with sigv4; smallest possible binary; considered viable given liters needs only 5 verbs. Keep as size-optimization option. |

Tigris specifics: full conditional-operations support (`If-Match`/`If-None-Match` on reads and writes, richer CAS than AWS; docs: tigrisdata.com/docs/objects/conditionals) and strong consistency — conditional PUT of LTX files (`If-None-Match: *`) gives cheap multi-writer detection. TLS on mobile: use **rustls** with **rustls-platform-verifier** (delegates trust evaluation to Security.framework / Android trust manager — required for corporate-MITM and cert-pinning environments); reqwest supports it; avoids shipping webpki roots.

Async runtime: object_store returns futures; recommended pattern for mobile is a **library-owned single `tokio` current-thread runtime on a dedicated thread**, exposed to apps as blocking/step-wise calls (`runtime.block_on`). Full multithreaded tokio is unnecessary; FullStory's mobile SDK experience (fullstory.com blog) recommends avoiding "use-all-cores" runtimes on mobile — a current-thread runtime sidesteps that while keeping the ecosystem. `smol`/blocking-only is viable with rust-s3 but sacrifices object_store.

## 4. Codecs / checksums

- **lz4_flex 0.13.1** (May 2026, MIT, PSeitz; 110M downloads; pure Rust, `#![no_unsafe]` default; fastest pure-Rust LZ4). Supports both **raw block format** (`block::compress`/`block::decompress(input, max_uncompressed_size)`) — required for LTX v0.5 page frames (Go `pierrec/lz4 CompressBlock/UncompressBlock` raw blocks, no size prefix — do NOT use lz4_flex's `_prepend_size` variants) — and **frame format** (feature `frame`) — required only for v0.3 legacy `.snapshot.lz4`/`.wal.lz4` if ever supported. lz4-sys/lz4 (C bindings) unnecessary; avoid C deps for mobile cross-compiles.
- **crc 3.4.0** (Nov 2025, MIT/Apache-2.0) with `crc::CRC_64_GO_ISO` — byte-exact match for Go `hash/crc64` ISO (params in §0.1); proven by superfly's own ltx-rs using it. Note `crc64fast`/`crc64fast-nvme` are ECMA/NVMe polynomials — **wrong variant, do not use**. Table-driven crc is ~1-3 GB/s, ample for page-sized inputs.
- xxhash: not needed for LTX compat (litestream's `cespare/xxhash` is an indirect dep only). If liters wants a fast internal page-dedup/dirty hash: **twox-hash 2.x** (rewritten, matches xxHash C 0.8, MIT, most downloads) or `xxhash-rust` (XXH3, runtime SIMD detection). Pick twox-hash.

## 5. Mobile packaging

- **UniFFI 0.32.0** (Jun 30 2026, MPL-2.0, Mozilla; releases ~monthly). Proc-macro mode (`#[uniffi::export]`, `#[derive(uniffi::Record/Object/Enum)]`) is the mainstream path; 0.29-0.32 added full interface renaming, trait exports, `uniffi::generate` programmatic bindgen, JNA direct-mapping Kotlin (0.30) + experimental JNI Kotlin bindgen (0.32), `uniffi_parse_rs` metadata without building. Async: Rust `async fn` maps to Swift `async`/Kotlin `suspend`, needs an executor — with a library-owned runtime, prefer exposing **blocking methods + a foreign callback interface** for progress/completion; simplest and BGTask-friendly. Production-proven (Firefox iOS/Android) but pre-1.0 (expect churn per minor).
- **Build/packaging pattern** (as shipped by Ferrostar, matrix-rust-sdk, automerge, uniffi-starter):
  - iOS: `cargo build` for `aarch64-apple-ios`, `aarch64-apple-ios-sim` (+x86_64 sim), lipo sims, `xcodebuild -create-xcframework` from static libs + generated modulemap/headers, distribute as **SPM binary target** (zip + checksum in `Package.swift`, git tag). `uniffi-bindgen-swift` generates the Swift layer. `cargo-swift` automates all of this for simple cases; [ianthetechie/uniffi-starter](https://github.com/ianthetechie/uniffi-starter) is the canonical template (Ferrostar's process derives from it).
  - Android: **cargo-ndk 4.1.2** (Aug 2025, MIT/Apache-2.0) + the cargo-ndk Gradle plugin → `.so` per ABI (arm64-v8a, armeabi-v7a, x86_64) into an AAR with generated Kotlin (JNA today; JNI bindgen incoming in 0.32 removes the JNA dependency).
- **Binary size (estimates)**: core (ltx codec + rusqlite bundled + crc + lz4_flex) ≈ 1.5–2.5 MB/arch; + reqwest/rustls/tokio-current-thread ≈ +2–3 MB/arch; UniFFI scaffolding ≈ +0.2 MB. Target ~3–6 MB per arch release-stripped (`opt-level="z"`, `lto=true`, `panic="abort"`, `strip`). A ureq+aws-sigv4 client could cut ~1.5–2 MB if needed.
- **Background execution → API design constraints**: iOS `BGAppRefreshTask` ≈ **30 s** wall clock; `BGProcessingTask` = minutes, only when charging/idle heuristics allow; expiration handler must cancel promptly. Android `WorkManager` Workers get ≈ **10 min**; expedited work less. Therefore the liters API must be: (a) **step-wise & resumable** — `push()` uploads one L0 LTX per committed batch; `pull()`/`sync(budget)` advances a persisted cursor (TXID high-water mark) and can stop after any object; (b) **cancellable** — every operation takes a cancellation token checked between object-storage calls; (c) **crash-safe** — object PUTs are atomic; local state (last-pushed TXID, restore progress) persisted so a killed task resumes, never restarts. Explicit-push design (apps own writers) fits perfectly: no file-watcher, no daemon thread required.

## 6. Prior art (sync architecture vs litestream)

- **Turso/libsql embedded replicas**: client keeps a local SQLite file plus a replication cursor (`frame_no`); a stateful server (sqld) streams **WAL frames** over HTTP/gRPC; writes are forwarded to the primary and echoed back ("consistent read your writes" mode blocks until the local db reflects the write). Contrast: litestream needs no server — LTX objects on dumb storage are the protocol; but libsql gets low-latency incremental pull and write forwarding, which liters must approximate with polling + out-of-band triggers.
- **PowerSync**: server-authoritative sync service partitions data into **sync buckets** (op-log per bucket); clients store schemaless op data in SQLite (views on top) and advance only at server **checkpoints** — a client holding unacked local writes never advances, so no client-side merge ever happens. Row/logical-level, partial replication, requires their service. Litestream/liters is page-level and whole-database — simpler, no server, but no partial sync and single-writer.
- **cr-sqlite**: loadable extension turning tables into CRDTs (LWW column maps, grow-only/OR sets, causal length delete tracking); peers exchange row-level change sets (`crsql_changes`) in any order/any topology; merge is deterministic. Multi-writer without a server, but requires schema constraints (pk-only, CRDT semantics) and rewrites app data model. Litestream approach is physically exact (byte-identical replica) with a single writer — orthogonal tradeoff.
- **ElectricSQL**: current "Electric" is a read-path sync engine over Postgres logical replication: **Shapes** (SQL-defined partial views) streamed to clients over plain HTTP with long-polling + resumable offsets; writes go through your own API (no built-in write-path since the 2024 rewrite). Elixir server, client stores in SQLite/PGlite. Like litestream it's log-shipping over simple transport, unlike it it's row-level, partial, Postgres-sourced.
- **Graft** (bonus; orbitinghail, dual MIT/Apache, alpha, v0.2.1 Dec 2025): transactional page store on object storage — Volumes with LSNs, metastore + pagestore split, lazy partial page replication with serializable snapshot isolation; SQLite integration via its `sqlite-plugin` VFS; explicitly targets mobile/edge. Closest architectural cousin to "litestream VFS in Rust"; validates sqlite-plugin as the VFS layer.

## 7. Concrete recommendations

| concern | pick | rationale |
|---|---|---|
| LTX codec | **new crate `ltx` in-workspace**, seeded from superfly/ltx-rs (Apache-2.0) types; implement v0.5: 6-byte page headers + `PageHeaderFlagSize`, per-page lz4 **block**, page index (uvarint triples + BE u64 length), `HeaderFlagNoChecksum` | litetx is 2 format-generations old; litepages only patches flags; format spec fully recoverable from Go ltx v0.5.1 + local litestream testdata for golden-file tests (`testdata/**/*.ltx`) |
| SQLite | **rusqlite 0.40 / libsqlite3-sys 0.38**, `bundled` on Android, feature-switch to system SQLite on iOS | maturity, hooks/backup APIs, AOSP-vetted |
| VFS (reader, optional phase 2) | **sqlite-plugin 0.10** | maintained, Graft-proven, works over libsqlite3-sys |
| Object storage | **object_store 0.14** (`aws` feature) + `rustls` + **rustls-platform-verifier**; conditional puts via `PutMode` for writer fencing; keep `HttpConnector` seam for a future ureq-based slim client | one trait covers S3/Tigris/GCS/Azure; ranged GET + list + metadata + CAS all present; tokio-optional core |
| Runtime | library-owned **tokio current-thread runtime** on one background thread; public API blocking + cancellation tokens | mobile-friendly, object_store-compatible, BGTask/WorkManager-safe |
| Compression | **lz4_flex 0.13** (`block` for v0.5 pages; `frame` only if v0.3 legacy read is ever added) | pure Rust, no cross-compile pain, byte-compatible with pierrec/lz4 |
| Checksums | **crc 3.4** `CRC_64_GO_ISO`; `ChecksumFlag = 1<<63`; XOR-rolling page checksums | exact Go `hash/crc64` ISO match (proven by ltx-rs) |
| FFI/packaging | **UniFFI 0.32** proc-macro mode; uniffi-starter/Ferrostar pattern: XCFramework + SPM binary target (iOS), cargo-ndk 4.1 + Gradle plugin AAR (Android); blocking API + callback interface over Rust-async export | production-proven pipeline; avoids async-executor coupling across FFI |

Open items to VERIFY during implementation (flagged above): (1) encoder behavior for incompressible pages (raw-frame fallback vs always-block); (2) exact byte coverage of trailer `FileChecksum` and its presence under `NoChecksum`; (3) whether `LTXFiles` S3 listing relies solely on key names vs metadata HEADs for `seek` (see `s3/replica_client.go:1547` `ltx.ParseFilename(key)` — names are authoritative). Golden-file round-trip tests against `reference/litestream/testdata/**/*.ltx` and a live `litestream restore` interop test against a liters-written bucket are the acceptance gates.

Key local references: `reference/litestream/litestream.go:184-197` (paths), `s3/replica_client.go:55,655,703` (keys/metadata/ranges), `replica_client.go:24-160` (client interface, page-index fetch), `replica.go:1441` (restore plan), `compaction_level.go:9-19` (levels), `db.go:1948,2747-2772` (NoChecksum, CRC64-ISO), `vfs.go:513-1618` (read-replica VFS design), `v3.go:98-111` (legacy naming).

Sources: [superfly/ltx-rs](https://github.com/superfly/ltx-rs) · [superfly/ltx](https://github.com/superfly/ltx) · [litetx](https://crates.io/crates/litetx) · [litepages](https://crates.io/crates/litepages) · [walrust](https://github.com/russellromney/walrust) · [rusqlite](https://github.com/rusqlite/rusqlite) · [sqlite-plugin](https://github.com/orbitinghail/sqlite-plugin) · [sqlite-vfs](https://github.com/rkusa/sqlite-vfs) · [object_store](https://docs.rs/object_store/latest/object_store/) · [S3ConditionalPut](https://docs.rs/object_store/latest/object_store/aws/enum.S3ConditionalPut.html) · [opendal](https://github.com/apache/opendal) · [rust-s3](https://github.com/durch/rust-s3) · [lz4_flex](https://github.com/PSeitz/lz4_flex) · [crc](https://crates.io/crates/crc) · [Go hash/crc64](https://pkg.go.dev/hash/crc64) · [twox-hash](https://github.com/shepmaster/twox-hash) · [uniffi-rs](https://github.com/mozilla/uniffi-rs) · [uniffi CHANGELOG](https://github.com/mozilla/uniffi-rs/blob/main/CHANGELOG.md) · [uniffi-starter](https://github.com/ianthetechie/uniffi-starter) · [Ferrostar iOS packaging](https://stadiamaps.com/news/ferrostar-building-a-cross-platform-navigation-sdk-in-rust-part-2/) · [cargo-ndk](https://crates.io/crates/cargo-ndk) · [Tigris conditionals](https://www.tigrisdata.com/docs/objects/conditionals/) · [libsql](https://docs.rs/libsql) · [PowerSync client architecture](https://docs.powersync.com/architecture/client-architecture) · [cr-sqlite](https://github.com/vlcn-io/cr-sqlite) · [ElectricSQL](https://electric-sql.com/blog/2023/09/20/introducing-electricsql-v0.6) · [Graft](https://github.com/orbitinghail/graft) · [FullStory mobile Rust SDK](https://www.fullstory.com/blog/rust-at-fullstory-part-2-mobile-sdk/) · [BGProcessingTask](https://developer.apple.com/documentation/backgroundtasks/bgprocessingtask) · [LiteSync](https://litesync.io/en/) · [verneuil](https://crates.io/crates/verneuil)
