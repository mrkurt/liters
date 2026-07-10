# Litestream v0.5.x Object-Storage Layout & ReplicaClient Contract (for a compatible Rust client)

Source pinned: reference repo at **v0.5.14** (`git describe: v0.5.14-1-gc96c0f4`), LTX library **github.com/superfly/ltx v0.5.1** (go.mod:30). All paths below relative to `/Users/kurt/code/liters/reference/litestream` unless noted; `ltx/` refers to `~/go/pkg/mod/github.com/superfly/ltx@v0.5.1`.

## 1. Key/path scheme in the bucket (S3 backend — the wire-compatibility target)

For a replica URL `s3://BUCKET/PATH[?query]`, `client.Path` = URL path with leading `/` trimmed and `path.Clean` applied (replica_url.go:105, s3/replica_client.go:279). All objects for one database live under `PATH/`:

```
PATH/
  0000/                                  # level 0 (raw per-transaction LTX)
    0000000000000001-0000000000000001.ltx
    0000000000000002-0000000000000002.ltx
  0001/                                  # level 1 (30s compactions, default)
    0000000000000001-00000000000000c8.ltx
  0002/ 0003/                            # level 2 (5m), level 3 (1h), default
  0009/                                  # SnapshotLevel = 9: full snapshots
    0000000000000001-00000000000000ff.ltx
  lock.json                              # optional distributed lease (leaser)
  generations/                           # only if migrated from v0.3.x (read-only legacy)
```

**Level directory**: `fmt.Sprintf("%04x", level)` — 4-digit zero-padded lowercase **hex** (s3/replica_client.go:655, 703, 1070, 1392). Since valid levels are 0–8 plus snapshot level 9 (compaction_level.go:9, 72–73), hex and decimal render identically in practice (`0000`…`0009`), but a Rust client must format with `%04x` for exactness. There is **no `ltx/` path segment on S3** — the "ltx" subdirectory exists only in the local metadata dir and in the file/GCS backends (see §9). Note docs/REPLICA_CLIENT_GUIDE.md:693–698 shows `ltx/%04d` — that doc is **stale**; the code is authoritative.

**Filename**: `ltx.FormatFilename(minTXID, maxTXID)` = `"%016x-%016x.ltx"` — two 16-char zero-padded lowercase-hex TXIDs joined by `-`, extension `.ltx` (ltx/ltx.go:487–489, TXID.String at 142–144). Parse regex: `^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$` (ltx/ltx.go:485); parsed base-16 (ltx/ltx.go:450–459). Non-matching keys are silently skipped when listing (s3/replica_client.go:1546–1550).

**Full object key**: `PATH + "/" + sprintf("%04x/%s", level, filename)` — e.g. `myapp/db/0000/0000000000000042-0000000000000042.ltx`.

**Lease key**: `PATH + "/lock.json"` (or `lock.json` if PATH empty) (s3/leaser.go:23, 83–88).

**TXID semantics**: `type TXID uint64` (ltx/ltx.go:127). L0 files always have `minTXID == maxTXID` (one committed transaction per file; db.go:1951–1952, replica.go:171–176). Compacted files span `[minTXID, maxTXID]`. Snapshot files (level 9) always have `minTXID == 1` (db.go:2470: `WriteLTXFile(ctx, SnapshotLevel, 1, pos.TXID, r)`); an LTX header with `MinTXID==1` is by definition a snapshot (ltx header validation, ltx/ltx.go ~247).

## 2. Levels & default compaction configuration (compaction_level.go)

- `SnapshotLevel = 9` (constant; line 9). Levels 0..len(levels)-1 are "compaction levels"; level numbers must equal their index and cannot exceed `SnapshotLevel-1 = 8` (lines 69–83).
- Defaults (lines 14–19): `{L0, interval 0}, {L1, 30s}, {L2, 5m}, {L3, 1h}`. L0 must have interval 0; others > 0.
- `NextLevel(maxLevel) == SnapshotLevel`; `PrevLevel(SnapshotLevel) == MaxLevel()` (lines 97–113).
- Compaction schedule is wall-clock aligned: `PrevCompactionAt = now.Truncate(interval).UTC()`, next = prev + interval (lines 33–41).
- Store defaults (store.go:60–72): `DefaultSnapshotInterval = 24h`, `DefaultSnapshotRetention = 24h`, `DefaultL0Retention = 5m`, `DefaultL0RetentionCheckInterval = 15s`, `DefaultRetentionCheckInterval = 1h`.
- Replica sync: `DefaultSyncInterval = 1s` (replica.go:24); error backoff caps at `DefaultSyncBackoffMax = 5m` (db.go:41); follow-mode poll `DefaultFollowInterval = 1s` (db.go:2805).

**Compaction algorithm** (compactor.go:104–192) — required for write-side compatibility:
1. `seekTXID = maxTXID(dstLevel) + 1` (from listing dst level; cached).
2. List src level (`dstLevel-1`) with `seek=seekTXID`; collect all matching files; output range = `[min(MinTXID), max(MaxTXID)]` over sources.
3. Merge via `ltx.NewCompactor` with `HeaderFlags = ltx.HeaderFlagNoChecksum` (page-level dedup, newest wins, lock page skipped).
4. `WriteLTXFile(dstLevel, minTXID, maxTXID, merged)`. Skip conditions: `ErrCompactionTooEarly` if dst's newest `CreatedAt > PrevCompactionAt(now)`; `ErrNoCompaction` if `srcMaxTXID <= dstMinTXID` (store.go:745–800). Snapshot level bypasses merge: writes a fresh full snapshot from the DB (store.go:762–770, db.go:2465–2480).

**Retention** (compactor.go:238–426, store.go:801+): snapshot level — delete snapshots with `CreatedAt < now-retention` but always keep the newest; then cascade `EnforceRetentionByTXID(level, minRetainedSnapshotTXID)` to every lower level, deleting files with `MaxTXID < txID` while always keeping the last file per level. L0 extra rule: delete only files both older than `now-L0Retention` **and** with `MaxTXID <= maxL1TXID` (must already be compacted into L1), preserving contiguity for readers; stops at first file newer than threshold (list-order dependent).

## 3. Per-level invariants a client may rely on / must maintain

- Within a level, files sorted by filename are expected **contiguous**: `prev.MaxTXID + 1 == curr.MinTXID` (gap/overlap detection in replica.go:1714–1778 and compactor.go:197–233).
- Listing order: S3 `ListObjectsV2` returns keys in ascending UTF-8 binary order; because filenames are fixed-width lowercase hex, this equals ascending `(MinTXID, MaxTXID)`. The S3 iterator does **no client-side sort** and consumers (restore cursors, compaction, follow, validation) depend on this order (s3/replica_client.go:1494–1588). Slice-based backends sort by `(Level, MinTXID, MaxTXID)` via `ltx.NewFileInfoSliceIterator` (ltx/ltx.go:533–545). A Rust implementation should sort defensively the same way.
- `seek` in `LTXFiles` is a client-side filter: skip any file with `MinTXID < seek` ("start from given TXID or next available"); S3 does not use `StartAfter` (s3/replica_client.go:1559–1562).
- Uploads are **not conditional** (no If-None-Match on LTX PUTs); last-writer-wins. Single-writer safety comes from the optional lease (§7). Re-uploading an identical key must be tolerated (idempotent re-push after crash).

## 4. `ReplicaClient` interface — the trait a Rust client must mirror (replica_client.go:18–51)

```go
type ReplicaClient interface {
    Type() string                                   // e.g. "s3", "file"
    Init(ctx) error                                 // idempotent connection/config setup
    LTXFiles(ctx, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error)
    OpenLTXFile(ctx, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
    WriteLTXFile(ctx, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error)
    DeleteLTXFiles(ctx, a []*ltx.FileInfo) error
    DeleteAll(ctx) error                            // wipe everything under PATH
    SetLogger(*slog.Logger)
}
```

Semantics:
- **LTXFiles**: iterator of `ltx.FileInfo` for one level, ascending filename order, filtered by `seek`. `useMetadata=true` ⇒ fetch accurate creation timestamps (needed only for timestamp-based point-in-time restore); `false` ⇒ fast timestamps (S3 `LastModified` from LIST). Iterator contract: `Next() bool`, `Item() *FileInfo`, `Err() error`, `Close() error` (ltx/ltx.go:498+).
- **OpenLTXFile**: byte-range read. `size==0` ⇒ read from `offset` to EOF (`Range: bytes=off-`); else `Range: bytes=off-(off+size-1)` (s3/replica_client.go:646–651). Missing object ⇒ **`os.ErrNotExist`** (mapped from S3 `NoSuchKey`, s3/replica_client.go:1691–1697). Range support is mandatory: resumable restore reads and page-index fetches depend on it (docs/REPLICA_CLIENT_GUIDE.md:509).
- **WriteLTXFile**: must (a) peek the 100-byte LTX header from the stream, (b) extract `Timestamp` (int64 Unix **milliseconds**), (c) store it as S3 object metadata key **`litestream-timestamp`** (constant, s3/replica_client.go:55) with value `time.RFC3339Nano` in UTC (s3/replica_client.go:697–707), (d) upload (multipart allowed), (e) fail if no ETag returned, (f) return `FileInfo{Level, MinTXID, MaxTXID, Size = bytes uploaded, CreatedAt = header timestamp}` (s3/replica_client.go:683–756). S3 metadata keys surface as header `x-amz-meta-litestream-timestamp`.
- **DeleteLTXFiles**: batch delete, `MaxKeys = 1000` per `DeleteObjects` call (s3/replica_client.go:58, 1077–1127); partial errors are collected and returned. When `RequireContentMD5` (default **true**), a `Content-MD5` header (base64 MD5 of the exact XML `<Delete>` body) is attached — required by some providers (s3/replica_client.go:758–943).
- **DeleteAll**: paginate-list `Prefix = PATH + "/"` and batch delete everything (s3/replica_client.go:1133–1177).
- Missing-level list must return an empty iterator, not an error (file/replica_client.go:97–99).
- `FindLTXFiles` helper filters via callback; `ErrStopIter` ends iteration early (replica_client.go:53–84).
- Optional `ReplicaClientV3` interface for v0.3 restore (v3.go:145–166): `GenerationsV3`, `SnapshotsV3`, `WALSegmentsV3`, `OpenSnapshotV3`, `OpenWALSegmentV3` (last two return LZ4-decompressed readers).

`ltx.FileInfo` (ltx/ltx.go:572–580): `{Level int, MinTXID, MaxTXID TXID, PreApplyChecksum, PostApplyChecksum Checksum, Size int64, CreatedAt time.Time}`. Only Level/Min/Max/Size/CreatedAt are populated from listings. `ltx.Pos = {TXID, PostApplyChecksum}`; string form `"%016x/%016x"` (33 chars, ltx/ltx.go:295–332).

## 5. useMetadata timestamp mechanics (S3)

When `useMetadata=true`, for each LIST page the client issues parallel `HeadObject` calls (semaphore, `MetadataConcurrency` default **50**; R2 multipart concurrency default 2 — s3/replica_client.go:66, 70, 1413–1484), parses `litestream-timestamp` as RFC3339Nano; on any per-object failure silently falls back to `LastModified.UTC()`. `file` backend instead preserves the timestamp via `os.Chtimes` on write and reads ModTime (file/replica_client.go:96, 228–231). `ltx.ParseTimestamp` accepts fixed-width `RFC3339Milli` (`2006-01-02T15:04:05.000Z07:00`) with RFC3339Nano fallback (ltx/ltx.go:461–483).

## 6. Upload/transport specifics (S3)

- SDK: AWS SDK Go v2 + `s3/manager.Uploader`. Multipart via Uploader: `PartSize` default 5 MB, `Concurrency` default 5 (SDK defaults; struct comment s3/replica_client.go:101–103); configurable via URL query `partSize`/`concurrency`.
- Retry: standard retryer, `MaxAttempts = 10`, **rate limiter disabled** (`ratelimit.None`) so retry quota never exhausts during outages (s3/replica_client.go:564–579).
- HTTP client timeout **24h** (long restores); custom transport: dial timeout 30s, keep-alive 30s, `MaxIdleConns` 100, idle timeout 90s (s3/replica_client.go:364–383).
- Endpoint: `Endpoint` string ⇒ `BaseEndpoint`; scheme defaults to `https://`; `http://` disables TLS-HTTPS; `ForcePathStyle` ⇒ `UsePathStyle`. Custom endpoint ⇒ default `forcePathStyle=true`, disable request/response checksum calc (`WhenRequired`) and remove the aws-chunked trailing-checksum middleware — many S3-compatibles reject aws-chunked (s3/replica_client.go:438–469, 849–858).
- `SignPayload` default true; when false, use `UNSIGNED-PAYLOAD` SigV4 (Filebase et al.) (s3/replica_client.go:834–847).
- Provider defaults table (s3/replica_client.go:240–275): Tigris ⇒ signPayload=true, requireMD5=false, plus header `X-Tigris-Consistent: true` on every request (lines 815–832); Filebase/Backblaze/MinIO/Supabase ⇒ path-style; R2 ⇒ upload concurrency 2.
- Region: if unset and no endpoint ⇒ `GetBucketLocation` (empty constraint ⇒ `us-east-1` = `DefaultRegion`, s3/replica_client.go:61, 581–625); with endpoint ⇒ default `us-east-1`.
- User-Agent must contain `litestream` (lines 793–812).
- Credentials env: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, fallback `LITESTREAM_ACCESS_KEY_ID`/`LITESTREAM_SECRET_ACCESS_KEY`; endpoint fallback `LITESTREAM_S3_ENDPOINT` (lines 219–238).
- Optional SSE-C (AES256, 32-byte base64 key + MD5; must be sent on GET/HEAD too) and SSE-KMS (write-only params) (lines 110–118, 507–561).

## 7. Concurrency control: lease (`lock.json`) — the only conditional-write use

s3/leaser.go: object `PATH/lock.json`, JSON body `{"generation": int64, "expires_at": RFC3339, "owner": "host:pid"}` (Lease struct leaser.go:31–36; ETag not serialized). `DefaultLeaseTTL = 30s`.
- **Acquire**: GET lock.json; if exists and not expired ⇒ `LeaseExistsError{Owner, ExpiresAt}`. Else PUT with `If-None-Match: *` (create) or `If-Match: <etag>` (expired takeover), `generation = prev+1` (start 1). HTTP 412/`PreconditionFailed` ⇒ lost race (s3/leaser.go:90–136, 231–266).
- **Renew**: PUT with `If-Match: lease.ETag`; 412 ⇒ `ErrLeaseNotHeld`.
- **Release**: `DeleteObject` with `IfMatch: etag`; 404 ⇒ `ErrLeaseAlreadyReleased`, 412 ⇒ not held.
The lease is optional and does not affect LTX layout; requires backend conditional-write support (AWS added If-None-Match PUT in 2024; R2/MinIO support it). heartbeat.go is unrelated to the bucket — it pings an external HTTP URL (healthchecks-style) every ≥1m (heartbeat.go:11–15).

## 8. Replication/restore algorithms that define expected bucket state

**Push (writer)** (replica.go:134–216): replica position = max `MaxTXID` over L0 remote listing (`calcPos`/`MaxLTXFileInfo`). For each `txID` from `pos+1` to local DB TXID, upload local file `ltx/0/<txid>-<txid>.ltx` via `WriteLTXFile(0, txID, txID, f)` sequentially. So an embedded writer needs only: list L0 (and if L0 empty, it's a fresh replica), then PUT single-transaction L0 files in TXID order; run compaction+snapshot+retention per §2 if it is the sole writer. A brand-new database's first L0 file contains **all** pages (full snapshot into L0) with `MinTXID==MaxTXID==txID` (db.go:1962–1974); LTX header offsets carry WALOffset/WALSize/salts.

**Restore plan** (`CalcRestorePlan`, replica.go:1441–1557): (1) choose newest level-9 snapshot with `MaxTXID <= txID` (or `CreatedAt < timestamp`); (2) open cursors on levels `8..0` (each listed in filename order), repeatedly choosing the candidate file that is contiguous (`MinTXID <= currentMax+1`) and best by: greater `MaxTXID`, then smaller `MinTXID`, then higher level, then earlier `CreatedAt` (replica.go:1626–1637); (3) error `non-contiguous ltx files` if a gap remains; feed the ordered list into `ltx.Compactor` and `Decoder.DecodeDatabaseTo` to materialize the DB. Empty ⇒ `ErrTxNotAvailable`.

**Read-replica polling ("follow")** (replica.go:737–994): keep `lastTXID` (persisted in sidecar `<db>-txid` file containing 16-hex TXID + newline, replica.go:1639–1703); each tick, list L0 with `seek=lastTXID+1`, apply each file's pages directly at `(pgno-1)*pageSize`, truncate to `Commit*pageSize`; on gaps (L0 pruned) bridge from levels 1..8 with files where `MinTXID <= current+1 && MaxTXID > current`. When patching page 1, force bytes 18–19 to `0x01 0x01` (DELETE journal mode) and randomize bytes 24–27 (schema cookie) to invalidate other connections (replica.go:871–930).

**Point reads / page index** (replica_client.go:88–160): the page index lives immediately before the trailer. Fetch algorithm: GET last `max(32KB, ...)` bytes (`DefaultEstimatedPageIndexSize = 32*1024`); page-index byte length is the big-endian u64 at `fileSize - TrailerSize(16) - 8`; index block spans `[size - 16 - 8 - idxLen, size - 16 - 8)`. Index encoding: repeated uvarint triples `(pgno, offset, size)` sorted by pgno, terminated by uvarint 0, then the u64 length (ltx/encoder.go:137–174, ltx/decoder.go:310–346). `FetchPage` GETs `[offset, offset+size)` and `ltx.DecodePageData` decompresses one page.

## 9. LTX file format constants (needed to write/read objects)

ltx/ltx.go: magic `"LTX1"` (bytes 0–3), `Version = 3`, `HeaderSize = 100`, `PageHeaderSize = 6`, `TrailerSize = 16`, `ChecksumSize = 8`, `ChecksumFlag = 1<<63`, `HeaderFlagNoChecksum = 1<<1` (the only valid flag; litestream sets it on compactions/restores), `MaxPageSize = 65536`, `PENDING_BYTE = 0x40000000` (lock page `pgno = PENDING_BYTE/pageSize + 1` is never stored). Header layout, big-endian (MarshalBinary, ltx/ltx.go:283–299): `[0:4] magic; [4:8] Flags u32; [8:12] PageSize u32; [12:16] Commit u32 (post-tx page count); [16:24] MinTXID u64; [24:32] MaxTXID u64; [32:40] Timestamp i64 (unix ms); [40:48] PreApplyChecksum u64; [48:56] WALOffset i64; [56:64] WALSize i64; [64:68] WALSalt1 u32; [68:72] WALSalt2 u32; [72:80] NodeID u64; [80:100] zero padding`. Page frame = 6-byte header `{Pgno u32, Flags u16(=0)}` + LZ4-compressed page body (encoder pipes pages through an LZ4 writer). Trailer = `{PostApplyChecksum u64, FileChecksum u64}` where FileChecksum = CRC-64 (flagged with `ChecksumFlag`). Restore validates `info.Size >= HeaderSize` before use (replica.go:640–643).

## 10. Backend divergence warning (affects "compatibility" definition)

The v0.5.x layouts are **not identical across backends**: S3/OSS use `PATH/%04x/<name>.ltx` (s3:655, oss/replica_client.go:390) while file/GCS (and NATS subject scheme) use `PATH/ltx/<decimal-level>/<name>.ltx` via `litestream.LTXDir/LTXLevelDir/LTXFilePath` (litestream.go:184–197; gs/replica_client.go:131,146; file/replica_client.go:85–92; nats:254). liters targets S3-compatible object storage ⇒ implement the `%04x` scheme; optionally support the `ltx/<decimal>` scheme behind a layout flag for file-tree replicas. Local writer metadata dir is `<dir>/.<dbname>-litestream/` (`MetaDirSuffix = "-litestream"`, litestream.go:20, db.go:289) containing `ltx/<level>/<name>.ltx`.

## 11. v0.3.x generations layout (legacy read/migration only — verify: **gone from the v0.5 write path**)

v0.5 never writes this; it only reads it for restore comparison (`shouldUseV3Restore`, replica.go:1327+ picks whichever format has the more recent backup). Layout (v3.go:60–113):

```
PATH/generations/{generation}/            # generation = exactly 16 lowercase hex chars
    snapshots/{index:%08x}.snapshot.lz4   # LZ4-compressed full DB
    wal/{index:%08x}_{offset:%08x}.wal.lz4  # LZ4-compressed WAL segments
```

Regexes: `^[0-9a-f]{16}$`, `^([0-9a-f]{8})\.snapshot\.lz4$`, `^([0-9a-f]{8})_([0-9a-f]{8})\.wal\.lz4$`. Restore: pick best snapshot by CreatedAt across all generations, then apply WAL segments of that generation with `index >= snapshotIndex`, requiring offset-0 segment per index and contiguous offsets, checkpointing each rebuilt WAL with `PRAGMA wal_checkpoint(TRUNCATE)` (replica.go:996–1256). Generations are discovered via delimiter-`/` CommonPrefixes listing (s3:1180–1214). There are **no other metadata/manifest files in v0.5**: no `generation` pointer file, no positions file in the bucket — state is derived entirely from LIST + filenames; the only non-LTX object v0.5 may create is `lock.json`.

## 12. Rust trait sketch (direct mapping)

```rust
trait ReplicaClient {
    fn r#type(&self) -> &str;
    async fn init(&self) -> Result<()>;                       // idempotent
    async fn ltx_files(&self, level: u8, seek: Txid, use_metadata: bool)
        -> Result<impl Stream<Item = Result<FileInfo>>>;     // ascending (min,max)
    async fn open_ltx_file(&self, level: u8, min: Txid, max: Txid,
        offset: u64, size: u64) -> Result<impl AsyncRead>;   // size==0 => to EOF; NotFound error kind
    async fn write_ltx_file(&self, level: u8, min: Txid, max: Txid,
        body: impl AsyncRead) -> Result<FileInfo>;           // peek 100B header -> ms timestamp -> x-amz-meta-litestream-timestamp (RFC3339Nano)
    async fn delete_ltx_files(&self, infos: &[FileInfo]) -> Result<()>; // batches of 1000 + Content-MD5
    async fn delete_all(&self) -> Result<()>;
}
// key = format!("{path}/{level:04x}/{min:016x}-{max:016x}.ltx")
```

Constants to bake in: `SNAPSHOT_LEVEL=9`, `MAX_KEYS=1000`, `DEFAULT_REGION="us-east-1"`, `METADATA_KEY="litestream-timestamp"`, `LEASE_PATH="lock.json"`, `LEASE_TTL=30s`, `EST_PAGE_INDEX=32768`, `LTX_HEADER=100`, `LTX_TRAILER=16`, filename regex above, default levels `[0s,30s,5m,1h]`, snapshot interval/retention 24h, L0 retention 5m.
