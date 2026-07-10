# Litestream v0.5.x Restore & Incremental Reader Mechanics — Research Report

## 1. Bucket layout and naming (ground truth for wire compatibility)

- Object key: `<prefix>/<level:%04x>/<minTXID:%016x>-<maxTXID:%016x>.ltx` (s3/replica_client.go:654-655, 702-703; filename via `ltx.FormatFilename`, ltx.go:487-489; parse regex `^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$`, ltx.go:484). Fixed-width hex ⇒ S3 lexicographic LIST order == ascending MinTXID order. All plan/apply algorithms depend on this ordering.
- Levels: `0`..`8` are compaction levels; `SnapshotLevel = 9` is a pseudo-level holding full snapshots (compaction_level.go:8-9). User-configured levels must be ≤ 8 (compaction_level.go:72-73).
- **L0 files are single-transaction**: writer uploads `(level=0, minTXID=txID, maxTXID=txID)` one per TX (replica.go:171-176).
- **Snapshots always have MinTXID=1**: `WriteLTXFile(ctx, SnapshotLevel, 1, pos.TXID, r)` (db.go:2470). `Header.IsSnapshot() ⇔ MinTXID==1` (ltx.go:198-200).
- `LTXFiles(ctx, level, seek, useMetadata)` iterator contract: skips files with `MinTXID < seek` (s3/replica_client.go:1559-1561; file/replica_client.go:117); `useMetadata=true` makes S3 HeadObject fetch the original LTX-header timestamp stored in object metadata at write time (`WriteLTXFile` peeks header, stores RFC3339Nano timestamp; s3/replica_client.go:684-716) — needed only for timestamp-accurate PIT restore. `OpenLTXFile(..., offset, size)` maps to an HTTP Range GET; missing object → `os.ErrNotExist` (s3/replica_client.go:673-676).

## 2. LTX v0.5.1 file format (Version=3)

Constants (ltx.go:18-41): `Magic="LTX1"`, `HeaderSize=100`, `PageHeaderSize=6`, `TrailerSize=16`, `ChecksumSize=8`, `ChecksumFlag = 1<<63`, `MaxPageSize=65536`, `PENDING_BYTE=0x40000000`, lock page = `PENDING_BYTE/pageSize + 1` (ltx.go:491-496).

**Header (100 bytes, big-endian; MarshalBinary ltx.go:283-299):**

| offset | size | field |
|---|---|---|
| 0 | 4 | Magic "LTX1" |
| 4 | 4 | Flags (`HeaderFlagNoChecksum = 1<<1`, only valid flag; ltx.go:171-176) |
| 8 | 4 | PageSize |
| 12 | 4 | Commit — DB size in pages *after* applying this file |
| 16 | 8 | MinTXID |
| 24 | 8 | MaxTXID |
| 32 | 8 | Timestamp (ms since epoch) |
| 40 | 8 | PreApplyChecksum — rolling DB checksum *before* MinTXID applies |
| 48 | 8 | WALOffset |
| 56 | 8 | WALSize |
| 64 | 4 | WALSalt1 |
| 68 | 4 | WALSalt2 |
| 72 | 8 | NodeID |
| 80 | 20 | zero/reserved |

**Body:** sequence of page frames: 6-byte page header (`Pgno` u32, `Flags` u16, must be 0) + **LZ4-frame-compressed** page data (one LZ4 frame per page; decoder.go:176-186). A frame with an all-zero page header terminates the page block (decoder.go:166-169).

**Page index** (after page block; decoder.go:310-346): uvarint triples `(pgno, absoluteFileOffset, size)` per page, `pgno=0` terminator, then 8-byte BE length of the index. Enables random page access via ranged GETs (`FetchPageIndex`/`FetchPage`, replica_client.go:88-160; fetches last 32KB estimated: `DefaultEstimatedPageIndexSize = 32*1024`).

**Trailer (16 bytes):** `PostApplyChecksum` (8) + `FileChecksum` (8). `FileChecksum = ChecksumFlag | CRC64-ISO(entire file except last 8 bytes)`, verified in `Decoder.Close()` (decoder.go:100-103). Rolling DB checksum = `ChecksumFlag | XOR over pages of ChecksumPage(pgno,data)` where `ChecksumPage = ChecksumFlag | CRC64-ISO(pgno_be32 || data)`, lock page excluded (checksum.go:106-132; decoder.go:189-193).

**Encoder invariants** (encoder.go:211-260): page numbers strictly ascending within a file; `pgno > Commit` rejected; snapshot files must contain *every* page 1..Commit consecutively, skipping exactly the lock page.

**Critical compatibility fact:** litestream v0.5 writes **every** file with `HeaderFlagNoChecksum` — L0 (db.go:1948), snapshots (db.go:2412), compactions (compactor.go:165, replica.go:682, vfs.go:706). With that flag, `PreApplyChecksum` and `PostApplyChecksum` MUST be 0 (`Header.Validate`, ltx.go:246-264; `Trailer.Validate`, ltx.go:355-366). So in real litestream buckets **the pre/post-apply checksum contract is inert**; the only integrity guarantees are the per-file CRC64 and TXID-range bookkeeping. liters must write `Flags=HeaderFlagNoChecksum`, `PreApplyChecksum=0`, `PostApplyChecksum=0` to be wire-compatible, and cannot rely on checksum-based position validation when reading.

`ltx.Pos = {TXID, PostApplyChecksum}` (ltx.go:66-69); `Header.PreApplyPos() = {MinTXID-1, PreApplyChecksum}` (ltx.go:275-280).

## 3. Restore plan algorithm — `CalcRestorePlan` (replica.go:1441-1557)

Inputs: `txID` (0 = latest), `timestamp` (zero = latest); mutually exclusive (replica.go:1442-1444).

1. **Snapshot selection** (replica.go:1450-1473): iterate level 9 in filename order; keep the *last* snapshot satisfying `(txID==0 || MaxTXID <= txID)` and `(timestamp.IsZero() || CreatedAt < timestamp)`. If found, it seeds `infos`; `currentMax = snapshot.MaxTXID`. If `txID != 0 && currentMax >= txID`, return just the snapshot (replica.go:1480-1482).
2. **Per-level cursors** for levels 8..0 (`maxLevel = SnapshotLevel-1`; replica.go:1477, 1484-1494), each streaming that level's files ascending.
3. **Greedy chain extension loop** (replica.go:1503-1536). Each iteration:
   - `cursor.refresh(currentMax, txID, timestamp)` (replica.go:1570-1608): advance the level iterator over every file with `MinTXID <= currentMax+1` (**contiguous or overlapping** with the current position); among them discard `MaxTXID <= currentMax` (fully covered), `MaxTXID > txID`, `CreatedAt >= timestamp`; retain the best as `candidate`. Stop advancing at the first file with `MinTXID > currentMax+1` (a gap at this level; keep it as `current` for the post-check).
   - Pick the best candidate across all levels via `restoreCandidateBetter` (replica.go:1626-1637): **higher MaxTXID > lower MinTXID > higher level > earlier CreatedAt**. Append; `currentMax = candidate.MaxTXID`. Break when no candidate or `currentMax >= txID`.
4. **Gap post-check** (latest-restore only, replica.go:1538-1547): after exhaustion, if any level's stalled `current` file has `MinTXID > currentMax+1` → error `"non-contiguous ltx files: have up to X but next file starts at Y"`. This refuses to silently return stale state when newer files exist beyond a hole.
5. Empty plan or `plan.MaxTXID < txID` → `ErrTxNotAvailable` (store.go:26-27; replica.go:1549-1555).

**Why the chain is correct:** if no snapshot is selected, `currentMax=0`, so only a file with `MinTXID==1` (by definition a snapshot-equivalent) can start the chain. Every subsequent file satisfies `ltx.IsContiguous(prevMax, min, max) ⇔ min <= prevMax+1 && max > prevMax` (ltx.go:623-625) — **partial overlap is explicitly legal**; the `ltx.Compactor` enforces exactly this pairwise on its inputs (ltx compactor.go:91-104). Because each LTX page frame is the *latest full image* of that page within the file's TX range, merging an ordered overlapping chain yields exactly the state at the final `MaxTXID`.

**Overlap tolerance is what makes the plan robust to concurrent compaction**: `FuzzRestoreWithMissingCompactedFile` (restore_fuzz_test.go:17-135) deletes a random L1/L2 file mid-history and asserts restore still succeeds with `integrity_check == ok` — the greedy cross-level cursor picks around the hole using L0s and/or larger merged files whose `MinTXID <= currentMax`.

## 4. Applying the plan — full restore (replica.go:622-731)

- Each plan file is wrapped in `internal.NewResumableReader` (replica.go:647): on connection reset or *premature EOF* (offset < known Size) it reopens with a Range GET at the current byte offset, ≤3 retries; `os.ErrNotExist` is **not** retried (resumable_reader.go:66-135). Rationale: the ltx.Compactor opens all streams upfront and may leave high-page-only streams idle for minutes (resumable_reader.go:18-37). Directly relevant to mobile networks.
- `ltx.NewCompactor(pw, rdrs)` with `HeaderFlags = HeaderFlagNoChecksum` streams the merged file (replica.go:674-684). Compactor: output header takes `PageSize/MinTXID/PreApplyChecksum` from the **first** input, `Commit/MaxTXID/Timestamp` from the **last** (ltx compactor.go:106-120); k-way merge by ascending pgno, and for each pgno the frame is copied **from the latest input** that has it (`writePageBuffer` iterates inputs last→first, ltx compactor.go:199-228); pages with `pgno > final Commit` are dropped (truncation handling, ltx compactor.go:217-218). **Input order = oldest→newest is the caller's responsibility**; the plan's construction order guarantees it. `PostApplyChecksum` = last input's trailer value (ltx compactor.go:140).
- **Per-file verification during restore:** each input decoder's `Close()` runs inside `Compact()` (ltx compactor.go:132-137), which verifies that input's CRC64 file checksum (decoder.go:100-103). So every downloaded byte is integrity-checked, but only *after* it has been consumed — errors surface before the temp file is renamed, so full restore is safe.
- `ltx.Decoder.DecodeDatabaseTo(f)` (decoder.go:221-268) materializes the SQLite image: requires `IsSnapshot()`; writes pages 1..Commit expecting exact sequential pgnos; **zero-fills the lock page** (never present in LTX); demands EOF after page Commit; `Close()` verifies the (pipe-side) checksum. Final DB size = `Commit * PageSize` by construction.
- Write to `OutputPath+".tmp"` → fsync → rename (replica.go:663-701); optional `PRAGMA quick_check|integrity_check`, deleting the output (+`-shm`/`-wal`) on failure (replica.go:703-713, 1259-1293).

## 5. Point-in-time restore mechanics

- Target resolution: `CalcRestoreTarget` computes `[createdAt, updatedAt]` over all levels 9..0 (`TimeBounds`, replica.go:474-495); timestamp outside bounds → error (replica.go:520-528).
- Plan filters: snapshot and level files require `CreatedAt.Before(timestamp)` (strict); TXID mode requires `MaxTXID <= txID`. `useMetadata=!timestamp.IsZero()` selects accurate header timestamps over S3 LastModified (replica.go:1452, 1487).
- **Precision degrades with compaction/retention**: a merged file straddling the target is excluded *entirely* (its `MaxTXID > txID` / `CreatedAt >= timestamp`), so a TXID/timestamp is only restorable while some file chain terminates at/before it. Once L0s covering the target are pruned and only a straddling merged file remains → `ErrTxNotAvailable`. L0 granularity = one TX per file, timestamp = encode time (db.go:1941-1958).

## 6. The incremental reader ("follow mode") — exactly the liters reader case

### State & bootstrap
- Position persisted in a sidecar `<db>-txid` file: 16-hex TXID + newline, written atomically (tmp → fsync → rename → dir fsync; `WriteTXIDFile`/`ReadTXIDFile`, replica.go:1645-1703). Written after initial restore (replica.go:724-728) and after each batch of applies (replica.go:783-790). **Ordering: DB pages are mutated before the sidecar is advanced** — crash between them leaves DB newer than sidecar; safe because re-apply is idempotent (full page images).
- Crash recovery (replica.go:560-598): if DB exists, read sidecar (0/missing ⇒ fatal, re-restore); then validate against the **latest snapshot**: `latestSnapshot.MinTXID > txid` ⇒ history pruned (fatal); `txid > latestSnapshot.MaxTXID` ⇒ "saved TXID ahead of latest snapshot" (fatal, delete & re-restore). Note this second check is very conservative — it fires even when L0 files legitimately cover txid+1.. beyond the snapshot; a liters implementation should instead check *forward coverage* (existence of an applicable chain from txid+1), and treat snapshot-head regression as the reseed signal.

### Poll/apply loop (`applyNewLTXFiles`, replica.go:798-869)
Given local position `N` (`currentTXID`):
1. `LTXFiles(level=0, seek=N+1)` — since L0 min==max, this lists exactly TXIDs > N, ascending.
2. For each L0 info:
   - **Gap** (`info.MinTXID > N+1`): call `fillFollowGap(N, info.MinTXID)`; after bridging, skip the file if now covered, or stop for this tick if the gap persists.
   - Skip if `info.MaxTXID <= N` (covered); else `applyLTXFile`; `N = info.MaxTXID`.
3. If **no L0 files at all** were listed, still call `fillFollowGap(N, N+1)` — handles "all new data lives only in compacted levels" (L0 pruned).
4. Return new N; caller persists sidecar and logs `from_txid`/`to_txid`.

### Gap bridging (`fillFollowGap`, replica.go:934-994)
For `level = 1 .. 8` (**never level 9**; the loop bound is `level < SnapshotLevel`): list all files ascending; `break` to next level at the first gap (`MinTXID > N+1`); skip covered (`MaxTXID <= N`); otherwise **apply the file even if it partially overlaps the position** (`MinTXID <= N+1 && MaxTXID > N`), advance `N = MaxTXID`; return once `N+1 >= gapMinTXID` or once any progress was made at a level.

### Can you apply a file whose MinTXID ≤ N? — YES
This is the core answer to the "compaction replaced my small files" question:
- Eligibility rule = `ltx.IsContiguous(N, MinTXID, MaxTXID)`: `MinTXID <= N+1 && MaxTXID > N`. Follow mode applies such overlapping files directly onto the live image (replica.go:953-972).
- **Safety argument**: LTX frames are full page images, and a file covering `[M, X]` contains, for every page touched anywhere in `[M, X]`, its newest image ≤ X. For a DB at state `N ∈ [M-1, X)`: pages last touched at TX ≤ N are overwritten with bytes the DB already holds; pages touched in `(N, X]` are necessarily present in the file and get their new image; untouched pages are untouched. Result = exact state X. `Commit` truncation handles shrinks. Hence overlapping application is idempotent and convergent.
- **Pre-apply checksum contract**: strictly, `Header.PreApplyPos().TXID = MinTXID-1`, so a checksum-verifying applier could only verify overlap-free applies (`MinTXID == N+1`, then check `PreApplyChecksum == localPos.PostApplyChecksum`). Litestream sidesteps this because all bucket files carry `HeaderFlagNoChecksum` (checksums zero) — follow mode never consults `PreApplyChecksum` at all. liters must adopt the same stance for compatibility: allow overlap, skip pre-apply verification, rely on TXID rules + per-file CRC64 + optional `PRAGMA quick_check`.

### Applying one file in place (`applyLTXFile`, replica.go:879-930; mirrors `Hydrator.ApplyLTX`, vfs.go:754-790)
1. `OpenLTXFile(level, min, max, 0, 0)`; `DecodeHeader()`.
2. **Exclusive OS file lock** on the DB file for the duration (`internal.LockFileExclusive`) so concurrent read-only SQLite connections never see torn pages.
3. Loop `DecodePage` → `WriteAt(data, (pgno-1)*pageSize)`.
4. **Page-1 rewrite**: bytes 18–19 := `0x01 0x01` (file-format read/write version = legacy/rollback-journal, so readers don't hunt for a `-wal`), bytes 24–27 := random (change counter, invalidates other connections' page caches and cached schema) (replica.go:907-910).
5. After pages: if `hdr.Commit > 0`, `Truncate(Commit * pageSize)` (replica.go:918-923) — this is the truncation/commit handling for incremental applies.
6. `dec.Close()` verifies the file CRC64 — **after pages were already written**. A corrupt object is detected (error, position not advanced) but may have poisoned pages until a successful re-apply covers them. A hardened liters applier should buffer/verify before writing, or write to a shadow copy (page count per file is usually small; the page index gives exact sizes up front).
7. `f.Sync()`.
- The lock page is never present in any LTX file and is never written incrementally; it was zero-filled at restore.
- Page size is read from the SQLite header at follow start (offset 16, 2 bytes BE; value 1 ⇒ 65536) (replica.go:747-754). `DefaultFollowInterval = 1s` (db.go:2805).

## 7. Writer-side/compaction facts the reader depends on

- Compaction of level `d`: `seekTXID = maxTXID(d)+1`; read **all** source-level files with `MinTXID >= seekTXID`; merge into one file `(minSrcTXID)-(maxSrcTXID).ltx` at level `d` (compactor.go:104-192). Because of the seek rule, each level 0..8 stays **sorted, non-overlapping, and contiguous** (`prev.MaxTXID+1 == next.MinTXID`) — checked by `ValidateLevel` (replica.go:1716-1778) and `VerifyLevelConsistency` (compactor.go:197-233). Level 9 is *not* contiguous (every snapshot starts at 1) (store.go:832).
- **Write-before-delete**: the merged file is PUT to level d, and source deletion happens later on independent retention schedules — coverage is duplicated, never lost:
  - L0: deleted only when `MaxTXID <= maxL1TXID` **and** older than `L0Retention` (time buffer explicitly "ensures contiguous L0 coverage for VFS reads"; compactor.go:339-426).
  - L1..L8: deleted when `MaxTXID < minRetainedSnapshotTXID`, always keeping ≥1 file per level (compactor.go:291-337; store.go:801-822).
  - Snapshots: age-based, newest always kept (compactor.go:238-289).
- Snapshot encoding: streamed from the live DB + WAL page map with `MinTXID=1, MaxTXID=pos.TXID, Flags=NoChecksum`, Commit from file size/WAL commit (db.go:2338-2440).

## 8. Read-after-list races (404s)

Between LIST and GET, retention/compaction may delete a listed file → `OpenLTXFile` returns `os.ErrNotExist`. `ResumableReader` deliberately does **not** retry not-found (resumable_reader.go:75-77); the restore/apply fails, position is not advanced, and the next iteration re-LISTs — the post-compaction listing contains a merged higher-level file overlapping the reader's position, which the overlap rules accept. The fuzz test (§3) is the regression proof. **Rule for liters: a 404 on GET invalidates the current plan/batch, never the local replica; re-list, re-plan, retry with backoff.** (Litestream's monitor uses exponential backoff `SyncInterval → ×2 → cap DefaultSyncBackoffMax = 5m`, rate-limited logging at `SyncErrorLogInterval = 30s`; replica.go:328-451, db.go:41-42.)

## 9. Divergence / reset detection

Signals and litestream behavior:
- **TXID regression / bucket wiped & reseeded**: after a wipe, all files (including L0 heads and the snapshot) have `MaxTXID` below the reader's saved position, and `LTXFiles(0, seek=N+1)` is empty with no bridge at any level ⇒ follow mode simply stalls forever (fillFollowGap never consults level 9). Follow *startup* catches it via `txid > latestSnapshot.MaxTXID` ⇒ fatal "delete and re-restore" (replica.go:591-593). There is **no epoch/generation marker in the v0.5 LTX bucket layout** (generations are a v0.3 concept), so regression must be detected by tracking the bucket-wide max TXID: **if max(MaxTXID over levels 9..0) < localTXID, declare divergence and reset** (discard local file + sidecar, full re-restore).
- **Snapshot-only recovery / history pruned**: if `LTXFiles(0, seek=N+1)` shows a gap unbridgeable through L8 but a snapshot with `MaxTXID > N` exists, the snapshot itself satisfies `IsContiguous(N, 1, MaxTXID)` and may be applied in place (it contains every page) — equivalent to re-restore without deleting the file. Litestream doesn't do this in follow mode (stalls); liters should.
- **Corruption/missing local files** (writer side): `LTXError.IsAutoRecoverable()` returns true for not-exist / `ErrLTXCorrupted` / `ErrChecksumMismatch` and, when `AutoRecoverEnabled`, triggers `db.ResetLocalState` (delete local `-litestream` state, resync) (litestream.go:39-93; replica.go:395-430; db.go:474).
- Because checksums are disabled bucket-wide, **there is no cryptographic tie between a reader's local image and the bucket lineage**. liters should persist alongside the TXID sidecar: last-applied file's `FileChecksum`+range, and last-seen bucket max TXID, to distinguish "no new data" from "reseeded bucket at lower TXID".

## 10. Invariants a correct incremental applier MUST enforce

1. **Apply-eligibility**: apply file `(min,max)` at local position `N` iff `min <= N+1 && max > N` (`ltx.IsContiguous`). Never apply across a gap (`min > N+1`); always skip fully-covered files (`max <= N`). Overlap (`min <= N`) is legal and required for compaction survival.
2. **Ordering**: within a batch, apply strictly by the chain order produced by the plan/level scan (each file contiguous-or-overlapping with the running position); after each file, `N := max`.
3. **Truncate after every file** when `Commit > 0`: size := `Commit * pageSize`.
4. **Verify per-file CRC64** (trailer FileChecksum) — ideally *before* mutating the live image (litestream verifies after; do better on mobile). Also validate `Header.Validate()` (magic, version=3, flags mask, page size ∈ {512..65536 pow2}, `MinTXID>0`, `Min<=Max`).
5. **Never write the lock page**; zero-fill it on full materialization.
6. **Position persistence**: atomic (tmp+fsync+rename), monotonically increasing, persisted after apply; recovery from apply-then-crash is safe only because applies are idempotent — therefore never partially apply a file's pages without eventually completing or re-applying the whole file.
7. **Concurrent-reader safety**: exclusive-lock the DB file (or swap via rename) during page writes; force journal-mode bytes 18/19 to `0x01` and randomize change counter bytes 24–27 on any applied page 1; consumers open read-only.
8. **Gap handling**: L0 gap ⇒ scan levels 1..8 ascending for a bridging file (levels are internally contiguous, so binary search on sorted listings is valid); no bridge ⇒ fall back to a snapshot with `MaxTXID > N` (apply in place or re-restore); no such snapshot ⇒ stall (transient) or, if bucket max TXID < N, reset (divergence).
9. **404 on GET ⇒ re-list & re-plan**, never fatal, never advance position.
10. **Latest-restore gap check**: when computing "current head", refuse a result if any level holds a file starting beyond `currentMax+1` (replica.go:1538-1547 semantics) — prevents serving a stale head when the bridge files were lost.
11. **Page size immutability**: reject inputs whose PageSize differs from the chain's (ltx compactor.go:95-97); a page-size change (VACUUM into new size) implies a bucket reset.

## 11. Misc constants & structs worth mirroring in liters

- `ltx.FileInfo{Level int; MinTXID, MaxTXID TXID; PreApplyChecksum, PostApplyChecksum Checksum; Size int64; CreatedAt time.Time}` (ltx.go:571-580); `FileInfoSlice` sort: Level then MinTXID (ltx.go:598-610).
- `RestoreOptions{OutputPath, TXID, Timestamp, Parallelism(=8), Follow, FollowInterval(=1s), IntegrityCheck}` (db.go:2816-2851).
- Minimum plausible file size = `ltx.HeaderSize` (100) — plan rejects smaller `info.Size` (replica.go:640-643).
- `DefaultSyncInterval = 1s` (replica.go:24); `SnapshotLevel = 9`; `ErrTxNotAvailable` (store.go:27); `ErrNoSnapshots`, `ErrChecksumMismatch`, `ErrLTXCorrupted`, `ErrLTXMissing` (litestream.go:32-37).
- VFS-style partial application alternative for constrained bandwidth: fetch page index (last 32KB range), then range-GET only needed page frames (`FetchPageIndex`/`FetchPage`, replica_client.go:90-160; `DecodePageData` decompresses a single frame, decoder.go:296-307) — valid because the trailer contains absolute offsets/sizes; note this bypasses whole-file CRC verification, so pair it with the per-frame LZ4 framing integrity only.

Key files: `/Users/kurt/code/liters/reference/litestream/replica.go` (Restore 544-732, follow 737-994, CalcRestorePlan 1441-1637, TXID sidecar 1645-1703, ValidateLevel 1716-1778), `/Users/kurt/code/liters/reference/litestream/compactor.go` (Compact 104-192, retention 238-426), `/Users/kurt/code/liters/reference/litestream/vfs.go` (Hydrator Restore/CatchUp/ApplyLTX 676-790), `/Users/kurt/code/liters/reference/litestream/internal/resumable_reader.go`, `/Users/kurt/go/pkg/mod/github.com/superfly/ltx@v0.5.1/{ltx.go,compactor.go,decoder.go,encoder.go,checksum.go}`, `/Users/kurt/code/liters/reference/litestream/restore_fuzz_test.go`.
