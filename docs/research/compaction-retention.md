# Litestream v0.5.x Compaction & Retention — Complete Mechanics and Fleet Compactor Analysis

Reference tree: `benbjohnson/litestream` at `v0.5.14-1-gc96c0f4`; LTX library `github.com/superfly/ltx v0.5.1`.

## 1. Level scheme

Constants (`compaction_level.go`, `store.go`):

| Constant | Value | Location |
|---|---|---|
| `SnapshotLevel` | `9` | compaction_level.go:9 |
| `DefaultCompactionLevels` | L0: interval 0 (raw); L1: 30s; L2: 5m; L3: 1h | compaction_level.go:14-19 |
| Max configurable level | `SnapshotLevel-1 = 8` | compaction_level.go:72-73 |
| `DefaultSnapshotInterval` | 24h | store.go:60 |
| `DefaultSnapshotRetention` | 24h | store.go:61 |
| `DefaultL0Retention` | 5m | store.go:68 |
| `DefaultL0RetentionCheckInterval` | 15s | store.go:72 |
| `DefaultRetentionCheckInterval` | 1h (declared, effectively unused; retention piggybacks the snapshot cycle) | store.go:64 |

- L0 = raw LTX files produced by each writer sync (one file per sync batch; a batch may cover multiple commits, filename `minTXID-maxTXID`).
- L(n) for n≥1 is compacted from L(n-1). `CompactionLevels.NextLevel` chains `maxLevel → SnapshotLevel(9)` (compaction_level.go:106-113); `PrevLevel(SnapshotLevel) == MaxLevel()` (:97-102). Levels 4–8 exist in namespace but are unused by default config.
- **Trigger**: `Store.monitorCompactionLevel` (store.go:538-604) runs one goroutine per level ≥1 plus one for the snapshot pseudo-level (`Store.SnapshotLevel()` = `{Level: 9, Interval: SnapshotInterval}`, store.go:531-536). Compaction times are **wall-clock aligned**: `PrevCompactionAt(now) = now.Truncate(interval).UTC()`, `NextCompactionAt = PrevCompactionAt + interval` (compaction_level.go:33-41). The first timer fires immediately (`time.NewTimer(time.Nanosecond)`, store.go:542), so a snapshot is taken at process start if none is recent.
- **Re-compaction guard**: `Store.CompactDB` (store.go:745-799) fetches `MaxLTXFileInfo(dstLevel)`; if `dstInfo.CreatedAt.After(prevCompactionAt)` → `ErrCompactionTooEarly` (store.go:754-759). If `srcInfo.MaxTXID <= dstInfo.MinTXID` → `ErrNoCompaction` (store.go:780-782). `db.PageSize()==0` → `ErrDBNotReady` (store.go:747-749).

### Bucket layout (wire compatibility)
- Key: `<path>/<level as %04x>/<FormatFilename(minTXID,maxTXID)>` where `FormatFilename = "%016x-%016x.ltx"` (s3/replica_client.go:654-656, 702-703; ltx.go:486-489, regex `^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$` ltx.go:485). Snapshot dir = `0009/`. Fixed-width hex makes lexicographic list order == TXID order.
- Lease object (if used): `<path>/lock.json` (s3/leaser.go:22-24, 84-89).
- S3 `WriteLTXFile` peeks the LTX header and stores `hdr.Timestamp` in object metadata (`MetadataKeyTimestamp`, RFC3339Nano) so `CreatedAt` can be recovered accurately; default listing uses S3 `LastModified` (s3/replica_client.go:681-717). `LTXFiles(ctx, level, seekTXID, useMetadata)` returns files at a level with `MaxTXID >= seek`, ordered by filename.

## 2. Compaction algorithm

### Input selection — `Compactor.Compact(ctx, dstLevel)` (compactor.go:104-192)
1. `srcLevel = dstLevel - 1` (:105).
2. `prevMaxInfo = MaxLTXFileInfo(dstLevel)` — the file with greatest `MaxTXID` at the destination (cached via `CacheGetter/CacheSetter`; DB caches in `db.maxLTXFileInfos`, db.go:323-331; VFS has no cache → full level list each time).
3. `seekTXID = prevMaxInfo.MaxTXID + 1` (:111); list **all** src files with `MaxTXID >= seekTXID` (:113).
4. For each src file: prefer local copy via `LocalFileOpener` (DB wires this to its on-disk LTX cache, db.go:320), fall back to remote `OpenLTXFile` wrapped in `internal.NewResumableReader` (:139-152), which reopens at byte offset on premature EOF/conn reset using the **known listing size** — this is the defense against eventually-consistent stores exposing truncated objects (resumable_reader.go:66-132; regression test store_compaction_remote_test.go:26-100).
5. If zero inputs → `ErrNoCompaction` (:154-156).
6. Output TXID range: `minTXID = min(inputs.MinTXID)`, `maxTXID = max(inputs.MaxTXID)` (:128-137).
7. Pipe through `ltx.NewCompactor(pw, rdrs)` with `HeaderFlags = ltx.HeaderFlagNoChecksum` (:158-167), stream into `client.WriteLTXFile(ctx, dstLevel, minTXID, maxTXID, pr)` (:169). **Source files are NOT deleted here** — deletion is a separate retention pass.
8. Optional `VerifyCompaction`: `VerifyLevelConsistency` asserts `prev.MaxTXID+1 == curr.MinTXID` for consecutive files at dst level; gap/overlap → logged warning + counter, not fatal (:180-233).

### Merge semantics — `ltx.Compactor` (ltx@v0.5.1/compactor.go)
- Inputs must be pre-sorted by TXID; validation: identical `PageSize` for all inputs, and TXID contiguity `IsContiguous(prevMax, min, max) = min <= prevMax+1 && max > prevMax` (compactor.go:91-104; ltx.go:623-625) — overlap allowed if it advances; gaps rejected unless `AllowNonContiguousTXIDs`.
- Output header synthesized (compactor.go:111-120): `Version`, `Flags=HeaderFlags`, `PageSize=first.PageSize`, `Commit=last.Commit` (final DB size in pages), `MinTXID=first.MinTXID`, `MaxTXID=last.MaxTXID`, `Timestamp=last.Timestamp`, `PreApplyChecksum=first.PreApplyChecksum`, `NodeID` dropped.
- **Page dedup rule**: k-way merge over inputs, each ordered by ascending pgno. Each round picks lowest pending `pgno` across inputs (`fillPageBuffers`, :177-196), then `writePageBuffer` (:199-228) scans inputs **from last (newest) to first** and emits the newest version of the page exactly once; pages with `pgno > Commit` are dropped (truncated by a later `VACUUM`/shrink); buffers of older duplicates are consumed and discarded.
- Lock page: encoder refuses to encode `LockPgno = PENDING_BYTE/pageSize + 1`, `PENDING_BYTE = 0x40000000` (encoder.go:219-233; ltx.go:494-499).
- Trailer: `PostApplyChecksum` copied from the **last input's trailer** (compactor.go:140); with `HeaderFlagNoChecksum` (bit `1<<1`, ltx.go:175) pre/post checksums are 0 and not validated; `FileChecksum` (CRC64 of file) is always computed by the encoder.
- Byte layouts (big-endian): `HeaderSize=100`: magic `"LTX\x00"`@0, Flags u32@4, PageSize u32@8, Commit u32@12, MinTXID u64@16, MaxTXID u64@24, Timestamp i64(ms)@32, PreApplyChecksum u64@40, WALOffset i64@48, WALSize i64@56, WALSalt1 u32@64, WALSalt2 u32@68, NodeID u64@72, zero pad to 100 (ltx.go:283-299). `PageHeaderSize=6`: Pgno u32@0, Flags u16@4 (:431-436). `TrailerSize=16`: PostApplyChecksum u64@0, FileChecksum u64@8 (:377-382).

### Invariants
- Within every level ≥1, files are TXID-contiguous and non-overlapping (enforced by seek-from-dst-max input selection; checked by `VerifyLevelConsistency` and `Store.Validate`/`ValidationInterval` monitor, store.go:833-895).
- L0 must retain contiguous coverage from `maxL1TXID` forward (VFS readers apply L0 deltas on top of L1+; see retention rules below).
- `Header.IsSnapshot() == (MinTXID == 1)`; snapshots must have `PreApplyChecksum == 0` (ltx.go:198-200, 247-250).

## 3. Snapshot level (L9) mechanics

- Snapshots are **full-image LTX files** with `MinTXID=1, MaxTXID=current pos`, written directly to level 9 — not compacted from L3. `Store.CompactDB` short-circuits: `if dstLevel == SnapshotLevel → db.Snapshot(ctx)` (store.go:762-770).
- Writer-side (`DB.Snapshot` → `SnapshotReader`, db.go:2337-2480): reads every page from the local DB file + WAL page map, encodes header with `WALOffset/WALSize/WALSalt1/2` for WAL continuity, uploads via `WriteLTXFile(SnapshotLevel, 1, pos.TXID, r)`.
- Remote-only (`VFSFile.Snapshot`, vfs.go:2915-2987): builds the full image **without any local DB** by walking its page index (pgno → {level,minTXID,maxTXID,offset,size}) and range-reading each page from remote LTX files; header `MinTXID=1, MaxTXID=pos.TXID`, no WAL fields. Proves L9 production is possible purely from bucket contents.
- Cadence: every `SnapshotInterval` (default 24h), plus effectively at process start.
- **Why restores are bounded**: `CalcRestorePlan` (replica.go:1440-1557) picks the newest snapshot ≤ target TXID/timestamp, then greedily extends the contiguous TXID range using per-level cursors, preferring the candidate covering the most TXIDs (higher levels first). Restore cost = 1 snapshot + O(few) L3 + O(few) L2 + O(few) L1 + ≤(L1 interval worth) of L0 files. Errors: `ErrTxNotAvailable` if the chain can't reach the target; explicit "non-contiguous ltx files" error if a gap is detected (replica.go:1538-1547).

## 4. Retention / GC — what is deleted, when, by whom

All deletion is performed by the **same process that runs compaction** (Store or VFS monitors). Nothing bucket-side triggers it. `RetentionEnabled=false` (config `retention.enabled`) suppresses remote deletes so provider lifecycle policies can do it; local cache cleanup still happens (compactor.go:29-32, 270-274, 318-321, 406-410).

1. **Snapshot retention (time-based)** — runs after each snapshot-level cycle (`Store.monitorCompactionLevel`, store.go:573-578 → `Store.EnforceSnapshotRetention`, store.go:803-823):
   - `db.EnforceSnapshotRetention(ctx, now - SnapshotRetention)` (db.go:2483-2541): deletes L9 files with `CreatedAt < cutoff`, **always keeping the newest snapshot** even if expired (db.go:2509-2511). Returns `minSnapshotTXID` = `MaxTXID` **of the snapshot immediately preceding the oldest retained one** (db.go:2513-2521; commit 2b34013 "retain prior snapshot txid" — keeps lower-level files needed by restore plans computed against the just-deleted snapshot; 0 if oldest retained snapshot is the first ever).
   - Cascade: for each level 1..maxLevel, `EnforceRetentionByTXID(level, minSnapshotTXID)` deletes files with `MaxTXID < minSnapshotTXID`, always keeping ≥1 file per level (store.go:811-820; compactor.go:291-337).
   - Compactor's own variant `Compactor.EnforceSnapshotRetention` (compactor.go:238-289, used by VFS at vfs.go:3044-3049) returns min `MaxTXID` among **retained** snapshots instead (slightly more aggressive floor).
2. **L0 retention (time + progress based)** — `EnforceL0RetentionByTime` (db.go:2545-2667) / `Compactor.EnforceL0Retention` (compactor.go:342-426). Runs every 15s (store.go:606-632) and after every L1 compaction (db.go:2451-2459). An L0 file is deleted only if **all** of:
   - `MaxTXID <= maxL1TXID` (already compacted into L1), and
   - `CreatedAt <= now - L0Retention` (default 5m grace so racing readers finish), and
   - deletion never leaves a gap: iteration **stops at the first too-recent file** ("L0 entries are ordered; once we reach a newer file we stop so we don't create gaps… VFS expects contiguous coverage", db.go:2605-2611), and
   - the newest L0 file is never deleted if the whole level was scanned (db.go:2627-2630).
3. L1..L3 files are otherwise only deleted by the snapshot-retention cascade — i.e., intermediate levels accumulate for up to `SnapshotRetention` (default 24h) then are trimmed to the retained-snapshot floor.

## 5. Crash-safety of compaction; readers racing compaction

- **Write-then-delete, widely separated**: compaction writes the merged dst file; source files remain until an independent retention pass whose eligibility additionally requires the 5-minute L0 age grace. A crash between write and delete leaves duplicate coverage across two levels — harmless: restore/VFS pick per-TXID-range and duplicates at *different* levels are fine; re-running compaction resumes at `dst.MaxTXID+1` so the same range is never re-compacted into overlapping dst files.
- **Restart storm guard**: `ErrCompactionTooEarly` via wall-clock-truncated `PrevCompactionAt` prevents re-compaction after restart within the same interval bucket (store.go:754-759).
- **Partial-object reads**: `ResumableReader` + known listing size detects truncated GETs and resumes at offset (up to 3 retries), preventing corrupted dst files from eventually-consistent stores (resumable_reader.go:64-132).
- **Readers racing GC**: a reader that already planned a restore keeps working because (a) deletes lag compaction by `L0Retention`, (b) snapshot floor is the *previous* snapshot's TXID, (c) newest file per level is never deleted. A reader that starts mid-GC re-lists and re-plans; `CalcRestorePlan` fails loudly on gaps rather than producing a wrong image.
- **Atomicity assumption**: object PUT is atomic (single PUT; S3 client uploads whole stream). No rename/temp-file dance is needed because filenames are content-determined (`min-max.ltx`) and a re-run writes the identical key.
- Residual race (accepted by design): two concurrent compactors at the same level can write **overlapping dst files** (e.g., `0000..0010.ltx` and `0000..0012.ltx` at L1) because there is no bucket-side locking; `VerifyLevelConsistency` flags this as "TXID overlap". Litestream avoids it by having exactly one process own compaction per DB prefix.

## 6. Lease / heartbeat — what they are and are NOT

- **Leaser** (`leaser.go:24-45`): interface `AcquireLease/RenewLease/ReleaseLease` returning `Lease{Generation int64, ExpiresAt time.Time, Owner string, ETag string}`. S3 impl (`s3/leaser.go`): object `<path>/lock.json`, `DefaultLeaseTTL=30s`, owner `hostname:pid`, acquisition via conditional PUT (`If-None-Match:*` for fresh, `If-Match:<etag>` for expired takeover; 412 → `LeaseExistsError`), release via `DeleteObject If-Match`.
- **Critical finding: the Leaser is NOT wired into anything in v0.5.14.** `AcquireLease` has zero non-test callers (verified by grep across the tree — only `leaser.go`, `s3/leaser.go` impl, s3 tests, docs). Neither `Store.CompactDB`, `Compactor.Compact`, `Replica.Sync`, nor the VFS takes a lease. It is scaffolding for multi-instance coordination (docs/ARCHITECTURE.md §Distributed Leasing), not an active multi-writer or compaction exclusion mechanism. Single-writer-per-bucket-prefix is an **operational convention**, not enforced.
- **Heartbeat** (`heartbeat.go`): plain HTTP GET dead-man's-switch to a health-check URL (`DefaultHeartbeatInterval=5m`, min 1m, timeout 30s), sent by the Store only when **all** open DBs synced within the interval (store.go:691-741). Purely observability; no bucket objects; no coordination role.

## 7. Consequences of never compacting

With mobile writers pushing raw L0 only:
- **Restore plan = newest snapshot + every L0 file after it.** With no L9 snapshots at all, the plan is the initial L0 snapshot (first-ever sync produces an LTX with `MinTXID=1`, i.e. a full image) + *every* L0 file since DB creation. Restore cost is O(total commits): N sequential range-GETs (one object each), N header decodes, and a k-way merge whose input count is N. `CalcRestorePlan` itself is O(N) listing time; the replica-client test suite asserts plans stay <30s with many files (replica_client_test.go:975-1061) — a fleet DB pushing every few seconds crosses thousands of objects within a day.
- **List amplification**: `MaxLTXFileInfo` and `EnforceL0Retention` iterate the full level listing; S3 LIST is 1000 keys/page. Every reader poll pays this.
- **No GC is possible**: L0 deletion is gated on `MaxTXID <= maxL1TXID` (db.go:2613); with no L1 files, `maxL1TXID==0` → nothing is ever deleted (db.go:2569-2574). Storage and object count grow without bound; retention windows are meaningless.
- **Reader/VFS cost**: a VFS-style reader must fetch/index every L0 file's page index to serve reads; hydration converges only if compaction bounds the file count.
- Cost curve: restore latency grows ~linearly in commits-since-snapshot; snapshotting alone (option c) flattens it to "snapshot size + ≤1 snapshot-interval of L0", which is the actual bound that matters.

## 8. Who must run compaction in a mobile-writer fleet — options for liters

Facts established above that drive the decision:
- `litestream.Compactor` operates **solely through the `ReplicaClient` interface** ("suitable for both DB (with local file caching) and VFS (remote-only)", compactor.go:17-19) — L1..L3 compaction and all retention need no local database.
- L9 snapshots can also be produced remote-only (`VFSFile.Snapshot`, vfs.go:2915-2987) by merging bucket LTX files (equivalently: `ltx.Compactor` over snapshot+deltas yields a `MinTXID=1` file).
- The **stock `litestream` CLI cannot do this**: there is no `compact` subcommand (cmd/litestream/ has replicate/restore/info/ltx/sync/…); `litestream replicate` requires locally-attached DBs (`Store.CompactDB` gates on `db.PageSize()!=0`, and L9 uses `db.Snapshot` reading the local file). The VFS compaction path (`VFS.CompactionEnabled`, vfs.go:86-103, 170-173, 2860-2904) *is* remote-capable but ships as a library/extension, not a fleet daemon.
- There is no compaction lock: whoever compacts must be **exactly one process per DB prefix** by construction.

### (a) Writer-side (device) compaction on push
Device runs L1/L2/L3 + snapshot + retention after each push burst. Correct (device is the sole writer, so it's trivially the sole compactor) and needs no server. Costs: compaction downloads the src level (unless liters keeps local copies of its own pushed L0s — mirror litestream's `LocalFileOpener` trick, compactor.go:38-41, so L0→L1 is upload-only); snapshots re-upload the full DB image over mobile bandwidth every interval; retention DELETE fan-out on metered radio; work is lost/deferred while app is backgrounded/killed; wall-clock-aligned intervals are meaningless for sporadic app usage — trigger by "on push, if `dst.CreatedAt < now-interval`" instead (same gate as store.go:754-759). Devices being offline for days simply means L0 accumulates; nothing breaks (restore just gets slower).

### (b) Server-side compactor
Not stock `litestream` CLI (verified: no such command; replicate needs the writer DB). But a ~100-line Go daemon using litestream-as-library is fully supported today: per DB prefix, `c := litestream.NewCompactor(replicaClient, logger)`; loop `c.Compact(ctx, 1..3)`; snapshot via a VFS-style page-index merge or `ltx.Compactor` over (latest snapshot + all newer files) writing `1-<max>.ltx` to level 9; then `Compactor.EnforceSnapshotRetention` + `EnforceRetentionByTXID` + `EnforceL0Retention` (all remote-only, compactor.go:238-426). This is exactly what `vfs_compaction_test.go:37-76` exercises (`NewCompactor(client, …).Compact(ctx, 1)` with no DB). For N devices × M databases, one daemon can iterate all prefixes; use the (currently unwired) S3 leaser pattern (`lock.json`, conditional writes) if you ever run >1 compactor instance.

### (c) Periodic snapshot-only from device
Device occasionally uploads a full L9 snapshot (it has the complete DB locally — a snapshot is a linear read + upload, much simpler than merge logic) and optionally deletes L0 older than the previous snapshot. Bounds restore to "snapshot + L0 since last snapshot". No inter-level merge code, no contiguity bookkeeping beyond the L9 write. Costs: full-image upload each time (bad for large DBs, fine ≤ tens of MB); between snapshots restore cost still grows linearly; L1-L3 stay empty (readers/restore fall back to many L0 GETs — wire-compatible, `CalcRestorePlan` handles it).

### Recommendation
**Primary: (c) on-device snapshot + L0 trim, with (b) as the fleet-scale upgrade path. Skip (a).**

Rationale:
- Mobile SQLite DBs are typically small (KBs–low MBs); a full snapshot upload is comparable in cost to a handful of L0 pushes and is *one* PUT vs. a download-merge-upload cycle. Level-merge compaction's value is amortizing huge L0 volumes — mobile writers produce modest volumes, so snapshot cadence ("every K pushes or every T hours or when Σ L0 bytes > X× DB size") plus deleting L0 files with `MaxTXID <= prevSnapshot.MaxTXID` and `CreatedAt < now-5m` (respecting the keep-newest + no-gap rules of db.go:2605-2630) gives 95% of the benefit with ~10% of the code. Liters must implement: L9 snapshot encode (it already needs the LTX encoder for pushes), retention-by-TXID delete, keep-newest guards.
- (a) full multi-level compaction on-device buys little over (c) for small DBs and imports all of litestream's contiguity/verification machinery into constrained clients.
- (b) becomes attractive when: DBs are large (snapshot upload too costly), fleets need uniform retention policy enforcement, or bucket-consuming read replicas need tight L0 windows. Because liters is wire-compatible, a server-side library-based compactor can be added later with **zero device changes** — but establish the ownership rule now: *the device owns L0 writes; exactly one party (device via (c), or server via (b)) owns levels ≥1 and all deletes per DB prefix; never both*, since v0.5.x has no bucket-side compaction lock (leaser exists at `s3/leaser.go` but is unwired).
- Whatever runs retention must preserve litestream's invariants or stock readers break: keep newest snapshot always; keep ≥1 file per level; never create L0 gaps above `maxL1TXID` (or above `prevSnapshot.MaxTXID` in snapshot-only mode); delay L0 deletes ≥ `DefaultL0Retention` (5m) after the covering snapshot/compaction lands.

Key files: `reference/litestream/compactor.go` (Compact :104-192, retention :238-426), `compaction_level.go` (:9-41), `store.go` (monitors :538-632, CompactDB :745-799, retention cascade :803-823, defaults :59-81), `db.go` (Snapshot :2337-2480, L0 retention :2545-2667, snapshot retention :2483-2541), `vfs.go` (remote-only compaction/snapshot :2860-3081), `replica.go` (CalcRestorePlan :1440-1557), `leaser.go` + `s3/leaser.go` (unwired lease), `heartbeat.go` (health ping only), `ltx@v0.5.1/compactor.go` (merge :78-228), `ltx@v0.5.1/ltx.go` (header/trailer layouts :283-299, :377-393; IsContiguous :623-625; FormatFilename :486-489).
