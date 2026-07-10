# Litestream experimental read-replica VFS (`vfs.go`) — analysis for liters' read path

## 1. What it is

- Build-tagged Go file (`//go:build vfs`, vfs.go:1) implementing a SQLite VFS over `github.com/psanford/sqlite3vfs`, shipped as a CGO loadable extension / static lib (docs/VFS.md "Building"). Configured by env vars (`LITESTREAM_REPLICA_URL`, `LITESTREAM_WRITE_ENABLED`, `LITESTREAM_SYNC_INTERVAL`, `LITESTREAM_BUFFER_PATH`).
- It presents the remote LTX object set (`{root}/ltx/{level}/{minTXID:016x}-{maxTXID:016x}.ltx`, litestream.go:184-197; levels 0..3 + `SnapshotLevel = 9`, compaction_level.go:9-19) as a **rollback-journal-mode** SQLite main database file. There is **no WAL/shm support**: `Access("*-wal")` always returns false (vfs.go:248-250), and psanford/sqlite3vfs has no shm methods. Journals, temp DBs, transient files (`OpenMainJournal|OpenTempDB|OpenTempJournal|OpenSubJournal|OpenSuperJournal|OpenTransientDB|DeleteOnClose`, vfs.go:257-268) are real local files in an `os.MkdirTemp("", "litestream-vfs-*")` dir, name-hashed with FNV-64a (vfs.go:293-341).

## 2. Where pages come from (xRead path, vfs.go:1422-1540)

`pgno = off/pageSize + 1`, `pageOffset = off % pageSize`. Priority order:
1. **Dirty write buffer** (write mode only): `dirty map[uint32]int64` pgno→offset in a local append-only buffer file (vfs.go:1433-1455).
2. **Hydrated local file** if background hydration completed (vfs.go:1457-1460).
3. **In-memory LRU page cache**: `lru.Cache[uint32, []byte]`, capacity = `CacheSize/pageSize` entries (`DefaultCacheSize = 10MB`, `DefaultPageSize = 4096`, vfs.go:31-33, 1042-1051).
4. **Ranged GET from the bucket**: `FetchPage(ctx, client, elem.Level, elem.MinTXID, elem.MaxTXID, elem.Offset, elem.Size)` — one HTTP range request per page miss, returning a self-contained frame: 6-byte page header (u32 pgno BE, u16 flags BE) + an independent LZ4 frame per page (encoder resets the LZ4 writer per page, ltx encoder.go:276-302; decode: `DecodePageData`, ltx decoder.go:296-307). Result added to LRU.

**Page-1 spoofing** (critical trick, applied on every read of offset 0 in all three paths — vfs.go:1447-1451, 1467-1471, 1533-1537, hydrator vfs.go:802-806): bytes 18,19 (file-format write/read version) forced to `0x01 0x01` (legacy rollback journal), and bytes 24..28 (file change counter) **randomized**. Since the change counter then never matches the version-valid-for counter at offset 92, SQLite (a) invalidates its pager cache at each new read transaction and (b) distrusts the in-header database size and calls `xFileSize` instead. `FileSize()` = max pgno across `index`+`pending`+`dirty` × pageSize (vfs.go:2153-2180). The lock page (`pgno = 0x40000000/pageSize + 1`, ltx.go:487-496) is never stored; reading it errors "page not found" (contract test vfs_test.go:406-451).

Page-fetch retry: `pageFetchRetryAttempts = 6`, linear backoff `15ms × attempt` (~315 ms total, vfs.go:35-37, 1499-1525). Retryable = context deadline/cancel, `io.ErrUnexpectedEOF`, string "unexpected EOF", **`os.ErrNotExist`** (vfs.go:2451-2469). After retries exhausted → **`SQLITE_BUSY`** to the statement; recovery depends on the next poll re-pointing the index at L1 objects.

## 3. Page index

```go
// ltx encoder.go:309-316
type PageIndexElem struct {
    Level   int      // which compaction level the object lives in
    MinTXID TXID     // object identity: {MinTXID}-{MaxTXID}.ltx under level dir
    MaxTXID TXID
    Offset  int64    // byte offset of page frame within LTX object
    Size    int64    // frame length (6-byte header + LZ4 data)
}
// VFSFile (vfs.go:513-565): index, pending map[uint32]ltx.PageIndexElem
```

**On-disk index format** (tail of every LTX object; encoder.go:80-173): after the page block's zero page-header terminator: repeated `uvarint pgno, uvarint offset, uvarint size`, sorted by pgno; end marker `uvarint 0`; then `u64 BE index_size`; then 16-byte trailer (`PostApplyChecksum u64, FileChecksum u64`). LTX header = 100 bytes: magic "LTX1"@0, flags u32@4 (`HeaderFlagNoChecksum = 1<<1`), pageSize u32@8, commit u32@12, minTXID u64@16, maxTXID u64@24, timestamp-ms u64@32, preApplyChecksum u64@40, walOffset u64@48, walSize u64@56, walSalt1 u32@64, walSalt2 u32@68, nodeID u64@72 (ltx.go:26-30, 283-299).

**Index fetch** (`FetchPageIndex`, replica_client.go:88-145): range-GET the last `max(32KB, size)` of the object (`DefaultEstimatedPageIndexSize = 32*1024`), read `index_size` from `b[len-TrailerSize-8:]`; if 32KB guess was short, a second range-GET from `info.Size - TrailerSize - 8 - index_size`. Header is a **separate** 100-byte range GET (`FetchLTXHeader`, replica_client.go:101-112) — 2-3 GETs per plan file.

**Build at open** (`Open` vfs.go:1020-1106): `waitForRestorePlan` loops `CalcRestorePlan` until files exist (blocks forever offline in read-only mode, vfs.go:2733-2761). `CalcRestorePlan` (replica.go:1441-1557): LIST snapshot level 9, take latest snapshot; then merge-cursor across levels 3→0 selecting files that extend the contiguous TXID chain; errors `non-contiguous ltx files` on gaps; `ErrTxNotAvailable` if empty. `buildIndexMap` (vfs.go:1250-1280) then overlays each plan file's index in order (later TXIDs win per pgno); `commit` = last header's Commit; `pos.TXID` = last file's MaxTXID; `maxTXID1` seeded from L1 files or pos (vfs.go:1211-1215; tests vfs_test.go:337-385).

**Cold-start cost (large DB)**: ~5 LIST calls + (2-3 GETs × plan files). A snapshot's index has one entry per page (~10 bytes serialized): 1 GiB DB @ 4 KB pages = 262,144 entries ≈ 2.5 MB index download (so always the second full-index GET), materialized as a Go map — 40 B struct + map overhead ≈ 25-100 MB RAM per GB of database. Index lives fully in memory; nothing about it persists across restarts. Memory is bounded by distinct pgno count (test vfs_test.go:227-250).

## 4. New-TXID discovery & mid-transaction invalidation

- Background goroutine `monitorReplicaClient` ticks every `PollInterval` (**default 1 s**, vfs.go:31, 2471-2496); on success stamps `lastPollSuccess` (surfaced as `litestream_lag`, FileControl vfs.go:2352-2363).
- `pollReplicaClient` (vfs.go:2500-2622) polls **L0 from pos.TXID+1** and **L1 from maxTXID1+1** via `pollLevel` (vfs.go:2626-2679). Contiguity invariant: next file's MinTXID must equal maxTXID+1. **L0 gap → warn + break, defer to L1** (retention deleted L0s, vfs.go:2646-2649); L1 gap → hard error and position freezes (test vfs_test.go:209-225). For each new file: fetch page index + header (2 GETs).
- **Commit shrink** (VACUUM/auto-vacuum): if a new header's `Commit` < running commit → `replaceIndex = true`, entire index rebuilt from that file forward, full cache purge, FileSize shrinks (vfs.go:2664-2667; tests vfs_test.go:252-325).
- **Snapshot stability for open read txns**: if `lockType >= LockShared`, poll results go into `pending` (+`pendingReplace`) instead of `index`; cache untouched. On `Unlock` to SHARED/NONE, pending is merged into index and each merged pgno evicted from cache (or index replaced + `cache.Purge()`) (vfs.go:2255-2276, 2564-2593). So a read transaction always sees the index/cache state as of its first SHARED lock — SQLite-consistent, verified by `TestVFSFile_PendingIndexIsolation` (vfs_test.go:66-114). Time travel (`targetTime != nil`) suspends applying polls entirely (vfs.go:2480-2482, 2557-2561).

## 5. Locking

In-process only; **no cross-process/multi-device coordination**. `f.lockType` guarded by `f.mu`. `Lock()` (vfs.go:2182-2229): rejects downgrades; RESERVED+ while `writeEnabled == false` → `ReadOnlyError`; RESERVED+ acquires a per-VFS single-writer slot (`vfs.writeFile`), second writer connection gets `BusyError`; entering RESERVED sets `inTransaction` and fast-forwards `expectedTXID` to `vfs.lastSyncedTXID` (cross-connection pool consistency). `Unlock()` only accepts SHARED/NONE, clears `inTransaction`, broadcasts `cond`, applies pending index. State machine contract in `TestVFSFile_LockStateMachine` (vfs_test.go:24-64). xOpen always reports ReadWrite (unless caller asked read-only) so runtime `PRAGMA litestream_write_enabled` cold-enable works; enforcement is at WriteAt/Truncate/Lock (vfs.go:204-217).

## 6. Write support status (experimental — docs/VFS.md §Write Mode)

- `WriteAt` → copy-on-write page into local buffer file (`dirty` map pgno→buffer offset; buffer discarded on every open, vfs.go:2085-2112 — crash loses unsynced writes). `Sync` skipped during `inTransaction`; periodic `syncLoop` every `WriteSyncInterval` (default `DefaultSyncInterval = 1s`, replica.go:24).
- `syncToRemoteWithLock` (vfs.go:1900-1967): `checkForConflict` = LIST L0 from `expectedTXID`; any `MaxTXID > expectedTXID` → `ErrConflict` (vfs.go:40, 1971-2000). Then streams one L0 LTX with `MinTXID = MaxTXID = pendingTXID`, `Flags = HeaderFlagNoChecksum` (checksum chain **not** maintained), sorted pgnos, lock page skipped (vfs.go:2006-2083). Optimistic concurrency with a check-then-PUT race window; no lease. Can bootstrap a brand-new DB (zero-filled reads for missing pages, vfs.go:1482-1494). VFS-side compaction/snapshot monitors can run on the same defaults (`DefaultCompactionLevels`: L1 30s, L2 5m, L3 1h; vfs.go:2860-3081).

## 7. Hydration (the materialized-replica layer already inside the VFS)

`Hydrator` (vfs.go:567-951): background restore of the full DB to a local file, then reads switch to local.
- Restore: open all plan files, `ltx.NewCompactor(pw, rdrs)` + `DecodeDatabaseTo(file)` (vfs.go:676-723). CatchUp applies newer L0 files whole (vfs.go:726-790). After completion, each poll's changed pages are ranged-fetched and written into the file (`ApplyUpdates` vfs.go:811-829); writer syncs also patch it (vfs.go:1348-1364).
- **Persistence**: with `HydrationPath` set, file + sidecar `{path}.meta` (decimal TXID + `\n`, atomic temp→fsync→rename→dir-fsync, vfs.go:880-951) survive restart; on reopen, resume if remote TXID ≥ meta TXID, else truncate and re-restore (vfs.go:1302-1346). PRAGMAs: `litestream_hydration_progress` / `_file` (vfs.go:2387-2404).
- **Caveat**: even with persistent hydration, `Open()` still requires network — `waitForRestorePlan` must LIST and `buildIndex` must fetch page indexes before hydration is even consulted (vfs.go:1020-1084). Hydration removes read latency, not the network dependency at open.

## 8. What survives restart / failure modes

| State | Survives restart? |
|---|---|
| Page index (in-memory map) | No — rebuilt from bucket every open |
| LRU page cache | No |
| Write buffer file | File may exist but is truncated on open (vfs.go:2086-2111) |
| Hydration file + `.meta` | Yes, iff `HydrationPath` configured |

Network loss: polls fail (logged, position frozen → stale-but-consistent reads, `litestream_lag` grows); uncached page miss → 6 retries → `SQLITE_BUSY`; cold open in read-only mode blocks indefinitely. Object deleted under the index (L0 retention: `DefaultL0Retention = 5min`, check every 15s, store.go:68-72) → `os.ErrNotExist` treated as retryable, then BUSY; heals when poll merges L1 entries.

## 9. Assessment for liters on iOS/Android

**VFS demand paging — costs on mobile**
- *Startup*: network-mandatory at every open; ≥5 LISTs + multi-MB index download for large DBs before the first row is returned; each uncached b-tree descent is a chain of dependent range GETs (3-5 × 50-300 ms mobile RTT per query).
- *Offline*: zero offline capability (open blocks; misses → BUSY). Disqualifying for most mobile apps.
- *Memory*: full pgno map (~25-100 MB/GB of DB) + LRU; hostile to iOS jetsam limits.
- *Battery/network*: 1 s polling keeps the radio warm; per-page GETs have terrible request:byte overhead. Poll cadence would need to become push/trigger-driven anyway (your constraint: readers poll or are triggered out-of-band).
- *Rust complexity*: a full VFS is buildable (`sqlite3_vfs_register` via rusqlite's bundled SQLite — required on iOS, where the system SQLite can't load extensions; crates: `sqlite-vfs`, or hand-rolled FFI), but you must replicate every subtlety: page-1 change-counter spoofing, lock state machine + pending-index swap on unlock, journal/temp file emulation, BUSY semantics, lock-page skipping. High-risk surface; the Go version is itself labeled experimental.

**Materialized local replica file — fit**
- Instant open after first sync; fully offline; plain SQLite (WAL locally if desired) for readers; radio used only on explicit pull triggers (matches "no bucket notifications; reader is triggered out-of-band"). This is precisely what Litestream's own Hydrator half-builds, minus the VFS wrapper.

**Recommendation: build the materialized-replica reader first.** Concretely, reuse from this code:
1. `CalcRestorePlan`'s snapshot+level merge-cursor algorithm and contiguity invariants (replica.go:1441-1557) for initial restore.
2. Incremental catch-up = the poll algorithm without the VFS: from stored `applied_txid`, list L0 (seek txid+1) and L1, enforce `MinTXID == applied+1`, tolerate L0 gaps by deferring to L1, detect commit shrink → truncate file; fetch **page indexes + ranged page GETs** (cheap for small deltas) or whole LTX files (cheap for compacted L1), apply pages at `(pgno-1)*pageSize`, then atomically persist `applied_txid` (Hydrator meta pattern, vfs.go:880-951).
3. Apply under an EXCLUSIVE SQLite lock on the local file and bump the real change counter (offsets 24-27 *and* 92-95) instead of the VFS's randomize-on-read hack, so concurrently-open reader connections invalidate correctly.
4. Keep the LTX parsing layer (header@100B, per-page LZ4 frames, varint tail index, trailer) as a shared crate — it is byte-compatible with both read and write paths, and the write path (your explicit push) is exactly `createLTXFromDirty`'s shape: single-TXID L0 file, sorted pgnos, skip lock page — but liters should maintain the checksum chain (don't copy `HeaderFlagNoChecksum`) since Litestream v0.5 verifies positions via `PreApplyChecksum`/`PostApplyChecksum`.

**Keep VFS demand paging as a later optional layer** for the "huge DB, need one query now" case — its primitives (page index fetch, ranged `FetchPage`, pending-index snapshot isolation) are a strict superset of what the materialized path already needs, so nothing is thrown away. If built later, copy: pending/index double-map swap keyed on SHARED lock, commit-shrink replace+purge, L0-gap deferral, BUSY-on-missing-object retry policy, and page-1 spoofing; skip: write mode (liters' writers own real local DBs), time travel, VFS-side compaction.
