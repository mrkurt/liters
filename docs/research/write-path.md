# Litestream v0.5.x Writer-Side Replication Pipeline — Full Analysis for liters

All references are to `/Users/kurt/code/liters/reference/litestream/` (litestream @ v0.5.x-era main) and `~/go/pkg/mod/github.com/superfly/ltx@v0.5.1/` (the exact LTX version pinned in `go.mod:30`).

---

## 1. Constants and defaults

| Constant | Value | Location |
|---|---|---|
| `DefaultMonitorInterval` | 1s (DB sync poll) | db.go:31 |
| `DefaultCheckpointInterval` | 1m (time-based PASSIVE checkpoint) | db.go:32 |
| `DefaultBusyTimeout` | 1s | db.go:33 |
| `DefaultMinCheckpointPageN` | 1000 pages (~4MB @4KB) — PASSIVE threshold | db.go:34 |
| `DefaultTruncatePageN` | 121359 pages (~500MB) — TRUNCATE emergency | db.go:35 |
| `DefaultShutdownSyncTimeout` / `Interval` | 30s / 500ms | db.go:36-37 |
| `DefaultSyncBackoffMax` | 5m | db.go:41 |
| `DefaultSyncInterval` (replica upload poll) | 1s | replica.go:24 |
| `MetaDirSuffix` | `"-litestream"` | litestream.go:20 |
| `WALHeaderSize` / `WALFrameHeaderSize` | 32 / 24 bytes | litestream.go:123,126 |
| `SnapshotLevel` | 9 | compaction_level.go:9 |
| `DefaultCompactionLevels` | L0:∅, L1:30s, L2:5m, L3:1h | compaction_level.go:14-19 |
| `DefaultSnapshotInterval` | 24h | store.go:60 |
| `DefaultL0Retention` | 5m (post-L1-compaction) | store.go:68 |
| `ltx.HeaderSize` / `PageHeaderSize` / `TrailerSize` | 100 / 6 / 16 | ltx.go:28-31 |
| `ltx.ChecksumFlag` | `1<<63` | ltx.go:55 |
| `ltx.HeaderFlagNoChecksum` | `1<<1` (= 0x2) | ltx.go:175 |
| `PENDING_BYTE` | 0x40000000; lock pgno = `PENDING_BYTE/pageSize + 1` | ltx.go:491-496 |
| RESTART checkpoint mode | **permanently removed** from automatic use (issue #724); only PASSIVE + TRUNCATE remain (RESTART still used internally by `CRC64()` verification at db.go:2767) | db.go:60-63 |

---

## 2. Initialization against a live DB (`DB.init`, db.go:967-1081)

Lazy — runs on first `Sync()` via `newSyncExecutor` (db.go:1739-1758), not on `Open()`. `Open()` (db.go:719-762) only validates config, removes stale `*.tmp` under the meta dir (`removeTmpFiles`, litestream.go:169-182), and starts the monitor goroutine.

Exact init sequence:

1. `os.Stat(path)` — if DB file absent, return nil (retry next sync). Cache `fileInfo` and parent `dirInfo` for permission propagation (db.go:974-986).
2. Open SQLite with DSN: `file:<path>?_pragma=busy_timeout(1000)&_pragma=wal_autocheckpoint(0)` (db.go:988-991). **`wal_autocheckpoint(0)` disables auto-checkpoint on litestream's own connection only** (it's per-connection).
3. `SQLITE_FCNTL_PERSIST_WAL = 1` via file control (db.go:943-963, called at 996) — prevents SQLite deleting the `-wal` file when the last connection closes.
4. Open a **long-running raw `os.File` on the DB** (`db.f`, db.go:1001) — kept open for the process lifetime. Reason: on POSIX non-OFD locks, opening+closing another fd on the same file in-process would release SQLite's locks; all direct DB-page reads go through this one fd via `ReadAt` (see `readWALFileAt` warning, litestream.go:151-152 — that helper is only used for the WAL).
5. `PRAGMA journal_mode = wal;` and **verify the returned mode string == "wal"**, else hard error (db.go:1019-1023).
6. `CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);` — used to force a WAL write on demand (db.go:1027).
7. `CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);` — used to promote a tx to a write lock during checkpoint (db.go:1033).
8. **Acquire the long-running read transaction** (db.go:1039, `acquireReadLock` 1126-1146): `BEGIN` (deferred) + `SELECT COUNT(1) FROM _litestream_seq;` executed inside the tx to actually take the read lock. Held indefinitely; released only around litestream's own checkpoints and on close.
9. `PRAGMA page_size;` → `db.pageSize` (db.go:1044-1048).
10. Create meta dir (`internal.MkdirAll` with uid/gid/mode inherited from the DB's parent dir, db.go:1051).
11. `ensureWALExists` (db.go:1389-1398): if `-wal` missing or `< 32` bytes, execute `INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1` to force a WAL header + at least one frame.
12. `checkDatabaseBehindReplica` (db.go:1406-1483, issue #781): compare local `Pos().TXID` vs remote max L0 TXID. If **local < remote** (DB was restored from an older copy), wipe local L0 dir, download the remote max L0 file (tmp+fsync+rename) to re-establish the position baseline. Next sync's `verify()` will then mismatch and force a **snapshot at TXID = remoteMax+1**, preventing TXID collisions in the bucket.
13. Start replica upload goroutine (db.go:1077).

### Why the long-running read transaction exists
- A WAL reader's read-mark prevents **any** checkpointer (including other processes and the app's own `wal_autocheckpoint`) from checkpointing past litestream's position, and blocks WAL **restart/truncation** entirely. So WAL bytes litestream hasn't copied yet can never be overwritten with new-salt frames between syncs.
- It makes litestream's **direct file reads of DB pages consistent**: pages present in the WAL are read from the WAL (via the page map); pages *not* in the WAL cannot be modified in the DB file by a concurrent checkpoint (a checkpoint only writes pages that are in the WAL, and those are all in the page map). Hence `writeLTXFromDB` can safely `ReadAt` the raw DB file.
- Corollary for liters: while this lock is held, **the app's checkpoints can never truncate the WAL** — so liters *must* perform checkpoints itself or the WAL grows forever. The checkpoint logic is not optional.

---

## 3. Local persisted state (the meta directory)

`metaPath = <dir>/.<dbname>-litestream/` (db.go:289). Contents:

```
.<dbname>-litestream/
└── ltx/
    └── 0/                                   # L0 only, decimal level dir locally
        ├── 0000000000000001-0000000000000001.ltx
        ├── 0000000000000002-0000000000000002.ltx
        └── *.ltx.tmp                         # staging; deleted on Open()
```

**There is no position file, no generation file, no cursor.** The replication position is *derived*: `Pos()` (db.go:559-590) scans `ltx/0/` for the max-TXID filename (`MaxLTX`, db.go:528-545), opens that file, runs a **full `ltx.Decoder.Verify()`** (reads whole file, checks CRC64), and returns `dec.PostApplyPos()` = `{TXID: MaxTXID, PostApplyChecksum: trailer value}`. Cached until invalidated. The newest L0 file is also the input to `verify()` (WAL offset/salt continuity + `lastPageMatch`), so **the minimum required local state is the single most recent L0 file**. Older L0s are kept only for replica upload lag and are pruned by L0 retention (5m after compacted into L1; db.go:2545-2667). Generations do not exist in v0.5.x — they were a v0.3.x concept; continuity is TXID-only.

---

## 4. WAL facts used (byte offsets, wal_reader.go)

WAL header (32 B): magic u32BE @0 (`0x377F0682` = LE checksums, `0x377F0683` = BE); version @4 (must be `3007000`); pageSize @8; checkpoint seq @12; **salt1 @16; salt2 @20**; cksum1 @24; cksum2 @28. Header checksum = `WALChecksum(bo, 0, 0, hdr[:24])` (wal_reader.go:111-114). Frame header (24 B): pgno u32BE @0; **commit** (db size in pages if commit record, else 0) @4; salt1 @8; salt2 @12; cksum1 @16; cksum2 @20. `WALChecksum` (wal_reader.go:273-282): SQLite's rolling `s0 += w[i] + s1; s1 += w[i+1] + s0` over 8-byte units, byte order per magic.

`WALReader.readFrame` (wal_reader.go:137-187) stops (returns `io.EOF`) on: short read, **frame salt ≠ expected salt**, or cumulative-checksum mismatch — this is how the committed end of the WAL is found without any SHM access.

`PageMap` (wal_reader.go:192-244): scan frames; accumulate `txMap[pgno]=frameOffset`; on a commit frame (`fcommit != 0`) flush `txMap` into the real map and set `commit = fcommit` — **uncommitted trailing frames are excluded**. After scan, **delete entries with `pgno > commit`** (VACUUM shrink between transactions in the same WAL). Returns `(map pgno→frame offset of latest committed version, maxOffset = highest used frame offset + frameSize, commit)`.

`NewWALReaderWithOffset` (wal_reader.go:45-75): resume from an offset with known salts; requires `(offset-32) % (24+pageSize) == 0`; reads the *previous* frame with `verifyChecksum=false` to seed the rolling checksum from that frame's stored checksum; salt mismatch there → `PrevFrameMismatchError` → caller resets to offset 32 and (via verify's earlier decision) full-file read.

---

## 5. TXID assignment and LTX creation

- **TXIDs are assigned locally by the writer**: `txID := pos.TXID + 1` (db.go:1815). Every L0 file covers exactly one TXID: `MinTXID = MaxTXID = txID`, filename `%016x-%016x.ltx` (ltx.go:487-489). One L0 file per *sync*, not per SQLite transaction — a sync batches all newly committed WAL frames since the last sync into one LTX "transaction".
- **TXID 1 (`MinTXID==1`) is by definition a snapshot** (`Header.IsSnapshot`, ltx.go:198-200) and must contain every page `1..Commit` in order, skipping the lock page (encoder enforces, encoder.go:226-242).
- **TXIDs are independent of checkpoints**: they keep incrementing monotonically across WAL restarts/checkpoints forever. There is no generation; a forced snapshot is just an L0 whose page set is the whole DB but whose `MinTXID = MaxTXID = txID` (NOT reset to 1) — except the very first file. Snapshot-level (L9) files created by compaction have `MinTXID=1, MaxTXID=pos.TXID` (db.go:2410-2422, 2470).

---

## 6. `verify()` — continuity check before every sync (db.go:1499-1633)

Inputs: current `pos` (from newest L0), `syncState{syncedToWALEnd, lastSyncedWALOffset}`. Output `syncInfo{offset, salt1, salt2, snapshotting, reason, clearSyncedToWALEnd}` where `offset` is where WAL copying resumes. `snapshotting` defaults to **true**; every branch must prove it can be false.

Decision tree:
1. `pos.TXID == 0` → first sync: `offset = 32`, snapshot. (db.go:1503-1506)
2. Open the newest L0 (`ltx/0/<txid>-<txid>.ltx`), decode header → `offset = hdr.WALOffset + hdr.WALSize`, salts = `hdr.WALSalt1/2`. Missing/corrupt → error (LTXError; recoverable by reset).
3. **`offset > wal file size`** (WAL truncated since last sync):
   - if `syncedToWALEnd` (we synced exactly to WAL end last time, so truncation is an *expected* checkpoint — issue #927 fix): read the new WAL header, reset `offset=32`, adopt new salts, `snapshotting=false`, `clearSyncedToWALEnd=true` → continue incrementally from the fresh WAL. (db.go:1529-1552)
   - else: `reason="wal truncated by another process"` → **snapshot**. (db.go:1554-1555)
4. Read live WAL header salts; `saltMatch = (hdr salts == LTX salts)`.
5. `offset == 32` (last LTX ended at WAL header, `WALSize=0` — issue #900 underflow guard): saltMatch → incremental; else snapshot ("wal header salt reset"). (db.go:1572-1580)
6. `prevWALOffset = offset - frameSize`; if it equals 32 (only one frame synced ever): saltMatch → incremental, else snapshot. (db.go:1586-1594)
7. **`lastPageMatch`** (db.go:1636-1672): read the frame at `prevWALOffset` from the real WAL; its frame-header salts must equal the LTX header salts, and its `(pgno, page bytes)` must be found by scanning pages inside the newest L0 file. Not found → `reason="last page does not exist in last ltx file, wal overwritten by another process"` → snapshot.
8. lastPageMatch OK but `!saltMatch` (WAL was restarted under us, e.g. app did a checkpoint): reset `offset=32`, adopt new salts; run `detectFullCheckpoint` (db.go:1676-1704) — scan all frame salts in the WAL until the last-known salt; if any salt other than {new header salt, old LTX salt} exists, a FULL/RESTART checkpoint cycled the WAL more than once and frames were missed → snapshot; otherwise incremental from 32. (db.go:1611-1628)
9. Otherwise: incremental from `offset`.

---

## 7. `sync()` — WAL→LTX, step-by-step (db.go:1807-2061)

Entry: `DB.Sync` (db.go:1165) takes `execMu` (serializes Sync/Checkpoint/CRC64); `syncLocked` → lazy `init()` → `ensureWALExists` → `verifyAndSyncWithExecutor` → `sync()`:

1. `txID = pos.TXID + 1`; target `filename = ltx/0/<txID>-<txID>.ltx`.
2. If not already inside a checkpoint, take `chkMu.RLock()` (blocks litestream's own checkpoint while a sync runs; db.go:1841-1844).
3. `commit = dbFileSize / pageSize` from the long-lived fd stat (db.go:1846-1851).
4. Open the WAL; create `WALReader` at offset 32 (fresh) or `NewWALReaderWithOffset(offset, salt1, salt2)` (resume). `PrevFrameMismatchError` → fall back to offset 32 (db.go:1861-1877).
5. `pageMap, maxOffset, walCommit := rd.PageMap(ctx)`; if `walCommit > 0`, `commit = walCommit` (db.go:1886-1892).
6. `sz = maxOffset - info.offset` (0 if no committed frames). **If `!snapshotting && sz == 0` → no-op return** (no empty LTX files; db.go:1913-1916).
7. Open `<filename>.tmp` (O_RDWR|CREATE|TRUNC, mode copied from DB file; chown to DB uid/gid).
8. `ltx.NewEncoder(tmp)`; `EncodeHeader(ltx.Header{ Version:3, Flags:HeaderFlagNoChecksum, PageSize, Commit:commit, MinTXID:txID, MaxTXID:txID, Timestamp:time.Now().UnixMilli(), WALOffset:info.offset, WALSize:sz, WALSalt1:rd.salt1, WALSalt2:rd.salt2 })` (db.go:1946-1958). Note `PreApplyChecksum` is left 0 (required by the NoChecksum flag).
9. Pages:
   - **Snapshot path** `writeLTXFromDB` (db.go:2063-2108): for `pgno = 1..commit`, skip lock pgno; if pgno ∈ pageMap read the page from the **WAL** at `offset+24`, else read from the **DB file** at `(pgno-1)*pageSize` via the long-lived fd; `EncodePage({Pgno})`.
   - **Incremental path** `writeLTXFromWAL` (db.go:2110-2137): sort pageMap keys ascending (encoder requires ascending pgno), read each latest committed page image from the WAL, `EncodePage`.
10. `enc.Close()` (writes end-of-pages marker, page index, trailer with CRC64), **`ltxFile.Sync()` (fsync), close, `os.Rename(tmp, final)`** (db.go:1993-2020). On rename failure, invalidate pos + L0 caches. (Note: no fsync of the `ltx/0/` directory — an OS crash can lose the rename; recovery relies on `checkDatabaseBehindReplica` + snapshot fallback.)
11. Result bookkeeping: `pos = enc.PostApplyPos()` (= `{txID, 0}` because NoChecksum); `newWALSize/lastSyncedWALOffset = info.offset + sz` (**logical** WAL end — used instead of file size for checkpoint thresholds to avoid issue #997 feedback loops from stale old-salt frames); `syncedToWALEnd = (finalOffset == current WAL file size)` (db.go:2031-2048). State is applied to `db.syncState` only after success (`applySyncExecutor`, db.go:1760-1789 — the executor pattern exists so failed syncs never corrupt persistent state; tests db_internal_test.go:2252,2378).

Then `checkpointIfNeeded` (db.go:1290-1345), priority order:
1. `TruncatePageN > 0 && origWALSize >= calcWALSize(pageSize, TruncatePageN)` → TRUNCATE (blocking).
2. `newWALSize >= calcWALSize(pageSize, MinCheckpointPageN)` → PASSIVE; SQLITE_BUSY is swallowed.
3. `CheckpointInterval > 0 && syncedSinceCheckpoint && dbFileMtime older than interval && newWALSize > calcWALSize(pageSize,1)` → PASSIVE. `syncedSinceCheckpoint` gate prevents infinite checkpoint→seq-write→checkpoint loops on idle DBs (issue #896; test db_internal_test.go:1232).

`calcWALSize(pageSize, n) = 32 + (24+pageSize)*n` (db.go:1384-1386).

---

## 8. LTX v0.5.1 file format (exact layout)

**Header — 100 bytes** (ltx.go:283-299; bytes 80–99 zero padding):

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | Magic `"LTX1"` |
| 4 | 4 | Flags u32BE (litestream sets `0x00000002` = NoChecksum) |
| 8 | 4 | PageSize |
| 12 | 4 | Commit (db size in pages after apply; 0 = deletion) |
| 16 | 8 | MinTXID |
| 24 | 8 | MaxTXID |
| 32 | 8 | Timestamp (Unix ms) |
| 40 | 8 | PreApplyChecksum (0 under NoChecksum / snapshots) |
| 48 | 8 | WALOffset (i64; source WAL offset copying started at) |
| 56 | 8 | WALSize (i64; bytes of source WAL covered) |
| 64 | 4 | WALSalt1 |
| 68 | 4 | WALSalt2 |
| 72 | 8 | NodeID (0) |
| 80 | 20 | zero padding |

**Page block**: repeated `[6-byte PageHeader: pgno u32BE @0, flags u16BE @4 (must be 0)] + [lz4-frame-compressed page data]` — each page is its own lz4 *frame* (block size 64KB, fast level; encoder.go:43-48), decoder does `zr.Reset(r)` per page and expects frame EOF (decoder.go:176-185). Pages strictly ascending by pgno; snapshots must cover `1..Commit` sequentially skipping the lock page; lock page may never be encoded (encoder.go:219-242). Terminated by a **zero PageHeader** (6 zero bytes).

**Page index** (encoder.go:137-174): per page in ascending pgno order: `uvarint(pgno) uvarint(fileOffset) uvarint(frameSize)` where offset/size describe the page-header+compressed-data span; then `uvarint(0)` end marker; then **u64BE index size** (bytes of elements+marker, excluding this size field).

**Trailer — 16 bytes**: `PostApplyChecksum u64BE @0` (0 under NoChecksum), `FileChecksum u64BE @8` = `ChecksumFlag | crc64-ISO` over: header bytes + page-header bytes + **uncompressed** page data + end marker + page index bytes + trailer[0:8] (encoder.go:270-307).

### Post-apply checksum: the real answer
**Litestream v0.5.x does NOT maintain the incremental post-apply database checksum.** Every LTX it writes carries `HeaderFlagNoChecksum` (db.go:1948, 2412) and never calls `enc.SetPostApplyChecksum`. Consequently `Pos.PostApplyChecksum` is always 0 and replication continuity is enforced **by TXID contiguity alone** plus the WAL offset/salt/lastPageMatch checks. The checksum machinery (`ltx.ChecksumPage` = `ChecksumFlag | crc64ISO(pgno_be ‖ page)`; rolling db checksum = `ChecksumFlag | XOR of all page checksums`, checksum.go:106-132) exists and the decoder verifies it *only when the flag is absent*. liters must set the same flag for wire compatibility and may skip checksum maintenance exactly as litestream does. `ltx.NewCompactor` used for restore is likewise run with `HeaderFlags = HeaderFlagNoChecksum` (replica.go:682).

---

## 9. Checkpointing (db.go:2140-2335)

`checkpointWithExecutor(mode)`:
1. `chkMu.TryLock()` — silently skip if a snapshot/sync holds it.
2. Read WAL header bytes (pre-image).
3. **Pre-copy**: full `verifyAndSync` (checkpointing=true) — capture every committed frame up to the WAL end into an L0 before the checkpoint destroys/recycles it (db.go:2216-2220).
4. `execCheckpoint` (db.go:2291-2335): **release the read lock** (rollback rtx), `PRAGMA wal_checkpoint(<MODE>);` (scan 3-int result), **reacquire the read lock immediately** (also deferred re-acquire on early return).
5. `INSERT ... INTO _litestream_seq ...` — forces a write so a restarted WAL immediately has a valid new header + ≥1 frame (db.go:2231).
6. Re-read WAL header. If unchanged (PASSIVE that didn't restart): `syncedSinceCheckpoint=false`, done.
7. If restarted: `BEGIN` + `INSERT INTO _litestream_lock (id) VALUES (1)` to grab the **write lock** (can't `BEGIN IMMEDIATE` through database/sql here), run a **post-copy** `verifyAndSync` under that write lock (no writer can commit concurrently), then `ROLLBACK` (db.go:2254-2285). The post-copy's `verify()` hits the "expected truncation" path (`syncedToWALEnd` true from the pre-copy) → resumes from offset 32 with new salts, *without* snapshotting (issue #927; tests db_internal_test.go:931, 1043).
8. `syncedSinceCheckpoint = false`.

Interaction with app writers: PASSIVE never blocks anyone and silently under-checkpoints on contention. TRUNCATE blocks new writers and waits for readers; there is a residual TOCTOU (frames committed between the pre-copy's WAL-size stat and the truncation) that litestream closes probabilistically via `syncedToWALEnd` — if any frame landed after the pre-copy's LTX end, `syncedToWALEnd=false` and the post-checkpoint `verify()` takes the "wal truncated by another process" branch → **full snapshot**, so no pages are ever lost, at worst an extra snapshot (test db_internal_test.go:1850 `TestDB_CheckpointPageGapWithConcurrentWrites`). `Checkpoint()` also uses `execMu`, so it never races `Sync()`.

---

## 10. Replica upload — "LTX object durable in bucket" (replica.go:134-207)

`Replica.Sync`:
1. If in-memory replica pos is zero (startup or after any error), `calcPos`: list remote L0 files, take max `MaxTXID` (replica.go:210-235).
2. Get local `db.Pos()`; if zero → `errReplicaWaitForData`.
3. `for txID := replicaPos+1; txID <= dbPos; txID++`: open local `ltx/0/<txID>-<txID>.ltx`, `Client.WriteLTXFile(ctx, 0, txID, txID, f)`, advance replica pos. Missing local file → `LTXError` (gap ⇒ unrecoverable without reset/snapshot). **On any error the replica pos is zeroed** so the next attempt re-derives it from the bucket — uploads are idempotent PUTs to a deterministic key, so double-upload is harmless.

**Bucket layout (S3 client — the wire format for liters):** key = `<configPath>/<level as %04x>/<minTXID %016x>-<maxTXID %016x>.ltx` (s3/replica_client.go:655,703,1070; list prefix `<path>/%04x/`, s3/replica_client.go:1393). L0 → `0000/`, L1 → `0001/`, snapshot level → `0009/`. Original LTX header timestamp is stored as S3 object metadata `litestream-timestamp` (RFC3339Nano) (s3/replica_client.go:55,706-708). Note the **file** replica client uses a different local layout (`<root>/ltx/<decimal-level>/…`, litestream.go:190-197) — do not copy that for object storage.

Durability chain: WAL bytes (already durable via SQLite) → tmp LTX + fsync + rename (local crash safety) → S3 PUT (multipart uploader; ETag checked). There is no fsync-before-upload dependency: an upload only happens from a fully-renamed local file. If the process crashes after rename but before upload, the next `Replica.Sync` re-derives remote pos and uploads the backlog. If local L0s were lost (no dir fsync), local pos regresses; `checkDatabaseBehindReplica` detects local<remote and rebaselines, and `verify()` forces a snapshot — correctness preserved, efficiency sacrificed.

Snapshots (L9): `Store.CompactDB` at `SnapshotInterval` (24h) calls `db.Snapshot` → `SnapshotReader` (db.go:2338-2440) which streams an `ltx.Header{MinTXID:1, MaxTXID:pos.TXID, WALOffset/Size/salts from a fresh WAL scan}` + `writeLTXFromDB` through a pipe directly to `WriteLTXFile(SnapshotLevel, 1, pos.TXID, r)` — no local staging. Triggered only by the store scheduler, never by the DB sync path.

---

## 11. Situations that force a full-snapshot L0

1. First sync ever (`pos.TXID==0`) — including first run against an existing populated DB (snapshot = full DB image as TXID 1).
2. WAL truncated when `syncedToWALEnd == false` ("truncated by another process").
3. WAL salt changed AND (`lastPageMatch` fails OR `detectFullCheckpoint` finds >1 unknown salt).
4. Salt reset while `offset==32`/single-frame edge cases (db.go:1572-1594).
5. Newest local L0 missing/corrupt after `ResetLocalState`/auto-recovery (replica.go:420-430).
6. DB restored to older state (`checkDatabaseBehindReplica` rebaseline → verify mismatch).

Not snapshot triggers: litestream's own checkpoints (any mode), idle time, replica errors.

---

## 12. Multi-process / crash safety summary

- Read-lock tx pins the WAL against restart by any process; PASSIVE checkpoints by the app can still copy frames (harmless — salts unchanged, appends continue).
- `PERSIST_WAL` + long-lived fds guard against WAL deletion and POSIX-lock self-release.
- External interference (another litestream, manual `wal_checkpoint`, VACUUM, WAL replacement) is *detected*, not prevented, by verify()'s salt/offset/lastPageMatch checks → snapshot.
- Two writers replicating the same DB to the same bucket path is **not safe** (both assign TXIDs locally); litestream assumes a single replicating process per bucket prefix (there is a `Leaser` abstraction in leaser.go for this, optional).
- Startup: delete `**/*.tmp` in meta dir; positions re-derived from files; remote pos re-derived from listing.
- `Close()` (db.go:767-819): final `syncLocked` + replica sync with retry (30s), release read lock, close handles.

Key test-encoded invariants (db_internal_test.go): checkpoint (PASSIVE or TRUNCATE) must not cause `verify().snapshotting` (931); repeated checkpoint+write cycles must not snapshot (1043); idle DB + CheckpointInterval must not generate L0 churn (1232, issue #994 at 1316); union of pages across L0 files must cover `1..maxCommit` minus lock page even with writes racing a TRUNCATE checkpoint (1850); every new WAL page must appear in the L0 chain incl. DB-growth pages (1450, 1518); failed syncs must not mutate `syncState`/pos cache (2252, 2378, 2450).

---

## 13. Distilled minimal writer algorithm for liters (explicit `push()`, no watcher)

### One-time setup — `Writer::open(db_path, client, prefix)`
1. Delete `<meta>/**/*.tmp`. Meta dir: `.<dbname>-litestream/ltx/0/` (keep the litestream name for drop-in interop with litestream tooling on the same DB).
2. Open a dedicated SQLite connection: `busy_timeout=1000`, `PRAGMA wal_autocheckpoint=0`, file-control `PERSIST_WAL=1`.
3. `PRAGMA journal_mode=wal` and assert result == `"wal"` (hard error otherwise).
4. Create `_litestream_seq` and `_litestream_lock` tables (keep the names — a litestream restore of our bucket will contain them; matching schema avoids divergence).
5. Open one long-lived read-only fd on the DB file (all raw page reads go through it).
6. Acquire the long-running read tx: `BEGIN; SELECT COUNT(1) FROM _litestream_seq;` — hold between pushes.
7. `PRAGMA page_size` → cache.
8. `ensure_wal_exists`: if `-wal` < 32 bytes, upsert `_litestream_seq`.
9. `check_behind_remote`: list bucket `prefix/0000/`, parse max `MaxTXID`; if local pos < remote max, wipe local `ltx/0/`, download remote max L0 (tmp+fsync+rename).
10. Derive `pos` from newest local L0 (full decode+CRC verify). In-memory `sync_state = {synced_to_wal_end: false, last_synced_wal_offset: 0, synced_since_checkpoint: false}`.

### `push()` — the whole hot path
```
1. wal_to_ltx():                                    # == db.Sync
   a. ensure_wal_exists()
   b. info = verify()                               # §6 decision tree, verbatim
   c. sync(info)                                    # §7: PageMap → encode L0 tmp → fsync → rename
      - skip if !snapshotting && sz==0  → no new L0
   d. update sync_state only on success
   e. checkpoint_if_needed():                       # thresholds §7; REQUIRED because our
      passive/truncate per calcWALSize thresholds   # read-lock blocks app checkpoints
2. upload():                                        # == Replica.Sync
   a. if remote_pos unknown: remote_pos = max TXID listed under prefix/0000/
   b. for tx in remote_pos+1 ..= local_pos: PUT prefix/0000/{tx:016x}-{tx:016x}.ltx
   c. on any error: remote_pos = None (re-derive next push)
3. prune local L0 files older than the newest       # keep >=1 (newest needed by verify);
   (optionally keep a small tail for retry safety)  # skip litestream's 5m/L1 gating since
                                                    # we are not running a local VFS reader
```
Checkpoint sub-protocol (when thresholds hit): pre-copy sync → rollback read tx → `PRAGMA wal_checkpoint(PASSIVE|TRUNCATE)` → re-`BEGIN`+read → `_litestream_seq` upsert → if WAL header changed: `BEGIN; INSERT INTO _litestream_lock VALUES(1);` → post-copy sync → `ROLLBACK` → `synced_since_checkpoint=false`. Since the app owns the writer, liters can offer `push(PushOptions{checkpoint: Auto|Force|Never})`; a `Force` while the app guarantees no concurrent writes eliminates litestream's TOCTOU entirely.

### Load-bearing (must keep, byte/behavior-exact)
1. `journal_mode=WAL` assertion; `wal_autocheckpoint(0)` on our connection; `PERSIST_WAL`.
2. Long-running read transaction held **between** pushes (else any app/auto checkpoint between pushes restarts the WAL and every push degenerates into the snapshot/`detectFullCheckpoint` path).
3. The complete `verify()` decision tree, including `syncedToWALEnd` expected-truncation, the `offset==32`/single-frame edge cases (issues #900/#927), `lastPageMatch`, and `detectFullCheckpoint`.
4. `PageMap` semantics: committed-frames-only (commit field), latest-version-per-page, prune `pgno > commit`, salt+rolling-checksum-bounded scan.
5. TXID rules: `pos+1`, `Min==Max` per L0, first file `MinTXID=1` full snapshot, forced snapshots keep the running TXID; strict remote contiguity (never skip; never reuse).
6. LTX byte format from §8, including `Flags=0x2` (NoChecksum), `PreApply=PostApply=0`, per-page lz4 frames (64KB block, fast), ascending pgnos, lock-page skip, page index varints, CRC64-ISO file checksum over uncompressed content, `Timestamp=now_ms`, `WALOffset/WALSize/WALSalt1/2` populated exactly (readers use them for continuity on the writer side; restore tooling validates header).
7. `Commit` = max(dbFileSize/pageSize, last WAL commit field).
8. tmp + fsync + rename for local L0; tmp cleanup on open; pos/caches invalidated on rename failure. (Improvement over litestream: also fsync the `ltx/0/` dir.)
9. Bucket key format `prefix/%04x/%016x-%016x.ltx`; upload strictly ascending; re-derive remote pos from listing after any upload error; idempotent PUT.
10. Checkpointing by liters itself (thresholds may be tuned, but *some* TRUNCATE-capable path must exist because our read lock starves all other checkpointers), incl. the pre-copy / seq-write / post-copy-under-write-lock protocol and the `syncedSinceCheckpoint` gate.
11. `check_behind_remote` rebaseline at open (prevents bucket TXID collisions after device restore/copy).
12. Read-lock release/reacquire strictly bracketing the `wal_checkpoint` PRAGMA.
13. Executor discipline: mutate `sync_state`/pos only after the full step succeeds.

### Droppable daemon conveniences
- `monitor()` goroutines, 1s tickers, notify channel, exponential backoff, rate-limited logging (db.go:2679-2745, replica.go:328-452) — replaced by explicit `push()`.
- Shutdown sync retry loop (db.go:826-939) — app calls `push()` before background/terminate.
- Heartbeats, Prometheus metrics, `SyncDiagnostic` phase tracking, slog plumbing.
- Compaction (L1/L2/L3), snapshot-level generation, retention enforcement, `VerifyCompaction` — these run in litestream's `Store`, not the writer. Optional for liters v1, but note: without them the bucket accumulates one L0 object per push forever and cold restores must replay the whole chain; a litestream server pointed at the same bucket can do compaction for us and remains fully compatible.
- Auto-recovery / `ResetLocalState` (expose as an explicit `reset()` instead).
- uid/gid/mode propagation (`internal.MkdirAll`, chown) — iOS/Android irrelevant.
- Time-based checkpoint (`CheckpointInterval`) — app-driven pushes make it redundant; keep size thresholds.
- `EnsureExists` restore-on-open, follow mode, v0.3.x compat, `CRC64` verification helper, leaser.
