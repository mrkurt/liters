# SQLite File/WAL Format Spec for liters (writer + reader sides)

All references are to files under `/Users/kurt/code/liters/reference/litestream/` unless noted. `ltx` refers to `github.com/superfly/ltx@v0.5.1` (source at `/Users/kurt/go/pkg/mod/github.com/superfly/ltx@v0.5.1/`).

## 1. WAL file layout

```
offset 0    WAL header        32 bytes         (WALHeaderSize = 32, litestream.go:123)
offset 32   frame[0] header   24 bytes         (WALFrameHeaderSize = 24, litestream.go:126)
offset 56   frame[0] data     pageSize bytes
...
frame[i] starts at: 32 + i * (24 + pageSize)
```
`calcWALSize(pageSize, pageN) = 32 + (24 + pageSize) * pageN` (db.go:1384-1386).

### 1.1 WAL header (32 bytes, all fields big-endian on disk)

| bytes | field | notes |
|---|---|---|
| 0-3 | magic | `0x377F0682` → checksum words read **little-endian**; `0x377F0683` → **big-endian** (wal_reader.go:100-107). SQLite writes the variant matching the writer machine's native byte order; a reader must support both. |
| 4-7 | file-format version | must equal `3007000`; otherwise error `unsupported wal version` (wal_reader.go:118-120) |
| 8-11 | page size | uint32 BE (wal_reader.go:122) |
| 12-15 | checkpoint sequence number | increments per checkpoint (wal_reader.go:123, field `seq`; not otherwise used by litestream) |
| 16-19 | salt-1 | incremented by 1 on each WAL restart (test data confirms: successive generations 0x1b9a2949 → 0x1b9a294a → 0x1b9a294b, wal_reader_test.go:268-275) |
| 20-23 | salt-2 | random per restart |
| 24-27 | checksum-1 | over header bytes 0..24, seed (0,0) |
| 28-31 | checksum-2 | |

Header validation order (wal_reader.go:90-129): (a) short read < 32 bytes → treat as empty WAL (`io.EOF`); (b) magic selects checksum byte order, invalid magic → hard error; (c) `WALChecksum(bo, 0, 0, hdr[0..24])` must equal stored bytes 24-31 — mismatch means a torn header write during checkpoint restart → treat WAL as empty (`io.EOF`), **not** an error; (d) version check.

### 1.2 WAL frame header (24 bytes, all BE on disk)

| bytes | field |
|---|---|
| 0-3 | page number (`pgno`, 1-based) |
| 4-7 | `commit`: DB size in pages **after** commit for a commit frame; `0` for non-commit frames |
| 8-11 | salt-1 (must equal WAL-header salt-1 of the current generation) |
| 12-15 | salt-2 (ditto) |
| 16-19 | cumulative checksum-1 |
| 20-23 | cumulative checksum-2 |

Frame acceptance (wal_reader.go:137-187), in order: read 24-byte header (short → EOF); read pageSize data (short → EOF); salts must match reader's expected salts else EOF (this is what terminates iteration at stale frames left over from before a WAL restart); then checksum verification.

## 2. WAL checksum algorithm (exact)

```rust
// bo = byte order chosen by header magic (0x...82 = LE, 0x...83 = BE)
// input length must be a multiple of 8
fn wal_checksum(bo: ByteOrder, mut s0: u32, mut s1: u32, b: &[u8]) -> (u32, u32) {
    for chunk in b.chunks_exact(8) {
        s0 = s0.wrapping_add(bo.read_u32(&chunk[0..4]).wrapping_add(s1));
        s1 = s1.wrapping_add(bo.read_u32(&chunk[4..8]).wrapping_add(s0));
    }
    (s0, s1)
}
```
(wal_reader.go:273-282; duplicate `Checksum` at litestream.go:110-119. Go relies on natural u32 wrap-around; Rust must use `wrapping_add`.)

Chaining rules:
- **Header checksum**: seed `(0,0)` over header bytes `[0,24)`; stored at 24/28. Stored checksums are always read/written **big-endian**, regardless of `bo` (wal_reader.go:111-112); only the *data words fed into the sum* use `bo`.
- **Frame checksum**: running value seeded from the header checksum, then for each frame: fold in frame-header bytes `[0,8)` (pgno+commit only — NOT the salts), then the full page data; result must equal frame-header bytes 16/20 (wal_reader.go:172-175). The chain is cumulative across all frames — frame N's checksum depends on every prior frame in the current WAL generation.
- Resuming mid-WAL: you cannot verify frame N without the chain; litestream seeds the chain by reading frame N-1 with `verifyChecksum=false`, which *trusts* the stored checksum at frame N-1 bytes 16/20 as the running value (wal_reader.go:69-71, 177-179), after verifying frame N-1's salts match the expected `(salt1,salt2)` recorded in the last LTX header. Salt mismatch → `PrevFrameMismatchError` → caller falls back to full-WAL read / snapshot (db.go:1866-1876).

## 3. Committed-transaction boundary detection (`PageMap`, wal_reader.go:189-244)

Algorithm (the core writer-side loop):
1. Iterate frames from current position until EOF (EOF = short read, salt mismatch, or checksum mismatch — all three simply terminate).
2. Accumulate `txMap[pgno] = frame_start_offset` for each frame (`Offset()` returns the *start* offset of the last-read frame: `32 + (frameN-1)*(24+pageSize)`, wal_reader.go:82-87).
3. When a frame has `commit != 0`: merge `txMap` into the result map `m` and record `commit` (db size in pages). `txMap` is *not* cleared (harmless: subsequent tx re-records pages; later offsets overwrite).
4. Frames after the last commit frame are an uncommitted (or torn) transaction tail — never merged.
5. Post-pass: delete `m[pgno]` for `pgno > commit` — handles DB shrink (VACUUM) between transactions inside the same WAL segment (wal_reader.go:218-224).
6. `maxOffset` = max frame-start offset in `m` + `24 + pageSize` (end of last committed frame); this becomes the exclusive end of the consumed WAL segment.

Safety invariant: a torn concurrent append fails the cumulative checksum → clean EOF; a frame from a previous generation fails the salt check → clean EOF; therefore reading the `-wal` file with plain `pread` (no locks on the WAL file itself) is safe.

## 4. Salt rotation & external-checkpoint detection

Salt semantics: SQLite rewrites the WAL header on **WAL restart** (first write after RESTART/TRUNCATE checkpoint, or after a full checkpoint when the writer wraps to the top): salt-1 += 1, salt-2 = random. Old frames beyond the new write head remain physically present with old salts.

Litestream persists `(WALSalt1, WALSalt2, WALOffset, WALSize)` in every L0 LTX header (ltx.Header bytes 64/68 for salts, 48/56 for offset/size; ltx.go:289-296) and on each sync runs `verify()` (db.go:1499-1633):

1. `pos.TXID == 0` → first sync → snapshot from offset 32 (db.go:1503-1506).
2. Resume offset = `prevLTX.WALOffset + prevLTX.WALSize` (db.go:1522).
3. If resume offset > current WAL file size: WAL was truncated. If the previous sync ended exactly at WAL EOF (`syncedToWALEnd`), it's an expected checkpoint → reset to offset 32 with the *new* header salts, no snapshot (db.go:1526-1552, issue #927). Otherwise → snapshot ("wal truncated by another process").
4. Compare current WAL-header salts to LTX salts (`saltMatch`).
5. Edge cases: resume offset == 32 (db.go:1572-1580) or prev-frame offset == 32 (db.go:1586-1596): saltMatch → continue, else snapshot.
6. `lastPageMatch` (db.go:1635-1672): re-read the last previously-synced frame (`offset - (24+pageSize)`); its frame salts (bytes 8/12) must equal the LTX salts, and its `(pgno, page data)` must byte-equal a page in the previous LTX file. Failure → snapshot ("wal overwritten by another process").
7. If lastPageMatch but !saltMatch, the WAL restarted after our last read position → resume from offset 32 with the new salts; but first run `detectFullCheckpoint` (db.go:1674-1704): `FrameSaltsUntil` (wal_reader.go:246-270) scans frame headers from the top of the WAL collecting distinct `(salt1,salt2)` pairs until it sees the last-known (LTX) salt; subtract {current header salt, LTX salt}; if ≥1 *unknown* salt generation remains, ≥2 restarts happened unseen (frames were missed) → snapshot.

## 5. How litestream reads safely while the app writes (locks)

- Opens its own connection with `_pragma=busy_timeout(N)&_pragma=wal_autocheckpoint(0)` (db.go:988-989) and forces `PRAGMA journal_mode=wal` (db.go:1019).
- Sets file-control `SQLITE_FCNTL_PERSIST_WAL=1` so the `-wal` file isn't deleted when the last connection closes (db.go:941-963).
- Creates `_litestream_seq(id INTEGER PRIMARY KEY, seq INTEGER)` and `_litestream_lock(id INTEGER)` tables (db.go:1027-1035).
- **Long-running read transaction** (`BEGIN` + `SELECT COUNT(1) FROM _litestream_seq`, db.go:1126-1146) held permanently: pins a WAL read-mark so no checkpointer (in any process) can restart/overwrite WAL frames litestream hasn't consumed. Released only around litestream's own `PRAGMA wal_checkpoint(...)` and immediately re-acquired (db.go:2308-2332).
- `ensureWALExists`: if `-wal` < 32 bytes, do `INSERT INTO _litestream_seq ... ON CONFLICT UPDATE seq=seq+1` to force a WAL header into existence (db.go:1389-1398).
- The WAL file itself is read lock-free via `O_RDONLY` + `pread` (wal_reader.go; `readWALFileAt`, litestream.go:152-166 — note comment: never use this pattern on the **database** file, it breaks non-OFD POSIX locks; litestream keeps one long-lived `db.f` read fd for the main db, db.go:1000-1003).
- During snapshot the `commit` fallback is `dbFileSize / pageSize` (db.go:1846-1851), overridden by the last WAL commit-frame value when present (db.go:1890-1892). Snapshot page source: for each pgno 1..commit (skipping lock page), prefer the WAL `pageMap` offset (`pread(walFile, offset+24, pageSize)`), else read the main db file at `(pgno-1)*pageSize` (db.go:2063-2108). Incremental sync reads only `pageMap` pages, sorted ascending by pgno because the LTX encoder requires sorted pages (db.go:2110-2137).
- Around its own checkpoint, to capture a quiescent boundary, litestream grabs the SQLite **write lock** by `BEGIN` + `INSERT INTO _litestream_lock (id) VALUES (1)` (never committed, always rolled back — it can't issue `BEGIN IMMEDIATE` inside the driver tx), syncs the post-checkpoint WAL tail while holding it, then rolls back (db.go:2248-2285). Checkpoint restart detection = compare 32-byte WAL header bytes before/after (db.go:2205, 2241-2246).

For liters' writer (app owns the DB, explicit pushes): the same primitives apply — hold a read tx during frame reads if checkpoints can happen concurrently, or simply require the app to not checkpoint during a push. The salt/checksum verification chain plus `(WALOffset, WALSize, WALSalt1, WALSalt2)` persisted per LTX gives crash-safe resumability.

## 6. Database (main file) header fields that matter

100-byte header inside page 1. Fields litestream actually touches:

| offset | size | field | usage |
|---|---|---|---|
| 0 | 16 | magic `"SQLite format 3\0"` | |
| 16 | 2 | page size, BE; value `1` means 65536 | read directly from file at replica.go:747-755 (`pageSize = u16BE; if ==1 {65536}`); writer side uses `PRAGMA page_size` instead (db.go:1044) |
| 18 | 1 | file-format **write** version: 1=rollback journal, 2=WAL | reader-side apply rewrites to `0x01` (replica.go:907-908) |
| 19 | 1 | file-format **read** version: 1/2 as above | rewritten to `0x01` (same line) |
| 24 | 4 | file change counter | reader-side apply overwrites bytes [24,28) with random data to invalidate other connections' page caches / cached schema (replica.go:909) |
| 28 | 4 | in-header DB size in pages | trusted by SQLite only when bytes 24-27 == bytes 92-95 and value ≠ 0; randomizing 24-27 (without touching 92) deliberately invalidates it, so SQLite derives size from the physical file — hence the mandatory truncate (below) |
| 32 | 4 | first freelist trunk page | left alone; freelist pages are ordinary pages carried in WAL/LTX |
| 36 | 4 | freelist page count | left alone |
| 92 | 4 | version-valid-for (change-counter value when offset-96 was written) | not modified by litestream |
| 96 | 4 | SQLITE_VERSION_NUMBER of last writer | not modified |

Writer side never parses the DB header beyond what PRAGMAs provide; commit size comes from WAL commit frames or `fileSize/pageSize`.

## 7. Lock/pending-byte page

- `PENDING_BYTE = 0x40000000` (1 GiB) (ltx.go:491).
- `LockPgno(pageSize) = u32(0x40000000 / pageSize) + 1` (ltx.go:493-496). 4K→262145, 8K→131073, 16K→65537, 32K→32769, 64K→16385.
- Relevant only when `commit >= lockPgno` (DB > 1 GiB). SQLite never stores data there and never writes a WAL frame for it.
- Writer: **skip** the lock page when encoding snapshot LTX (db.go:2064-2070). LTX files must never contain it (`ltx` encoder enforces this).
- Reader/materializer: when streaming pages 1..commit to a `.db` file, emit a page of **all zero bytes** at `pgno == lockPgno` (ltx decoder.go:236-249).
- Rollback-mode file-lock byte ranges live in that page: PENDING=0x40000000 (1 byte), RESERVED=0x40000001, SHARED_FIRST=0x40000002, SHARED_SIZE=510 (internal/lock_unix.go:12-14). Litestream's reader-side applier takes SQLite-compatible exclusive locks by `fcntl(F_WRLCK)` on `[0x40000000,1)` then `[0x40000002,510)` (unix, internal/lock_unix.go:17-29; `LockFileEx` on Windows) so concurrent SQLite readers of the replica can't see torn applies.

## 8. Reader side: materializing a valid standalone `.db`

Full restore path (replica.go:544-732 → ltx decoder.go:223-268 `DecodeDatabaseTo`):
1. LTX inputs (snapshot + increments) are compacted into one snapshot stream (must be a snapshot: `MinTXID == 1`).
2. For `pgno` in `1..=hdr.Commit`: write pageSize bytes at file offset `(pgno-1)*pageSize`; lock page → zeros; all other pages must appear in ascending pgno order with no gaps.
3. Final file length is exactly `Commit * pageSize`.
4. No header fixups are performed on this path — page 1 arrives with whatever the source wrote (bytes 18/19 will be 2/2 = WAL mode; change counters as-is). This is valid: SQLite opens it, sees WAL mode, creates fresh empty `-wal`/`-shm`. Restore then runs `PRAGMA quick_check/integrity_check` and deletes leftover `-shm`/`-wal` (replica.go:1258-1293). Write to `X.tmp`, fsync, close, `rename(2)` into place (replica.go:663-701).
5. Critical negative invariant: never leave a **stale** `-wal`/`-shm` next to a freshly materialized db — SQLite would attempt recovery against it.

Incremental live apply onto an existing replica file (follow mode, replica.go:871-930 `applyLTXFile`) — the model for liters' reader when third parties may have the file open:
1. Acquire the SQLite-compatible exclusive byte-range locks (§7) for the duration of the apply.
2. For each LTX page: `pwrite(data, (pgno-1)*pageSize)`.
3. If the decoded page is `pgno == 1` (and ≥28 bytes): set byte 18 = `0x01`, byte 19 = `0x01` (force rollback-journal read/write versions so plain read-only opens need no `-shm` and no directory write access — important on iOS/Android read-only consumers), and fill bytes [24,28) with random bytes (invalidate cached schemas/pages in other connections) (replica.go:907-910).
4. After all pages: `ftruncate(Commit * pageSize)` — handles shrinks and repairs the now-invalidated in-header size (replica.go:918-923).
5. `fsync` (replica.go:929). Progress (last applied TXID) is persisted in a `<db>-txid` sidecar for crash recovery (replica.go:560-597, 724-727).
6. Gap handling: apply L0 files in TXID order; `info.MinTXID` must equal `currentTXID+1`; gaps are bridged from higher compaction levels (replica.go:798-869, 932-994).

Opening the materialized replica read-only afterwards: with bytes 18/19 = 1/1 the file is a plain rollback-mode db — `sqlite3_open_v2(SQLITE_OPEN_READONLY)` works with no `-shm`/`-wal` considerations. If 18/19 are left at 2/2, a read-only open requires either an existing/creatable `-shm` or `file:...?immutable=1` (which is unsafe if liters keeps applying updates — immutable disables all change detection). Recommendation embodied by litestream: flip to 1/1 whenever the file will be read in place.

## 9. LTX↔WAL linkage fields (for wire compatibility)

L0 LTX header fields litestream writes per sync (db.go:1946-1958): `Version`, `Flags=HeaderFlagNoChecksum`, `PageSize`, `Commit` (pages), `MinTXID=MaxTXID=prevTXID+1` (one LTX per sync, may span many SQLite transactions), `Timestamp` (Unix ms), `WALOffset` (byte offset in `-wal` where this segment began), `WALSize` (bytes consumed: `maxOffset - WALOffset`), `WALSalt1/2` (current generation salts). Binary offsets within the 100-byte LTX header: Commit@12(u32), MinTXID@16(u64), MaxTXID@24(u64), Timestamp@32, WALOffset@48(i64), WALSize@56(i64), WALSalt1@64(u32), WALSalt2@68(u32), all BE (ltx.go:289-296). Salts/offset are zero for compacted (level>0) files; `verify()` only consults them on L0.

## 10. Constants summary

```
WAL_HEADER_SIZE        = 32
WAL_FRAME_HEADER_SIZE  = 24
WAL_MAGIC_LE           = 0x377F0682   // checksum words little-endian
WAL_MAGIC_BE           = 0x377F0683   // checksum words big-endian
WAL_VERSION            = 3007000
DB_HEADER_PAGE_SIZE_OFFSET = 16       // u16 BE, 1 => 65536
DB_HEADER_WRITE_VERSION    = 18       // 1=journal, 2=wal
DB_HEADER_READ_VERSION     = 19
DB_HEADER_CHANGE_COUNTER   = 24       // u32
DB_HEADER_DB_SIZE_PAGES    = 28       // u32, valid iff [24]==[92] && !=0
DB_HEADER_VERSION_VALID_FOR= 92
PENDING_BYTE           = 0x40000000
RESERVED_BYTE          = 0x40000001
SHARED_FIRST           = 0x40000002
SHARED_SIZE            = 510
lock_pgno(ps)          = 0x40000000/ps + 1
frame_offset(i, ps)    = 32 + i*(24+ps)
page_data_offset(i,ps) = frame_offset(i,ps) + 24
// litestream defaults (db.go:31-35): monitor 1s, checkpoint interval 1m,
// busy timeout 1s, MinCheckpointPageN 1000 (PASSIVE), TruncatePageN 121359 (TRUNCATE)
```

## 11. Rust implementation notes / invariants checklist

- All on-disk multi-byte WAL/DB header fields are big-endian **except** the checksum's interpretation of input words, which follows the magic. Use `wrapping_add`.
- WAL frame iteration termination conditions are all *soft* (return "no more frames"), never errors: short read, salt mismatch, checksum mismatch, header-checksum mismatch, file < 32 bytes.
- Never emit uncommitted frames: only merge a transaction's pages after seeing `commit != 0`; drop `pgno > commit` after the final commit (VACUUM shrink).
- Resume state to persist per push: `(wal_offset_end, salt1, salt2)`; on resume, verify previous frame's salts and byte-identical page content vs. last pushed LTX, else full snapshot.
- Snapshot = pages `1..=commit`, lock page skipped on encode, zero-filled on decode; WAL overlay wins over main-file page.
- Materialized file must be truncated to exactly `commit * page_size`; delete or never create stale `-wal`/`-shm`; flip header bytes 18/19→1 and randomize 24-27 when applying in place; hold SQLite byte-range locks (fcntl F_WRLCK at 0x40000000 len 1 and 0x40000002 len 510) during in-place applies if local SQLite readers exist.
