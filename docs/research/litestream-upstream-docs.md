All research complete. Here is the report.

---

# Litestream v0.5.x + LTX Wire-Compatibility Research Report

Research date: 2026-07-10. Litestream latest: **v0.5.14** (2026-07-06). LTX library: **github.com/superfly/ltx v0.5.1** (pinned by litestream `go.mod`). All byte offsets verified against actual source, not docs (several official docs are stale/wrong — see §10).

## 1. LTX file format — authoritative wire spec (format "Version 3", ltx v0.5.1)

Source of truth: `github.com/superfly/ltx` — `ltx.go`, `encoder.go`, `decoder.go`, `checksum.go` (main branch == v0.5.1 tag). All integers **big-endian**.

### 1.1 Constants

```
Magic          = "LTX1"        (4 bytes, ALL format versions 1–3 share this magic)
Version        = 3             (NOT stored in the file; implied — see §10 risk)
HeaderSize     = 100
PageHeaderSize = 6
TrailerSize    = 16
ChecksumSize   = 8
ChecksumFlag   = 1 << 63       (OR'd into every non-zero checksum to force non-zero)
MaxPageSize    = 65536         (valid page sizes: powers of 2, 512..65536)
PENDING_BYTE   = 0x40000000    (LockPgno(pageSize) = PENDING_BYTE/pageSize + 1; lock page NEVER encoded)
HeaderFlagNoChecksum = 1 << 1  (0x00000002); HeaderFlagMask = 0x00000002 (only valid flag bit in v3)
PageHeaderFlagSize   = 1 << 0  (0x0001; page data is LZ4 *block* format preceded by 4-byte size)
RFC3339Milli   = "2006-01-02T15:04:05.000Z07:00" (timestamp string format in JSON/metadata)
```

### 1.2 Header (100 bytes) — `Header.MarshalBinary`, ltx.go

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 4 | Magic `"LTX1"` | |
| 4 | 4 | Flags (uint32) | litestream always writes `0x00000002` (NoChecksum) |
| 8 | 4 | PageSize (uint32) | |
| 12 | 4 | Commit (uint32) | DB size in pages after applying file; 0 = deletion file |
| 16 | 8 | MinTXID (uint64) | ≥1 required; MinTXID==1 ⇒ snapshot |
| 24 | 8 | MaxTXID (uint64) | ≥ MinTXID |
| 32 | 8 | Timestamp (int64) | ms since Unix epoch |
| 40 | 8 | PreApplyChecksum | 0 when snapshot or NoChecksum; else must have bit63 set |
| 48 | 8 | WALOffset (int64) | source-WAL byte offset; 0 if journal/compaction |
| 56 | 8 | WALSize (int64) | 0 if journal |
| 64 | 4 | WALSalt1 (uint32) | 0 if journal or compaction |
| 68 | 4 | WALSalt2 (uint32) | |
| 72 | 8 | NodeID (uint64) | 0 if unset (litestream leaves 0) |
| 80 | 20 | Reserved (zeros) | |

**The superfly/ltx README table is STALE** (still shows a "Database ID" field at offset 16 and TXIDs at 20/28 with flags "always 0") — it describes the pre-v0.3 layout. Trust `MarshalBinary`, not the README (https://github.com/superfly/ltx).

Header validation invariants (`Header.Validate`): flags ⊆ mask; page size valid; 0 < MinTXID ≤ MaxTXID; WALOffset/WALSize ≥ 0; salts require WALOffset≠0; WALSize requires WALOffset≠0; snapshot ⇒ PreApplyChecksum==0; NoChecksum ⇒ PreApplyChecksum==0; otherwise PreApplyChecksum required with bit63 set.

### 1.3 Page block

Repeated frames, then a 6-byte all-zero PageHeader as end-of-pages sentinel:

```
PageHeader: [0:4] Pgno uint32   [4:6] Flags uint16
if Flags & PageHeaderFlagSize:
    [6:10] DataSize uint32 (BE)   [10:10+DataSize] LZ4 *block*-compressed page data
else (legacy ltx v0.5.0 encoding):
    LZ4 *frame*-format stream of one page (frame footer = 4-byte EndMark + 4-byte content checksum)
```

Encoder (v0.5.1, `encoder.go:EncodePage`) **always** LZ4-block-compresses (pierrec/lz4 v4 `CompressBlock`) and always sets `PageHeaderFlagSize`. Decoder handles both encodings (`decoder.go:DecodePageData`). Ordering invariants: non-snapshot files — strictly ascending pgnos, any subset; snapshot files — must start at pgno 1 and be sequential through `Commit`, skipping exactly the lock page; pgno ≤ Commit; encoding the lock page is an error; file must contain ≥1 page.

### 1.4 Page index (between page block sentinel and trailer) — added in ltx v0.5.0

`encoder.go:encodePageIndex` / `decoder.go:DecodePageIndex`:

```
for each page in ascending pgno order:
    uvarint pgno        (≠0)
    uvarint offset      (absolute byte offset of the page's PageHeader from file start)
    uvarint size        (bytes: PageHeader + size prefix + compressed data)
uvarint 0               (end marker)
uint64 BE index_size    (byte length of the index section EXCLUDING this 8-byte field)
```

Random access recipe (used by litestream `replica_client.go:fetchPageIndexData`): range-GET the last `max(32KB, ...)` bytes (`DefaultEstimatedPageIndexSize = 32*1024`); `index_size` is at `fileSize - TrailerSize - 8`; index starts at `fileSize - TrailerSize - 8 - index_size`; re-fetch if the guess was short. Then range-GET individual pages by (offset,size) and `ltx.DecodePageData`.

### 1.5 Trailer (16 bytes)

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | PostApplyChecksum (0 if NoChecksum; must equal `ChecksumFlag` alone for deletion files Commit==0) |
| 8 | 8 | FileChecksum (always required, bit63 set) |

### 1.6 Checksums

- Algorithm: **CRC-64 with the ISO polynomial** — Go `crc64.MakeTable(crc64.ISO)` (`encoder.go:EncodeHeader`, `decoder.go:NewDecoder`, `checksum.go:NewHasher`). NOT ECMA (litestream's own `docs/LTX_FORMAT.md` wrongly says ECMA — see §10).
- FileChecksum = `ChecksumFlag | crc64_ISO(header ‖ page-headers ‖ **uncompressed** page data ‖ page-block-sentinel ‖ page-index ‖ trailer[0:8])`. Note: the hash is fed *uncompressed* page bytes even though compressed bytes are on the wire (`encoder.go`: `_, _ = enc.hash.Write(data)` for page data; all other written bytes hashed as-written).
- Per-page checksum (used for DB rolling checksum, not stored per page): `ChecksumFlag | crc64_ISO(BE_uint32(pgno) ‖ pageData)` (`checksum.go:ChecksumPage`).
- Rolling DB checksum = XOR-accumulation of per-page checksums (maintained via `ChecksumReader`/`ChecksumPages`); **litestream v0.5 disables all of this** by setting `HeaderFlagNoChecksum` on every file it writes — L0 sync files (`db.go:1948`), compactions (`compactor` call sites set `Compactor.HeaderFlags = ltx.HeaderFlagNoChecksum`), and restore/VFS paths. Only FileChecksum is live in practice.
- `Pos` = `{TXID, PostApplyChecksum}`, string form `"%016x/%016x"` (33 chars). `PreApplyPos = {MinTXID-1, PreApplyChecksum}`.

### 1.7 `FileInfo` (listing metadata, ltx.go:579)

`{Level int; MinTXID, MaxTXID TXID; PreApplyChecksum, PostApplyChecksum Checksum; Size int64; CreatedAt time.Time}`.

### 1.8 Filenames

`FormatFilename`: `fmt.Sprintf("%016x-%016x.ltx", minTXID, maxTXID)`; regex `^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$`. Example `0000000000000001-0000000000000010.ltx`.

## 2. Bucket layout (the de-facto contract — NOT uniform across backends!)

There is **no documented public bucket-layout contract**; the layout is defined per replica client in litestream source. Two conventions exist in v0.5.0–v0.5.14:

**S3 (`s3/replica_client.go:655,703,1070,1392`) and Alibaba OSS (`oss/replica_client.go:390`):**
```
<path>/<level %04x>/<minTXID %016x>-<maxTXID %016x>.ltx
e.g. s3://bucket/db/0000/0000000000000001-0000000000000001.ltx   (L0)
     s3://bucket/db/0009/0000000000000001-00000000000004d2.ltx   (snapshot level)
```
No `ltx/` directory; level is 4-digit zero-padded lowercase hex.

**file, GCS (`gs`), Azure (`abs`), NATS, SFTP, WebDAV** — via `litestream.LTXFilePath` (`litestream.go:184-197`):
```
<path>/ltx/<level decimal>/<minTXID>-<maxTXID>.ltx
e.g. /backup/db/ltx/0/...ltx, /backup/db/ltx/9/...ltx
```

For "liters" you must implement the **S3 convention** for S3-compatible stores and be aware of the other for parity/tests. This split is undocumented and a plausible target for future unification (risk, §10).

**Other objects in the bucket:**
- Lease (v0.5.8+, S3 only): `<path>/lock.json` (`s3/leaser.go`: `DefaultLeasePath = "lock.json"`, `DefaultLeaseTTL = 30s`). JSON body `{"generation":int64,"expires_at":RFC3339,"owner":string}`; acquired/renewed/released with S3 **conditional writes** (ETag `If-Match`; delete with `If-Match`). LTX uploads themselves use **plain PutObject — no If-None-Match**.
- S3 object metadata: key `litestream-timestamp` (= `x-amz-meta-litestream-timestamp`), value RFC3339Nano of the LTX header timestamp — used for accurate timestamp-based restore (`MetadataKeyTimestamp`, `s3/replica_client.go:55`). File backend instead sets file mtime via Chtimes. Compaction preserves the earliest source timestamp (v0.5.1 fix #778).
- v0.3.x legacy tree (read-only support since v0.5.7, `v3.go`): `generations/{16-hex generation}/snapshots/{index:08x}.snapshot.lz4` and `.../wal/{index:08x}_{offset:08x}.wal.lz4`. v0.5 never writes this; restore auto-selects between v0.3 and LTX trees, choosing whichever has the most recent (or closest-before-timestamp) backup (https://litestream.io/docs/migration/). Generations are otherwise fully gone from the write path.
- Tigris endpoints get `X-Tigris-Consistent` header (v0.5.4).

## 3. Levels & compaction

`compaction_level.go`:
- `SnapshotLevel = 9` (fixed, reserved). Configurable levels are 0..8 (`Level > SnapshotLevel-1` rejected). CLI `-level` accepts 0–9.
- Defaults (`DefaultCompactionLevels`): L0 interval 0 (raw per-sync uploads, ~every `sync-interval`=1s), L1=30s, L2=5m, L3=1h. Config schema (litestream.yml top level):
```yaml
levels:            # index must equal level number; L0 implicit
  - interval: 5m
  - interval: 1h
  - interval: 24h
snapshot: {interval: 1h, retention: 24h}   # store defaults: interval 24h, retention 24h (store.go:60-63)
l0-retention: 5m                            # L0 kept 5m after compaction into L1 (store.go:68)
l0-retention-check-interval: 15s
retention: {enabled: true}                  # v0.5.8+: skip remote deletion for lifecycle-policy-managed buckets
```
- Compaction windows are wall-clock aligned: `PrevCompactionAt = now.Truncate(interval).UTC()`.
- Compaction (ltx `compactor.go` + litestream `compactor.go`): merge N contiguous files; inputs must have equal PageSize and contiguous TXIDs (unless `AllowNonContiguousTXIDs`); output `MinTXID = first input's MinTXID`, `MaxTXID = last input's MaxTXID`; newest version of each page wins; pages emitted sorted; lock page skipped; litestream sets `HeaderFlags = NoChecksum`. Highest configured level compacts into level 9 (full snapshot, MinTXID=1). Retention deletes snapshots older than `snapshot.retention` keeping ≥1, then deletes higher-level files no longer reachable (v0.5.1 changed enforcement to use minTXID; v0.5.14 preserved retention snapshot TXIDs and fixed compaction remote-read resumption).

## 4. Writer path (what a compatible pusher must produce)

`db.go` sync (~line 1946): each sync converts new WAL frames into ONE L0 LTX file with **`MinTXID == MaxTXID == prevTXID+1`** — a TXID is a *sync batch*, not a SQLite transaction. Header: `Version: ltx.Version, Flags: HeaderFlagNoChecksum, PageSize, Commit (post-txn page count), Timestamp: UnixMilli(now), WALOffset, WALSize, WALSalt1/2` (salts/offsets from the source WAL — informational; zero is legal "journal" mode, which is what a non-WAL-tailing writer like liters should write). On WAL discontinuity (checkpoint gap, salt mismatch) litestream writes a *full-DB* L0 file (all pages, still single TXID, `db.go:1964`) so TXID contiguity is preserved without a level-9 snapshot. Snapshot files (`db.go:2410`): `MinTXID: 1, MaxTXID: pos.TXID`, all pages. Uploads go to L0 (`ltx/0` or `0000/`); the store's background compactor produces L1+/snapshots. **Key invariant consumed by readers/restore: TXID ranges at each level are contiguous; a gap ⇒ `non-contiguous ltx files` error / restore-plan failure.** Since checksums are disabled, contiguity-by-TXID is the only continuity check — a compatible writer must never reuse or skip TXIDs.

DB-level config: `monitor-interval: 1s`, `checkpoint-interval: 1m` (PASSIVE), `min-checkpoint-page-count: 1000`, `truncate-page-n: 121359`, `busy-timeout: 1s`; replica: `sync-interval: 1s`, `auto-recover: false` (https://litestream.io/reference/config/).

## 5. Restore semantics

`replica.go:CalcRestorePlan(client, txID, timestamp, ...)` (v0.5.8+ streaming version, `replica.go:1441-1560`):
1. Scan level 9; keep the LAST snapshot with `MaxTXID ≤ txID` (or `CreatedAt < timestamp`). (Snapshot optional — plan can start from L0 file with MinTXID=1.)
2. Open one iterator per level maxLevel..0 (cursors). Repeatedly pick, across all levels, the candidate file with `MinTXID ≤ currentMax+1` that best extends coverage (largest MaxTXID wins); append; `currentMax = MaxTXID`; stop at target txID.
3. If restoring to latest (no txid/timestamp): verify no level has a next file with `MinTXID > currentMax+1`, else fail `non-contiguous ltx files`. Errors: `ErrTxNotAvailable` if plan empty or target beyond max.
4. Apply: decode files in order into the DB image (page writes at `(pgno-1)*pageSize`), truncate to Commit of last file. Post-restore integrity validation added v0.5.10.

CLI (https://litestream.io/reference/restore/): `-txid <16-hex>`, `-timestamp`, `-o`, `-parallelism 8`, `-if-db-not-exists`, `-if-replica-exists`, `-dry-run`, `-json`; **follow mode** `-f` + `-follow-interval 1s` (v0.5.9) continuously applies new LTX files and writes a `-txid` sidecar file next to the output DB for crash-resume — this is essentially the "poll-driven read replica to a real local file" mode liters wants; incompatible with `-txid`/`-timestamp`.

## 6. VFS read replica (litestream `vfs.go`, docs https://litestream.io/how-it-works/vfs/, blogs https://fly.io/blog/litestream-vfs/ + https://fly.io/blog/litestream-writable-vfs/)

- Startup: compute restore plan (contiguous LTX sequence; "fails fast on gaps, waits if no snapshot exists yet"); for each file, range-fetch page index only, build in-memory pgno→(file,offset,size) map.
- Reads: consult map → range-GET page bytes → LRU cache (`CacheSize` default 10MB). Page 1 is rewritten in-memory to present journal mode DELETE (replica looks like a read-only rollback DB; the real `-wal` never exists client-side).
- Polling: default 1s, scans L0 first, tolerates L0 gaps by deferring to higher levels (hence `l0-retention` ≥ poll interval matters). Two page indexes (main + pending) give snapshot isolation during open read transactions.
- Time travel: `PRAGMA`-based historical queries (v0.5.3, #850), observability PRAGMAs (#856), `PRAGMA litestream_hydration_progress` (v0.5.7), background hydration to local disk (v0.5.7), `PRAGMA litestream_write_enabled` (v0.5.9).
- Write mode (v0.5.6+, `LITESTREAM_WRITE_ENABLED=true`): local write buffer, sync every ~1s or on shutdown; **single-writer assumed**; polling disabled in write mode; conflict = `ErrConflict = errors.New("remote has newer transactions than expected")` (`vfs.go:40`) — optimistic detection by listing, not conditional PUTs. Fly's blog explicitly rejects multi-writer ("multiple-writer distributed SQLite databases are the Lament Configuration"). "Eventual durability" only. VFS compaction from the client side via shared `Compactor` (v0.5.7) — i.e., embedded writers are expected to run their own compaction, exactly liters' situation.
- VFS extension ships as prebuilt SQLite loadable extension via GitHub releases and PyPI/npm/RubyGems (v0.5.10).

## 7. Release timeline — wire-relevant changes (github.com/benbjohnson/litestream/releases)

- **v0.5.0** (2025-09-30): LTX storage engine (ltx v0.5.0: page index + per-page LZ4 *frame* compression); levels + retention; CGO-free (`modernc.org/sqlite`); NATS backend; `wal`→`ltx` command; **cannot restore v0.3 backups**; config backward compatible; single `replica:` (array deprecated).
- **v0.5.1** (2025-10-13): retention by minTXID; **timestamps preserved through compaction (#778)**; position preserved after restore (#783); **age encryption config rejected (#791)**; `-if-replica-exists` restored.
- v0.5.2: retention removes local L0 (#795).
- **v0.5.3** (2025-12-11): VFS page cache/polling; Tigris auto-detect; configurable payload signing + Content-MD5; time-travel PRAGMAs; OSS backend.
- **v0.5.4** (2025-12-17): **ltx upgraded v0.5.0→v0.5.1 (#909) — page encoding switched LZ4-frame→LZ4-block+size-prefix (PageHeaderFlagSize) with NO container version bump**; Tigris consistent-read header.
- v0.5.5: S3 SignPayload default true.
- **v0.5.6** (2026-01-09): VFS write support; SSE-C/SSE-KMS for S3.
- **v0.5.7** (2026-02-02): **v0.3.x restore compat (5-phase)**; hydration; Azure SAS.
- **v0.5.8** (2026-02-12): **distributed leasing via If-Match conditional writes (lock.json)**; register/unregister IPC; streaming restore-plan across levels; `skip-remote-deletion` retention.
- **v0.5.9** (2026-02-23): follow mode `-f`; sync IPC (on-demand push — relevant precedent for liters' explicit-push model); `PRAGMA litestream_write_enabled`.
- v0.5.10 (2026-03-19): `-txid` sidecar for follow crash-recovery; VFS packages on PyPI/npm/RubyGems.
- v0.5.11 (2026-04-08): nightly LTX behavioral tests; LTXError wrapping.
- v0.5.12 (2026-06-08): JSON CLI schemas; sync diagnostics "for library usage".
- v0.5.13 (2026-06-30): meta-directory for directory databases (local side).
- **v0.5.14** (2026-07-06): **S3 `storage-class` option** (PutObject StorageClass passthrough); SFTP atomic writes; compaction remote-read resumption fix.

## 8. LTX format version history & LiteFS divergence

| ltx module tag | wire `Version` | Page header | Compression | Page index | Flags |
|---|---|---|---|---|---|
| v0.3.x (≤ v0.3.18) | 1 | 4 bytes (pgno only) | optional whole-stream LZ4 via `HeaderFlagCompressLZ4 = 0x00000001` | none | mask 0x1 |
| v0.4.0 | 2 | 4 bytes | (flag 0x1 removed) | none | `HeaderFlagNoChecksum = 0x2`, mask 0x2 |
| v0.5.0 | 3 | 6 bytes (pgno+flags) | per-page LZ4 frame | yes | mask 0x2 |
| v0.5.1 (current) | 3 | 6 bytes | per-page LZ4 block + 4-byte size, `PageHeaderFlagSize=0x0001` | yes | mask 0x2 |

**All versions share magic `"LTX1"` and the Version is never serialized** (`UnmarshalBinary` hardcodes `h.Version = Version`). Files are distinguishable only heuristically (v1 compressed files have flag 0x1 which v3 rejects; v1/v2 uncompressed files would misparse under a v3 decoder). **LiteFS pins `superfly/ltx v0.3.14` (format v1)** — LiteFS LTX files and Litestream v0.5 LTX files are mutually unreadable despite identical magic and filename convention. LiteFS repo is still maintained (last push 2026-05-11) but on the frozen v1 format. Do not treat "LTX" as one format.

## 9. Prior/planned features affecting liters

- **Encryption (HIGH risk of format change):** age encryption removed in v0.5.0; migration guide states: *"Encryption support will be re-implemented directly in the LTX format to support per-page encryption. This is planned work but no timeline has been announced"* (https://litestream.io/docs/migration/). Expect a new header flag bit and/or page-header flag bits plus key metadata — the 20 reserved header bytes and 15 unused PageHeader flag bits are the obvious landing zone. A liters decoder must hard-fail on unknown header flags (mirror `IsValidHeaderFlags`) rather than ignore them.
- **Multi-writer:** explicitly rejected; single-writer with S3-conditional-write leases (lock.json) is the supported story ("Litestream: Revamped", https://fly.io/blog/litestream-revamped/; writable-VFS post). No write-forwarding shipped or announced for Litestream (write-forwarding was a LiteFS roadmap item only). No public v0.6 roadmap exists.
- **Read path:** VFS is the flagship consumer of the layout; its needs froze useful properties: page index at tail, 32KB tail-fetch convention, L0-gap tolerance, `l0-retention` grace window.

## 10. Compatibility guarantees & "will this change under us?" flags

**No formal stability guarantee exists anywhere** — not in superfly/ltx, litestream repo, or litestream.io. Observed posture:
- Bucket layout and LTX container changed incompatibly at v0.5.0 (generations→levels; new LTX version, same magic).
- The page encoding changed *within* format Version 3 between ltx v0.5.0 and v0.5.1 (frame→block), shipped mid-v0.5.x (litestream v0.5.4) with decoder-side dual support as the only compat mechanism. Expect future changes to follow this pattern: **new writers + tolerant readers, no version negotiation**. liters must read both encodings and should write the current one (block+size).
- v0.3 restore compat (v0.5.7) shows they do invest in *read-side* backward compat once formats stabilize.
- Nightly "LTX behavioral tests" + release gating since v0.5.11 signal the format is now treated as a stability surface.
- Official docs are unreliable for wire details: litestream.io/how-it-works still describes v0.3 generations/shadow-WAL; `docs/LTX_FORMAT.md` in the litestream repo claims CRC-64 **ECMA** (code: **ISO**), claims `Version == 2`, shows 4-byte page headers, and shows a fictional `/snapshots/20240101120000.ltx` path; superfly/ltx README shows the pre-v0.3 header. **Implement from `superfly/ltx` v0.5.1 source and litestream replica-client source only.**
- S3-vs-others path divergence (`%04x` no-`ltx/` vs `ltx/<decimal>`) is undocumented and inconsistent; a future unification would be a breaking bucket-layout change. Pin to litestream's exact per-backend behavior and integration-test against real `litestream` binaries.
- Reserved surface that may be claimed: header bytes 80–100, header flag bits other than 0x2, PageHeader flag bits other than 0x0001, `NodeID` (currently 0 from litestream; used by LiteFS-lineage code), `lock.json` schema, S3 metadata keys.

### Primary sources
- https://github.com/superfly/ltx (ltx.go, encoder.go, decoder.go, checksum.go, compactor.go; tags v0.3.14/v0.4.0/v0.5.0/v0.5.1)
- https://github.com/benbjohnson/litestream (litestream.go:184–197; compaction_level.go; store.go:58–81; db.go:1946,2410; replica.go:1441–1560; replica_client.go:88–160; s3/replica_client.go:55,655,703,1392; s3/leaser.go:219–285; vfs.go:39–92; docs/LTX_FORMAT.md; docs/REPLICA_CLIENT_GUIDE.md §v0.3.x Path Structure) and /releases
- https://litestream.io/reference/config/ · /reference/restore/ · /reference/ltx/ · /how-it-works/vfs/ · /docs/migration/
- https://fly.io/blog/litestream-revamped/ · https://fly.io/blog/litestream-v050-is-here/ · https://fly.io/blog/litestream-vfs/ · https://fly.io/blog/litestream-writable-vfs/
- https://mtlynch.io/notes/hold-off-on-litestream-0.5.0/ (early v0.5.0 restore-gap bug #752, "transaction not available" reports)
