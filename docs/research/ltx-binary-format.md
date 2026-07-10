All source files read and cross-checked. The report follows as my final answer (read-only research; no plan file needed).

# LTX v0.5.1 Byte-Level Format Specification (ground truth: `github.com/superfly/ltx@v0.5.1` source)

**Authoritative note:** `reference/litestream/docs/LTX_FORMAT.md` contains multiple errors vs. the code — it claims CRC-64 **ECMA** (code uses CRC-64 **ISO**, `checksum.go:178`), claims `Version == 2` (code: `Version = 3`, `ltx.go:23`), shows a 4-byte page header with a per-page checksum field (actual: 6 bytes, `{Pgno u32, Flags u16}`, no checksum), and omits LZ4 compression, the page-block end marker, and the page-index binary layout entirely. **The code wins on every discrepancy.**

## 1. Constants

| Constant | Value | Source |
|---|---|---|
| `Magic` | ASCII `"LTX1"` (`4C 54 58 31`) | ltx.go:20 |
| `Version` | `3` (implied by magic; not stored separately) | ltx.go:23 |
| `HeaderSize` | 100 | ltx.go:28 |
| `PageHeaderSize` | 6 | ltx.go:29 |
| `TrailerSize` | 16 | ltx.go:30 |
| `ChecksumSize` | 8 | ltx.go:39 |
| `TrailerChecksumOffset` | 8 (= TrailerSize − ChecksumSize) | ltx.go:40 |
| `ChecksumFlag` | `1 << 63` = `0x8000000000000000` | ltx.go:55 |
| `HeaderFlagNoChecksum` | `1 << 1` = `0x00000002` | ltx.go:175 |
| `HeaderFlagMask` | `0x00000002` (only valid flag; bit 0 is **invalid** in v0.5.x) | ltx.go:173 |
| `MaxPageSize` | 65536 | ltx.go:396 |
| `PENDING_BYTE` | `0x40000000` | ltx.go:491 |

All multi-byte integers in LTX structures are **big-endian**. Page-index element tuples are unsigned LEB128 varints (Go `binary.AppendUvarint`). LZ4 frames internally are little-endian per the LZ4 frame spec.

## 2. Overall file structure

```
[Header: 100 bytes]
[Page frame]*            (0..N; N==0 only for deletion files, Commit=0)
[End-of-pages marker: 6 zero bytes]        (an all-zero PageHeader)
[Page index: varint tuples + varint 0 end marker]
[Page index size: 8 bytes BE u64]          (size of index bytes EXCLUDING this field)
[Trailer: 16 bytes]
```

## 3. Header (100 bytes) — ltx.go:283–326

| Offset | Size | Field | Type | Notes |
|---|---|---|---|---|
| 0 | 4 | Magic | `"LTX1"` | decoder: mismatch → `ErrInvalidFile` (ltx.go:320) |
| 4 | 4 | Flags | u32 BE | only `0x2` (NoChecksum) may be set |
| 8 | 4 | PageSize | u32 BE | power of 2 in [512, 65536] (ltx.go:399–406) |
| 12 | 4 | Commit | u32 BE | DB size **in pages after applying this file** (truncation target). 0 = deletion file |
| 16 | 8 | MinTXID | u64 BE | must be ≥ 1 |
| 24 | 8 | MaxTXID | u64 BE | must be ≥ 1 and ≥ MinTXID |
| 32 | 8 | Timestamp | i64 BE | milliseconds since Unix epoch (not validated) |
| 40 | 8 | PreApplyChecksum | u64 BE | rolling DB checksum before applying this file |
| 48 | 8 | WALOffset | i64 BE | byte offset in original WAL; 0 if from journal. Must be ≥ 0 |
| 56 | 8 | WALSize | i64 BE | original WAL segment size; must be ≥ 0; must be 0 if WALOffset==0 |
| 64 | 4 | WALSalt1 | u32 BE | 0 if journal/compaction; nonzero salt requires WALOffset≠0 |
| 68 | 4 | WALSalt2 | u32 BE | same rule |
| 72 | 8 | NodeID | u64 BE | 0 if unset; dropped on compaction (compactor.go:110) |
| 80 | 20 | Reserved | zeros | writer zero-fills; **reader does not verify** |

`Header.Validate()` (ltx.go:208–267) enforces, in order: Version==3; `flags == flags & 0x2`; valid page size; MinTXID≠0; MaxTXID≠0; MinTXID≤MaxTXID; WALOffset≥0; WALSize≥0; salts⇒WALOffset≠0; WALSize≠0⇒WALOffset≠0; then checksum rules:
- **Snapshot** (`IsSnapshot() ⇔ MinTXID == 1`, ltx.go:198): `PreApplyChecksum` MUST be 0.
- Non-snapshot + `NoChecksum` flag: `PreApplyChecksum` MUST be 0.
- Non-snapshot + checksums enabled: `PreApplyChecksum` MUST be nonzero AND have bit 63 (`ChecksumFlag`) set.

Positions (ltx.go:275–280, decoder.go:54): `PreApplyPos = (MinTXID−1, PreApplyChecksum)`, `PostApplyPos = (MaxTXID, Trailer.PostApplyChecksum)`. `Pos` string form: `"%016x/%016x"`, 33 chars (ltx.go:80–103). Contiguity check used for chaining/compaction: `IsContiguous(prevMax, min, max) = min <= prevMax+1 && max > prevMax` (ltx.go:623–625) — overlap allowed if it extends.

## 4. Page frames — ltx.go:408–447, encoder.go:206–267

Each frame = 6-byte page header + **one complete standalone LZ4 frame** containing exactly `PageSize` bytes of uncompressed page data.

PageHeader layout:

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 4 | Pgno | u32 BE, 1-based; 0 is the end-of-pages sentinel |
| 4 | 2 | Flags | u16 BE, MUST be 0 ("reserved for future use", ltx.go:424) |

**End-of-pages marker:** an all-zero PageHeader (6 zero bytes), written by `Encoder.Close` (encoder.go:88–93); decoder treats `hdr.IsZero()` as EOF of the page block (decoder.go:166–169). It is followed by **no** page data.

### 4.1 Compression (always on in v0.5.x)

Every page's data is compressed as an independent LZ4 **frame** (the encoder does `zw.Reset` per page, encoder.go:281): pierrec/lz4 v4.1.22 with `BlockSizeOption(Block64Kb)` and `CompressionLevelOption(lz4.Fast)` (encoder.go:43–49). pierrec defaults otherwise apply (options.go:31–34): content checksum **enabled**, block checksums disabled, block-independence **true** (lz4stream/frame.go:141). Resulting frame bytes:

```
04 22 4D 18                         LZ4 frame magic (LE 0x184D2204)
FLG = 0x64                          version=01, blk-indep=1, blk-cksum=0, content-size=0, content-cksum=1, dictID=0
BD  = 0x40                          max block size = 64 KiB
HC                                  (xxHash32(FLG||BD, seed=0) >> 8) & 0xFF
[blocks]                            each: u32 LE size (high bit set ⇒ stored uncompressed), then data
00 00 00 00                         end mark
xxHash32(uncompressed data, seed=0) u32 LE content checksum
```
Max page size (65536) exactly fits one 64 KiB block. Since the **file checksum covers the UNCOMPRESSED page bytes** (see §6), a Rust encoder may use any spec-compliant LZ4 frame writer — byte-identical compression is NOT required for compatibility; only the page-index offsets/sizes must reflect your actual compressed lengths. A Rust **decoder** must decode exactly one frame per page and consume exactly the frame's bytes from the stream (Go verifies this by requiring EOF after `PageSize` bytes — "expected lz4 end frame", decoder.go:276–281); do not over-read into the next page header.

### 4.2 Encoder-side page rules (encoder.go:206–242)

- `len(data) == PageSize` exactly.
- `Pgno <= Commit` (else "out-of-bounds for commit size").
- `Pgno != 0`, `Flags == 0`.
- **Lock page never encoded**: `LockPgno(pageSize) = PENDING_BYTE/pageSize + 1` (ltx.go:494–496). E.g. 4096→262145, 512→2097153, 65536→16385. Only reachable when DB ≥ 1 GiB.
- **Snapshot files (MinTXID==1)**: first page MUST be pgno 1; pages strictly sequential `prev+1`, except after `lockPgno−1` the next must be `prev+2` (skip lock page). Must include ALL pages 1..Commit except the lock page.
- **Non-snapshot files**: pgnos strictly ascending (`prevPgno < Pgno`); any subset of pages.
- Decoder does NOT re-verify ordering/commit-bound/lock-page on non-snapshots — but a compatible encoder must obey them.

## 5. Page index — encoder.go:137–174, decoder.go:309–346

Written after the end-of-pages marker, uncompressed, included in the file checksum. Elements sorted ascending by pgno:

```
repeat per page:  uvarint(pgno) uvarint(offset) uvarint(size)
end marker:       uvarint(0)                       (single 0x00 byte)
index size:       u64 BE = byte length of (tuples + end marker), excluding this 8-byte field
```

- `offset` = absolute file offset (from byte 0 of the file) of the page's 6-byte PageHeader (encoder.go:244, 261–264).
- `size` = 6 + compressed-LZ4-frame length for that page (encoder.go:263).
- Random access: read last `TrailerSize + 8 = 24` bytes; `indexSize = u64be(file[len-24 : len-16])`; index starts at `len − 16 − 8 − indexSize`. Then `DecodePageIndex` semantics: read tuples until pgno varint is 0, then read the 8-byte size (decoder.go:310–346).

## 6. Trailer (16 bytes) — ltx.go:348–393

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 8 | PostApplyChecksum | u64 BE; rolling DB checksum after applying this file; MUST be 0 when `NoChecksum`, else nonzero with bit 63 set |
| 8 | 8 | FileChecksum | u64 BE; `ChecksumFlag | crc64_iso(covered bytes)`; always required, bit 63 set |

`Trailer.Validate` (ltx.go:355–374). Note: the Go decoder does **not** call `Trailer.Validate` on read; the encoder does on write. Deletion files (Commit==0) additionally require `PostApplyChecksum == ChecksumFlag` exactly (`0x8000000000000000`, "empty checksum"), enforced at `Encoder.Close` (encoder.go:119–121) — consequently deletion files cannot use the `NoChecksum` flag (the two requirements conflict).

## 7. Checksums — three distinct kinds, one hash function

**Hash function everywhere:** CRC-64 with Go's `crc64.MakeTable(crc64.ISO)` (encoder.go:189, decoder.go:36, checksum.go:178). This is **CRC-64/GO-ISO**: width=64, normal poly `0x000000000000001B` (Go's `crc64.ISO = 0xD800000000000000` is the reflected representation), init `0xFFFFFFFFFFFFFFFF`, refin=true, refout=true, xorout `0xFFFFFFFFFFFFFFFF`, check("123456789") = `0xB90956C775A41001`. In Rust: `crc::CRC_64_GO_ISO`.

### 7.1 Per-page checksum (checksum.go:106–116)
```
ChecksumPage(pgno, data) = ChecksumFlag | crc64_go_iso( u32_be(pgno) ‖ data )
```
Bit 63 is always set (destroying the CRC's top bit); this guarantees checksums are nonzero.

### 7.2 Rolling database checksum (pre-apply / post-apply) (checksum.go:119–132, decoder.go:189–193)
The checksum of an entire database of `Commit` pages is the XOR of all per-page checksums, excluding the lock page, with bit 63 forced on after every step:
```
chksum = 0
for pgno in 1..=commit:
    if pgno == lock_pgno(page_size): continue
    chksum = ChecksumFlag | (chksum ^ ChecksumPage(pgno, page_data))
```
Because every per-page checksum has bit 63 set, this is equivalent to `ChecksumFlag | XOR(per_page_checksums & !ChecksumFlag)`. **Incremental maintenance** (the key to producing PreApply/PostApply without rescanning): when page p changes from old→new bytes,
```
new_db = ChecksumFlag | (old_db ^ ChecksumPage(p, old_data) ^ ChecksumPage(p, new_data))
```
Grow: XOR in new pages' checksums. Truncate (Commit shrinks): XOR out removed pages' checksums. `Header.PreApplyChecksum` = DB rolling checksum at TXID MinTXID−1; `Trailer.PostApplyChecksum` = DB rolling checksum at MaxTXID. Snapshots have PreApply = 0 (no prior state). Helper `ChecksumPages` (checksum.go:26–103) computes per-page checksums of a raw DB file, optionally with 24 workers when `pageSize*nPages > 512 MiB`.

### 7.3 File checksum (integrity of the LTX file itself)
`Trailer.FileChecksum = ChecksumFlag | crc64_go_iso(covered bytes)` where covered bytes are, in order (traced through `enc.write`/`writeCompressed`/`writeToHash`, encoder.go:270–307, and decoder mirror, decoder.go:83,128,163,180):
1. the 100 header bytes;
2. per page: the 6 page-header bytes, then the **UNCOMPRESSED** `PageSize` bytes of page data (encoder.go:298 — compressed bytes go to the file, uncompressed bytes go to the hash);
3. the 6-byte end-of-pages marker;
4. all page-index bytes including the 8-byte index-size field;
5. the trailer's first 8 bytes, i.e. PostApplyChecksum big-endian (encoder.go:110).
The FileChecksum field itself is excluded. Verification: recompute, OR with ChecksumFlag, compare; mismatch → `ErrChecksumMismatch` (decoder.go:101–103).

## 8. Snapshot vs non-snapshot semantics

- **Snapshot** ⇔ `MinTXID == 1`. Must contain every page 1..Commit except the lock page, in order; `PreApplyChecksum = 0`; decoder incrementally computes the rolling checksum during `DecodePage` and at `Close` verifies it equals `Trailer.PostApplyChecksum` (decoder.go:106–110) unless `NoChecksum`.
- **Non-snapshot**: sparse ascending page set; PreApplyChecksum links it to the prior state (`PosMismatchError` when a consumer's current Pos ≠ file's PreApplyPos, ltx.go:112–124). PostApplyChecksum is not verifiable from the file alone.
- `DecodeDatabaseTo` (decoder.go:223–268) materializes a SQLite DB from a snapshot: for pgno 1..Commit write decoded pages in order, emitting an all-zeros page for the lock pgno; errors if any frame's pgno mismatches expectation, if pages remain after Commit, or on `Close` failure.
- **Deletion file**: Commit=0, zero page frames (header → 6-byte zero marker → empty index (just `00` + 8-byte size=1) → trailer), PostApplyChecksum = `0x8000000000000000`.

## 9. Decoder algorithm & mandatory error checks (decoder.go)

Streaming decode: `DecodeHeader` (read 100 B, hash, unmarshal, magic check, `Validate`, init `chksum=ChecksumFlag` if tracking) → loop `DecodePage` (read 6 B header, hash; zero header ⇒ page block done; `hdr.Validate()`; lz4-decode exactly PageSize bytes, hash them; require lz4 frame EOF; snapshot: fold page into rolling checksum unless lock page) → `Close` (read all remaining bytes; hash `remaining[:len−8]`; parse page index; parse trailer; compare FileChecksum; snapshot: compare PostApplyChecksum). `Verify()` = exactly that sequence discarding data (decoder.go:200–219). A compatible decoder must detect: bad magic; header validation failures (§3); page `Flags != 0`; short reads anywhere; LZ4 frame longer/shorter than PageSize; trailing garbage inside an LZ4 frame; FileChecksum mismatch; snapshot PostApply mismatch; malformed varints in the index. Not checked by Go (decoder is lenient; encoder is strict): page ordering on non-snapshots, pgno ≤ Commit, page count vs Commit for snapshots (`TODO` at decoder.go:98), trailer format validation, reserved header bytes.

## 10. Encoder algorithm (exact write order)

1. `EncodeHeader(hdr)`: validate; init CRC; write 100 bytes (encoder.go:177–203).
2. For each page (sorted per §4.2): write 6-byte header; compress page into a fresh LZ4 frame; write frame; record `index[pgno] = {Offset: file_offset_of_page_header, Size: 6 + frame_len}` (encoder.go:206–267).
3. `SetPostApplyChecksum` (must be called before Close; encoder.go:75–77).
4. `Close`: write 6 zero bytes; write index tuples sorted by pgno + `00` end marker + u64 BE index size; hash trailer[0..8]; `FileChecksum = ChecksumFlag | crc`; validate trailer (+deletion rule); write 16-byte trailer (encoder.go:80–135).

## 11. Naming, compaction, ancillary

- Filename: `%016x-%016x.ltx` of (MinTXID, MaxTXID); parse regex `^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$` (ltx.go:449–489). Litestream stores these under level dirs (e.g. `ltx/0000/...`).
- Timestamp string form where used externally: `RFC3339Milli = "2006-01-02T15:04:05.000Z07:00"` fixed-width UTC, sortable (ltx.go:35, 461–482).
- **Compactor** (compactor.go:78–228): inputs must be pre-sorted ascending by TXID with matching PageSize and contiguous TXIDs (per `IsContiguous`, overridable via `AllowNonContiguousTXIDs`). Output header: `{Flags: c.HeaderFlags, PageSize: first.PageSize, Commit: last.Commit, MinTXID: first.MinTXID, MaxTXID: last.MaxTXID, Timestamp: last.Timestamp, PreApplyChecksum: first.PreApplyChecksum, NodeID: 0}`. Merge: k-way by lowest pending pgno; on ties the **latest input wins**; pages with `pgno > last.Commit` are dropped (truncation); output PostApplyChecksum = last input's trailer PostApplyChecksum. Compacting a chain starting at a snapshot yields a snapshot, and the encoder's sequential-page rule then enforces full coverage.
- `PageIndexElem` in-memory struct carries `{Level int, MinTXID, MaxTXID TXID, Offset, Size int64}` (encoder.go:309–316); only Offset/Size are on the wire — Level/TXIDs are populated by the caller of `DecodePageIndex` (decoder.go:310).

## 12. Rust implementation notes (liters-specific)

- Checksum: `crc = crc::Crc::<u64>::new(&crc::CRC_64_GO_ISO)`; per-page = `0x8000_0000_0000_0000 | crc(pgno.to_be_bytes() ++ page)`.
- LZ4: any frame-format-compliant encoder works (file checksum covers uncompressed bytes); decoder must track exact frame byte length (use the page index `Size − 6`, or a frame reader that reports consumed bytes). For byte-parity with Go output, emit FLG=0x64/BD=0x40 frames with content checksum, 64 KiB blocks, fast level.
- Writer-side (push): maintain rolling DB checksum incrementally via §7.2 XOR; emit non-snapshot files with `PreApplyChecksum` = last `PostApplyChecksum`, `MinTXID = lastTXID+1`; snapshots when starting fresh (`MinTXID=1`, all pages, PreApply=0). Consumers compare `PreApplyPos` to their current `Pos` and reject on mismatch (`PosMismatchError` semantics).
- Reader-side (subscribe): fetch trailer+index tail (last 24 bytes → index) for point page reads over ranged GETs, or stream-decode whole files; apply pages in pgno order, truncate/extend DB to `Commit` pages, verify rolling checksum equals `PostApplyChecksum` after apply.
