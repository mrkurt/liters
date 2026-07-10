use std::fmt;
use std::time::SystemTime;

use crate::{
    Checksum, Error, Result, CHECKSUM_FLAG, HEADER_FLAG_MASK, HEADER_FLAG_NO_CHECKSUM,
    HEADER_SIZE, MAGIC, MAX_PAGE_SIZE, PAGE_HEADER_SIZE, PENDING_BYTE, TRAILER_SIZE, VERSION,
};

/// A transaction ID. Displayed as fixed-width lowercase hex (`%016x`). (ltx.go:127)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Txid(pub u64);

impl Txid {
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Parses a 16-character hex string. (ltx.go:130)
    pub fn parse(s: &str) -> Option<Txid> {
        // Explicit digit check: from_str_radix would accept a leading '+',
        // which Go's ParseUint rejects.
        if s.len() != 16 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        u64::from_str_radix(s, 16).ok().map(Txid)
    }
}

impl fmt::Display for Txid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl fmt::Debug for Txid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Txid({:016x})", self.0)
    }
}

/// The transactional position of a database: TXID + rolling checksum. (ltx.go:66)
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Pos {
    pub txid: Txid,
    pub post_apply_checksum: Checksum,
}

impl Pos {
    pub fn new(txid: Txid, post_apply_checksum: Checksum) -> Pos {
        Pos { txid, post_apply_checksum }
    }

    pub fn is_zero(self) -> bool {
        self == Pos::default()
    }

    /// Parses the 33-character `txid/checksum` form. (ltx.go:80)
    pub fn parse(s: &str) -> Option<Pos> {
        if s.len() != 33 || s.as_bytes()[16] != b'/' {
            return None;
        }
        Some(Pos {
            txid: Txid::parse(&s[..16])?,
            post_apply_checksum: Checksum::parse(&s[17..])?,
        })
    }
}

impl fmt::Display for Pos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.txid, self.post_apply_checksum)
    }
}

/// Returns true if `sz` is a power of two between 512 and 64K. (ltx.go:399)
pub fn is_valid_page_size(sz: u32) -> bool {
    (512..=MAX_PAGE_SIZE).contains(&sz) && sz.is_power_of_two()
}

/// Returns true unless flags outside the valid mask are set. (ltx.go:344)
pub fn is_valid_header_flags(flags: u32) -> bool {
    flags == flags & HEADER_FLAG_MASK
}

/// The page number containing SQLite's PENDING_BYTE locking region.
/// Never stored in an LTX file; only reachable when the DB is ≥1GiB. (ltx.go:494)
pub fn lock_pgno(page_size: u32) -> u32 {
    (PENDING_BYTE / page_size as u64) as u32 + 1
}

/// Returns true if the transaction range `[min_txid, max_txid]` is contiguous
/// with (or overlapping-but-extending) a database at `prev_max_txid`.
/// Wrapping addition matches Go's defined overflow for `prev = u64::MAX`.
/// (ltx.go:623)
pub fn is_contiguous(prev_max_txid: Txid, min_txid: Txid, max_txid: Txid) -> bool {
    min_txid.0 <= prev_max_txid.0.wrapping_add(1) && max_txid.0 > prev_max_txid.0
}

/// Formats an LTX filename: `%016x-%016x.ltx`. (ltx.go:487)
pub fn format_filename(min_txid: Txid, max_txid: Txid) -> String {
    format!("{min_txid}-{max_txid}.ltx")
}

/// Parses an LTX filename of the form `%016x-%016x.ltx`. (ltx.go:450, regex ltx.go:484)
pub fn parse_filename(name: &str) -> Option<(Txid, Txid)> {
    let rest = name.strip_suffix(".ltx")?;
    let bytes = rest.as_bytes();
    if bytes.len() != 33 || bytes[16] != b'-' {
        return None;
    }
    if !bytes[..16].iter().chain(&bytes[17..]).all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b)) {
        return None;
    }
    Some((Txid::parse(&rest[..16])?, Txid::parse(&rest[17..])?))
}

/// The header frame of an LTX file. 100 bytes on disk, big-endian. (ltx.go:179)
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Header {
    pub flags: u32,
    pub page_size: u32,
    /// Database size in pages after this file is applied. 0 = deletion file.
    pub commit: u32,
    pub min_txid: Txid,
    pub max_txid: Txid,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// Rolling database checksum before this file applies. Zero on snapshots
    /// and whenever `HEADER_FLAG_NO_CHECKSUM` is set.
    pub pre_apply_checksum: Checksum,
    /// Byte offset in the source WAL; zero if journal-derived or compacted.
    pub wal_offset: i64,
    /// Size of the source WAL segment; zero if journal-derived.
    pub wal_size: i64,
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    /// Originating node; zero if unset. Dropped by compaction.
    pub node_id: u64,
}

impl Header {
    /// A snapshot contains every page of the database. (ltx.go:198)
    pub fn is_snapshot(&self) -> bool {
        self.min_txid.0 == 1
    }

    /// True if pre/post-apply checksums are not tracked. (ltx.go:270)
    pub fn no_checksum(&self) -> bool {
        self.flags & HEADER_FLAG_NO_CHECKSUM != 0
    }

    pub fn lock_pgno(&self) -> u32 {
        lock_pgno(self.page_size)
    }

    /// The database position required before applying this file. Wrapping,
    /// as in Go: an (invalid) MinTXID of 0 yields TXID u64::MAX. (ltx.go:275)
    pub fn pre_apply_pos(&self) -> Pos {
        Pos {
            txid: Txid(self.min_txid.0.wrapping_sub(1)),
            post_apply_checksum: self.pre_apply_checksum,
        }
    }

    /// Validation rules, in Go's exact order. (ltx.go:208-267)
    pub fn validate(&self) -> Result<()> {
        if !is_valid_header_flags(self.flags) {
            return Err(Error::invalid(format!("invalid flags: 0x{:08x}", self.flags)));
        }
        if !is_valid_page_size(self.page_size) {
            return Err(Error::invalid(format!("invalid page size: {}", self.page_size)));
        }
        if self.min_txid.is_zero() {
            return Err(Error::invalid("minimum transaction id required"));
        }
        if self.max_txid.is_zero() {
            return Err(Error::invalid("maximum transaction id required"));
        }
        if self.min_txid > self.max_txid {
            return Err(Error::invalid(format!(
                "transaction ids out of order: ({},{})",
                self.min_txid.0, self.max_txid.0
            )));
        }
        if self.wal_offset < 0 {
            return Err(Error::invalid(format!("wal offset cannot be negative: {}", self.wal_offset)));
        }
        if self.wal_size < 0 {
            return Err(Error::invalid(format!("wal size cannot be negative: {}", self.wal_size)));
        }
        if (self.wal_salt1 != 0 || self.wal_salt2 != 0) && self.wal_offset == 0 {
            return Err(Error::invalid("wal offset required if salt exists"));
        }
        if self.wal_offset == 0 && self.wal_size != 0 {
            return Err(Error::invalid("wal offset required if wal size exists"));
        }

        if self.is_snapshot() {
            if !self.pre_apply_checksum.is_zero() {
                return Err(Error::invalid("pre-apply checksum must be zero on snapshots"));
            }
        } else if self.no_checksum() {
            if !self.pre_apply_checksum.is_zero() {
                return Err(Error::invalid("pre-apply checksum not allowed"));
            }
        } else {
            if self.pre_apply_checksum.is_zero() {
                return Err(Error::invalid("pre-apply checksum required on non-snapshot files"));
            }
            if !self.pre_apply_checksum.is_flagged() {
                return Err(Error::invalid("invalid pre-apply checksum format"));
            }
        }

        Ok(())
    }

    /// Encodes to the 100-byte on-disk form. (ltx.go:283)
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut b = [0u8; HEADER_SIZE];
        b[0..4].copy_from_slice(MAGIC);
        b[4..8].copy_from_slice(&self.flags.to_be_bytes());
        b[8..12].copy_from_slice(&self.page_size.to_be_bytes());
        b[12..16].copy_from_slice(&self.commit.to_be_bytes());
        b[16..24].copy_from_slice(&self.min_txid.0.to_be_bytes());
        b[24..32].copy_from_slice(&self.max_txid.0.to_be_bytes());
        b[32..40].copy_from_slice(&(self.timestamp as u64).to_be_bytes());
        b[40..48].copy_from_slice(&self.pre_apply_checksum.0.to_be_bytes());
        b[48..56].copy_from_slice(&(self.wal_offset as u64).to_be_bytes());
        b[56..64].copy_from_slice(&(self.wal_size as u64).to_be_bytes());
        b[64..68].copy_from_slice(&self.wal_salt1.to_be_bytes());
        b[68..72].copy_from_slice(&self.wal_salt2.to_be_bytes());
        b[72..80].copy_from_slice(&self.node_id.to_be_bytes());
        // Bytes 80..100 are reserved and zero.
        b
    }

    /// Decodes from the on-disk form. The magic is checked; reserved bytes are
    /// not (mirroring Go). (ltx.go:302)
    pub fn decode(b: &[u8]) -> Result<Header> {
        if b.len() < HEADER_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short header buffer",
            )));
        }
        if &b[0..4] != MAGIC {
            return Err(Error::InvalidFile);
        }
        Ok(Header {
            flags: u32::from_be_bytes(b[4..8].try_into().unwrap()),
            page_size: u32::from_be_bytes(b[8..12].try_into().unwrap()),
            commit: u32::from_be_bytes(b[12..16].try_into().unwrap()),
            min_txid: Txid(u64::from_be_bytes(b[16..24].try_into().unwrap())),
            max_txid: Txid(u64::from_be_bytes(b[24..32].try_into().unwrap())),
            timestamp: u64::from_be_bytes(b[32..40].try_into().unwrap()) as i64,
            pre_apply_checksum: Checksum(u64::from_be_bytes(b[40..48].try_into().unwrap())),
            wal_offset: u64::from_be_bytes(b[48..56].try_into().unwrap()) as i64,
            wal_size: u64::from_be_bytes(b[56..64].try_into().unwrap()) as i64,
            wal_salt1: u32::from_be_bytes(b[64..68].try_into().unwrap()),
            wal_salt2: u32::from_be_bytes(b[68..72].try_into().unwrap()),
            node_id: u64::from_be_bytes(b[72..80].try_into().unwrap()),
        })
    }
}

/// The trailer frame of an LTX file. 16 bytes on disk. (ltx.go:349)
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Trailer {
    /// Rolling database checksum after this file is applied. Zero when
    /// `HEADER_FLAG_NO_CHECKSUM` is set.
    pub post_apply_checksum: Checksum,
    /// CRC-64/GO-ISO of the file contents (with uncompressed page data),
    /// excluding this field itself. Always present.
    pub file_checksum: Checksum,
}

impl Trailer {
    /// Validation rules. (ltx.go:355-374)
    pub fn validate(&self, h: &Header) -> Result<()> {
        if h.no_checksum() {
            if !self.post_apply_checksum.is_zero() {
                return Err(Error::invalid("post-apply checksum not allowed"));
            }
        } else if self.post_apply_checksum.is_zero() {
            return Err(Error::invalid("post-apply checksum required"));
        } else if !self.post_apply_checksum.is_flagged() {
            return Err(Error::invalid("invalid post-apply checksum format"));
        }

        if self.file_checksum.is_zero() {
            return Err(Error::invalid("file checksum required"));
        } else if !self.file_checksum.is_flagged() {
            return Err(Error::invalid("invalid file checksum format"));
        }
        Ok(())
    }

    pub fn encode(&self) -> [u8; TRAILER_SIZE] {
        let mut b = [0u8; TRAILER_SIZE];
        b[0..8].copy_from_slice(&self.post_apply_checksum.0.to_be_bytes());
        b[8..16].copy_from_slice(&self.file_checksum.0.to_be_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Result<Trailer> {
        if b.len() < TRAILER_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short trailer buffer",
            )));
        }
        Ok(Trailer {
            post_apply_checksum: Checksum(u64::from_be_bytes(b[0..8].try_into().unwrap())),
            file_checksum: Checksum(u64::from_be_bytes(b[8..16].try_into().unwrap())),
        })
    }
}

/// The header for a single page frame. 6 bytes on disk. (ltx.go:409)
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct PageHeader {
    pub pgno: u32,
    /// Reserved; must be zero in format version 3.
    pub flags: u16,
}

impl PageHeader {
    /// An all-zero page header marks the end of the page block. (ltx.go:415)
    pub fn is_zero(&self) -> bool {
        *self == PageHeader::default()
    }

    /// (ltx.go:420)
    pub fn validate(&self) -> Result<()> {
        if self.pgno == 0 {
            return Err(Error::invalid("page number required"));
        }
        if self.flags != 0 {
            return Err(Error::invalid("no flags allowed, reserved for future use"));
        }
        Ok(())
    }

    pub fn encode(&self) -> [u8; PAGE_HEADER_SIZE] {
        let mut b = [0u8; PAGE_HEADER_SIZE];
        b[0..4].copy_from_slice(&self.pgno.to_be_bytes());
        b[4..6].copy_from_slice(&self.flags.to_be_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Result<PageHeader> {
        if b.len() < PAGE_HEADER_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short page header buffer",
            )));
        }
        Ok(PageHeader {
            pgno: u32::from_be_bytes(b[0..4].try_into().unwrap()),
            flags: u16::from_be_bytes(b[4..6].try_into().unwrap()),
        })
    }
}

/// Metadata about an LTX file, as derived from storage listings. (ltx.go:572)
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileInfo {
    pub level: u8,
    pub min_txid: Txid,
    pub max_txid: Txid,
    pub pre_apply_checksum: Checksum,
    pub post_apply_checksum: Checksum,
    pub size: u64,
    pub created_at: Option<SystemTime>,
}

impl FileInfo {
    /// The database position required before applying this file. (ltx.go:583)
    pub fn pre_apply_pos(&self) -> Pos {
        Pos {
            txid: Txid(self.min_txid.0.wrapping_sub(1)),
            post_apply_checksum: self.pre_apply_checksum,
        }
    }

    /// The database position after applying this file. (ltx.go:591)
    pub fn pos(&self) -> Pos {
        Pos {
            txid: self.max_txid,
            post_apply_checksum: self.post_apply_checksum,
        }
    }

    pub fn filename(&self) -> String {
        format_filename(self.min_txid, self.max_txid)
    }
}

// Compile-time guard that CHECKSUM_FLAG stays in sync with the checksum module.
const _: () = assert!(CHECKSUM_FLAG == 1 << 63);
const _: () = assert!(VERSION == 3);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_roundtrip() {
        let name = format_filename(Txid(0xd), Txid(0x1_0000));
        assert_eq!(name, "000000000000000d-0000000000010000.ltx");
        assert_eq!(parse_filename(&name), Some((Txid(0xd), Txid(0x1_0000))));
        assert_eq!(parse_filename("000000000000000d-000000000001000.ltx"), None);
        assert_eq!(parse_filename("000000000000000D-0000000000010000.ltx"), None); // uppercase rejected
        assert_eq!(parse_filename("junk"), None);
    }

    #[test]
    fn lock_pgno_values() {
        assert_eq!(lock_pgno(512), 2097153);
        assert_eq!(lock_pgno(4096), 262145);
        assert_eq!(lock_pgno(65536), 16385);
    }

    #[test]
    fn page_size_validation() {
        for sz in [512u32, 1024, 4096, 65536] {
            assert!(is_valid_page_size(sz), "{sz}");
        }
        for sz in [0u32, 256, 513, 4097, 131072] {
            assert!(!is_valid_page_size(sz), "{sz}");
        }
    }

    #[test]
    fn contiguity() {
        // min <= prev+1 && max > prev
        assert!(is_contiguous(Txid(5), Txid(6), Txid(9)));
        assert!(is_contiguous(Txid(5), Txid(3), Txid(9))); // overlap that extends
        assert!(!is_contiguous(Txid(5), Txid(7), Txid(9))); // gap
        assert!(!is_contiguous(Txid(5), Txid(3), Txid(5))); // fully covered

        // Extreme values must wrap like Go, not panic in debug builds.
        assert!(!is_contiguous(Txid(u64::MAX), Txid(1), Txid(2)));
        assert_eq!(
            Header { min_txid: Txid(0), ..Default::default() }.pre_apply_pos().txid,
            Txid(u64::MAX)
        );
    }

    #[test]
    fn hex_parse_rejects_sign_prefix() {
        // Go's ParseUint rejects a leading '+'; from_str_radix would not.
        assert_eq!(Txid::parse("+00000000000000f"), None);
        assert_eq!(crate::Checksum::parse("+00000000000000f"), None);
        assert!(Pos::parse("0000000000000001/+00000000000000f").is_none());
        // Both sides accept uppercase hex in string parsing.
        assert_eq!(Txid::parse("000000000000000F"), Some(Txid(0xf)));
    }

    #[test]
    fn header_roundtrip() {
        let h = Header {
            flags: HEADER_FLAG_NO_CHECKSUM,
            page_size: 4096,
            commit: 7,
            min_txid: Txid(2),
            max_txid: Txid(2),
            timestamp: 1_720_000_000_123,
            pre_apply_checksum: Checksum(0),
            wal_offset: 32,
            wal_size: 4120,
            wal_salt1: 0xdead,
            wal_salt2: 0xbeef,
            node_id: 0,
        };
        h.validate().unwrap();
        let b = h.encode();
        assert_eq!(Header::decode(&b).unwrap(), h);
        assert_eq!(&b[80..], &[0u8; 20]);
    }

    #[test]
    fn header_validation_rules() {
        let base = Header {
            flags: 0,
            page_size: 4096,
            commit: 1,
            min_txid: Txid(1),
            max_txid: Txid(1),
            ..Default::default()
        };
        base.validate().unwrap();

        // Unknown flag bit rejected.
        assert!(Header { flags: 1, ..base }.validate().is_err());
        // Snapshot with pre-apply checksum rejected.
        assert!(Header { pre_apply_checksum: Checksum(CHECKSUM_FLAG | 1), ..base }.validate().is_err());
        // Non-snapshot without checksum (and no flag) rejected.
        assert!(Header { min_txid: Txid(2), max_txid: Txid(2), ..base }.validate().is_err());
        // Non-snapshot + NoChecksum flag + zero checksum accepted.
        Header {
            flags: HEADER_FLAG_NO_CHECKSUM,
            min_txid: Txid(2),
            max_txid: Txid(2),
            ..base
        }
        .validate()
        .unwrap();
        // Salt without WAL offset rejected.
        assert!(Header { wal_salt1: 1, ..base }.validate().is_err());
        // WAL size without offset rejected.
        assert!(Header { wal_size: 10, ..base }.validate().is_err());
        // TXIDs out of order rejected.
        assert!(Header { min_txid: Txid(3), max_txid: Txid(2), ..base }.validate().is_err());
    }

    #[test]
    fn pos_roundtrip() {
        let p = Pos::new(Txid(42), Checksum(CHECKSUM_FLAG | 7));
        let s = p.to_string();
        assert_eq!(s.len(), 33);
        assert_eq!(Pos::parse(&s), Some(p));
    }
}
