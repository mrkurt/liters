use std::fmt;

use crc::{Crc, CRC_64_GO_ISO};

/// CRC-64 with Go's `crc64.ISO` table — the hash used for every LTX checksum.
/// Catalog name CRC-64/GO-ISO: poly 0x1B (reflected 0xD800000000000000),
/// init/xorout 0xFFFFFFFFFFFFFFFF, refin/refout, check("123456789") =
/// 0xB90956C775A41001. (checksum.go:178)
pub(crate) static CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_GO_ISO);

/// Flag OR'd into every stored checksum to guarantee it is non-zero. (ltx.go:55)
pub const CHECKSUM_FLAG: u64 = 1 << 63;

/// An LTX checksum: a CRC-64/GO-ISO value with bit 63 forced on.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Checksum(pub u64);

impl Checksum {
    /// The "empty database" checksum (`CHECKSUM_FLAG` alone). Required as the
    /// post-apply checksum of deletion files. (encoder.go:119)
    pub const EMPTY: Checksum = Checksum(CHECKSUM_FLAG);

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// True if bit 63 is set — the well-formedness requirement for any
    /// non-zero stored checksum.
    pub fn is_flagged(self) -> bool {
        self.0 & CHECKSUM_FLAG != 0
    }

    /// Parses a 16-character hex string. (checksum.go:135)
    pub fn parse(s: &str) -> Option<Checksum> {
        // Explicit digit check: from_str_radix would accept a leading '+',
        // which Go's ParseUint rejects.
        if s.len() != 16 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        u64::from_str_radix(s, 16).ok().map(Checksum)
    }
}

impl fmt::Display for Checksum {
    /// Fixed-width hex, mirroring Go's `%016x`. (checksum.go:148)
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl fmt::Debug for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Checksum({:016x})", self.0)
    }
}

/// Returns the per-page checksum: `CHECKSUM_FLAG | crc64(be32(pgno) ‖ data)`.
/// (checksum.go:106-116)
pub fn checksum_page(pgno: u32, data: &[u8]) -> Checksum {
    let mut digest = CRC64.digest();
    digest.update(&pgno.to_be_bytes());
    digest.update(data);
    Checksum(CHECKSUM_FLAG | digest.finalize())
}

/// Folds a per-page checksum into a rolling database checksum:
/// `CHECKSUM_FLAG | (db ^ page)`. XOR makes the rolling checksum
/// order-independent and incrementally maintainable — fold the same page
/// checksum in again to remove it. (checksum.go:119-132, decoder.go:191)
pub fn xor_page_checksum(db: Checksum, page: Checksum) -> Checksum {
    Checksum(CHECKSUM_FLAG | (db.0 ^ page.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_go_iso_check_value() {
        // The catalog check value for CRC-64/GO-ISO; proves we picked the
        // polynomial variant matching Go's hash/crc64 ISO table.
        assert_eq!(CRC64.checksum(b"123456789"), 0xB909_56C7_75A4_1001);
    }

    #[test]
    fn page_checksum_is_flagged_and_nonzero() {
        let c = checksum_page(1, &[0u8; 512]);
        assert!(c.is_flagged());
        assert!(!c.is_zero());
    }

    #[test]
    fn rolling_checksum_is_incremental() {
        // Folding a page in twice removes it.
        let p1 = checksum_page(1, b"a");
        let p2 = checksum_page(2, b"b");
        let db = xor_page_checksum(xor_page_checksum(Checksum(0), p1), p2);
        let removed = xor_page_checksum(db, p2);
        assert_eq!(removed, xor_page_checksum(Checksum(0), p1));
    }

    #[test]
    fn display_roundtrip() {
        let c = Checksum(CHECKSUM_FLAG | 0x1234);
        assert_eq!(Checksum::parse(&c.to_string()), Some(c));
        assert_eq!(c.to_string().len(), 16);
    }
}
