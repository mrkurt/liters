//! SQLite WAL reading and database-header parsing for liters.
//!
//! Ports litestream's `wal_reader.go` byte-for-byte: salt/checksum-verified
//! frame iteration over a live `-wal` file, committed-transaction page
//! mapping, and salt scanning for checkpoint detection. All conditions that
//! terminate a scan in Go with `io.EOF` (short reads, salt mismatch, checksum
//! mismatch, torn header) are *soft* here too: they end iteration rather than
//! erroring, because a live WAL legitimately contains torn/stale tails.

pub mod db_header;
mod reader;

pub use db_header::DbHeader;
pub use reader::{Frame, PageMap, WalError, WalReader};

/// Size of the WAL file header. (litestream.go:123)
pub const WAL_HEADER_SIZE: u64 = 32;

/// Size of a WAL frame header. (litestream.go:126)
pub const WAL_FRAME_HEADER_SIZE: u64 = 24;

/// WAL magic for little-endian checksum words. (wal_reader.go:101)
pub const WAL_MAGIC_LE: u32 = 0x377F_0682;

/// WAL magic for big-endian checksum words. (wal_reader.go:103)
pub const WAL_MAGIC_BE: u32 = 0x377F_0683;

/// The only supported WAL format version. (wal_reader.go:118)
pub const WAL_VERSION: u32 = 3_007_000;

/// Total WAL size for a page count: `32 + (24 + page_size) * n`. (db.go:1384)
pub fn calc_wal_size(page_size: u32, n: u64) -> u64 {
    WAL_HEADER_SIZE + (WAL_FRAME_HEADER_SIZE + page_size as u64) * n
}

/// Positioned reads, mirroring Go's `io.ReaderAt` as used by the WAL reader.
pub trait ReadAt {
    /// Reads up to `buf.len()` bytes at `offset`, returning the number read.
    /// Fewer bytes than requested (including 0) means end-of-file territory —
    /// never an error.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize>;
}

impl ReadAt for std::fs::File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        // Loop to fill the buffer: pread may return short counts.
        let mut n = 0;
        while n < buf.len() {
            match std::os::unix::fs::FileExt::read_at(self, &mut buf[n..], offset + n as u64) {
                Ok(0) => break,
                Ok(m) => n += m,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(n)
    }
}

impl ReadAt for [u8] {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        let offset = usize::try_from(offset).unwrap_or(usize::MAX);
        if offset >= self.len() {
            return Ok(0);
        }
        let n = buf.len().min(self.len() - offset);
        buf[..n].copy_from_slice(&self[offset..offset + n]);
        Ok(n)
    }
}

impl<T: ReadAt + ?Sized> ReadAt for &T {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        (**self).read_at(buf, offset)
    }
}

/// Computes a running SQLite WAL checksum over `b` (length must be a multiple
/// of 8). The byte order applies to how *input words* are read; stored
/// checksums in headers are always big-endian. (wal_reader.go:273-282)
pub fn wal_checksum(big_endian: bool, mut s0: u32, mut s1: u32, b: &[u8]) -> (u32, u32) {
    debug_assert_eq!(b.len() % 8, 0, "misaligned checksum byte slice");
    for chunk in b.chunks_exact(8) {
        let (w0, w1) = if big_endian {
            (
                u32::from_be_bytes(chunk[0..4].try_into().unwrap()),
                u32::from_be_bytes(chunk[4..8].try_into().unwrap()),
            )
        } else {
            (
                u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
                u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
            )
        };
        s0 = s0.wrapping_add(w0.wrapping_add(s1));
        s1 = s1.wrapping_add(w1.wrapping_add(s0));
    }
    (s0, s1)
}
