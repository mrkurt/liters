//! SQLite main database file header parsing — just the fields litestream
//! touches. (docs/SQLITE_INTERNALS.md; replica.go:747-755, 907-910)

/// Byte offsets within the 100-byte SQLite database header that liters reads
/// or patches.
pub mod offsets {
    /// u16 BE; the value 1 means 65536. (offset 16)
    pub const PAGE_SIZE: usize = 16;
    /// File-format write version: 1 = rollback journal, 2 = WAL. (offset 18)
    pub const WRITE_VERSION: usize = 18;
    /// File-format read version. (offset 19)
    pub const READ_VERSION: usize = 19;
    /// u32 BE file change counter; randomized by the reader-side applier to
    /// invalidate other connections' caches. (offset 24)
    pub const CHANGE_COUNTER: usize = 24;
    /// u32 BE in-header database size in pages; trusted by SQLite only when
    /// the change counter matches version-valid-for. (offset 28)
    pub const DB_SIZE_PAGES: usize = 28;
    /// u32 BE change-counter value at which offset 96 was stored. (offset 92)
    pub const VERSION_VALID_FOR: usize = 92;
}

const MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Parsed fields of a SQLite database header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DbHeader {
    pub page_size: u32,
    /// In-header database size in pages. Only trustworthy on a cleanly
    /// checkpointed database; prefer `file_size / page_size` on live files.
    pub page_count: u32,
    pub write_version: u8,
    pub read_version: u8,
    pub change_counter: u32,
}

impl DbHeader {
    /// Parses the first 100 bytes of a database file.
    pub fn parse(b: &[u8]) -> Option<DbHeader> {
        if b.len() < 100 || &b[..16] != MAGIC {
            return None;
        }
        let mut page_size =
            u32::from(u16::from_be_bytes([b[offsets::PAGE_SIZE], b[offsets::PAGE_SIZE + 1]]));
        if page_size == 1 {
            page_size = 65536;
        }
        Some(DbHeader {
            page_size,
            page_count: u32::from_be_bytes(
                b[offsets::DB_SIZE_PAGES..offsets::DB_SIZE_PAGES + 4].try_into().unwrap(),
            ),
            write_version: b[offsets::WRITE_VERSION],
            read_version: b[offsets::READ_VERSION],
            change_counter: u32::from_be_bytes(
                b[offsets::CHANGE_COUNTER..offsets::CHANGE_COUNTER + 4].try_into().unwrap(),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rejects_garbage() {
        assert!(DbHeader::parse(&[0u8; 100]).is_none());
        assert!(DbHeader::parse(b"short").is_none());
    }

    #[test]
    fn parse_page_size_one_means_64k() {
        let mut b = vec![0u8; 100];
        b[..16].copy_from_slice(MAGIC);
        b[16] = 0;
        b[17] = 1; // page size sentinel
        let h = DbHeader::parse(&b).unwrap();
        assert_eq!(h.page_size, 65536);
    }
}
