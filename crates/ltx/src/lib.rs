//! Codec for LTX (Liteserver Transaction) files.
//!
//! Byte-compatible with `github.com/superfly/ltx` v0.5.1, the format used by
//! Litestream v0.5.x. The Go source is the authoritative spec; every constant,
//! layout, and validation rule here mirrors it (file/line references point at
//! the Go implementation).
//!
//! File layout:
//!
//! ```text
//! [Header: 100 bytes]
//! [Page frame]*                    6-byte page header + one LZ4 frame per page
//! [End-of-pages marker]            6 zero bytes (an all-zero page header)
//! [Page index]                     uvarint (pgno, offset, size) tuples + uvarint 0
//! [Page index size: u64 BE]        length of the index excluding this field
//! [Trailer: 16 bytes]              post-apply checksum + file checksum
//! ```
//!
//! All multi-byte integers are big-endian except inside LZ4 frames (which are
//! little-endian per the LZ4 frame spec). Checksums are CRC-64/GO-ISO with bit
//! 63 (`CHECKSUM_FLAG`) forced on.

mod checksum;
mod compactor;
mod decoder;
mod encoder;
mod error;
mod lz4f;
mod page_index;
mod types;

pub use checksum::{checksum_page, xor_page_checksum, Checksum, CHECKSUM_FLAG};
pub use compactor::Compactor;
pub use decoder::{decode_page_data, Decoder};
pub use encoder::Encoder;
pub use error::Error;
pub use page_index::{decode_page_index, PageIndexElem};
pub use types::{
    format_filename, is_contiguous, is_valid_header_flags, is_valid_page_size, lock_pgno,
    parse_filename, FileInfo, Header, PageHeader, Pos, Trailer, Txid,
};

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Magic bytes at the start of every LTX file. (ltx.go:20)
pub const MAGIC: &[u8; 4] = b"LTX1";

/// Current LTX format version. Implied by the magic; not stored in the file. (ltx.go:23)
pub const VERSION: u32 = 3;

/// Size of the file header in bytes. (ltx.go:28)
pub const HEADER_SIZE: usize = 100;

/// Size of a page header in bytes. (ltx.go:29)
pub const PAGE_HEADER_SIZE: usize = 6;

/// Size of the file trailer in bytes. (ltx.go:30)
pub const TRAILER_SIZE: usize = 16;

/// Size of a serialized checksum in bytes. (ltx.go:39)
pub const CHECKSUM_SIZE: usize = 8;

/// Offset of the file checksum within the trailer. (ltx.go:40)
pub const TRAILER_CHECKSUM_OFFSET: usize = TRAILER_SIZE - CHECKSUM_SIZE;

/// Header flag: pre/post-apply checksums are not tracked in this file.
/// Litestream v0.5.x sets this on every file it writes. (ltx.go:175)
pub const HEADER_FLAG_NO_CHECKSUM: u32 = 1 << 1;

/// Mask of all valid header flags. (ltx.go:173)
pub const HEADER_FLAG_MASK: u32 = HEADER_FLAG_NO_CHECKSUM;

/// Maximum allowed SQLite page size. (ltx.go:396)
pub const MAX_PAGE_SIZE: u32 = 65536;

/// Byte offset of SQLite's locking region. The page containing this offset
/// (the "lock page") is never stored in an LTX file. (ltx.go:491)
pub const PENDING_BYTE: u64 = 0x4000_0000;
