//! The WAL frame reader, ported from litestream's `wal_reader.go`.

use std::collections::{HashMap, HashSet};

use crate::{
    wal_checksum, ReadAt, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WAL_MAGIC_BE, WAL_MAGIC_LE,
    WAL_VERSION,
};

/// Hard errors from WAL reading. End-of-valid-WAL is *not* an error — frame
/// reads return `Ok(None)` and header parsing distinguishes `EmptyWal`.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// The WAL is missing, shorter than a header, or its header checksum does
    /// not match (torn header write during checkpoint). Treat as "no WAL
    /// content". (wal_reader.go:93-115)
    #[error("empty or torn wal")]
    EmptyWal,

    /// (wal_reader.go:106)
    #[error("invalid wal header magic: {0:x}")]
    InvalidMagic(u32),

    /// (wal_reader.go:119)
    #[error("unsupported wal version: {0}")]
    UnsupportedVersion(u32),

    /// (wal_reader.go:64)
    #[error("unaligned wal offset {offset} for page size {page_size}")]
    UnalignedOffset { offset: u64, page_size: u32 },

    /// (wal_reader.go:48)
    #[error("offset ({0}) must be greater than the wal header size (32)")]
    OffsetTooSmall(u64),

    /// Resuming at an offset failed because the frame before it no longer
    /// matches the expected salts (WAL was rewritten). (wal_reader.go:284)
    #[error("prev frame mismatch")]
    PrevFrameMismatch,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A decoded frame header returned by [`WalReader::read_frame`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    pub pgno: u32,
    /// Database size in pages if this is a commit frame; 0 otherwise.
    pub commit: u32,
}

/// The result of scanning all committed transactions in a WAL.
/// (wal_reader.go:189-244)
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PageMap {
    /// pgno → file offset of the *start* of the latest committed frame for
    /// that page.
    pub pages: HashMap<u32, u64>,
    /// Exclusive end offset of the consumed WAL segment (end of the last
    /// committed frame), or 0 if no committed transactions were found.
    pub max_offset: u64,
    /// Final database size in pages, or 0 if no committed transactions.
    pub commit: u32,
}

/// Verifying reader over a live SQLite `-wal` file. (wal_reader.go:19)
#[derive(Debug)]
pub struct WalReader<R: ReadAt> {
    r: R,
    frame_n: u64,
    big_endian: bool,
    page_size: u32,
    /// Checkpoint sequence number from the header (informational).
    pub seq: u32,
    pub salt1: u32,
    pub salt2: u32,
    chksum1: u32,
    chksum2: u32,
}

impl<R: ReadAt> WalReader<R> {
    /// Opens a reader at the start of the WAL, parsing and verifying the
    /// header. (wal_reader.go:34)
    pub fn new(r: R) -> Result<WalReader<R>, WalError> {
        let mut rd = WalReader {
            r,
            frame_n: 0,
            big_endian: false,
            page_size: 0,
            seq: 0,
            salt1: 0,
            salt2: 0,
            chksum1: 0,
            chksum2: 0,
        };
        rd.read_header()?;
        Ok(rd)
    }

    /// Opens a reader resuming at `offset` with expected salts, seeding the
    /// checksum chain from the frame *before* the offset (whose stored
    /// checksum is trusted after its salts are verified). (wal_reader.go:45)
    pub fn with_offset(r: R, offset: u64, salt1: u32, salt2: u32) -> Result<WalReader<R>, WalError> {
        if offset <= WAL_HEADER_SIZE {
            return Err(WalError::OffsetTooSmall(offset));
        }

        let mut rd = WalReader::new(r)?;

        // Adopt the expected salts in case the start of the file has been
        // overwritten by a newer generation.
        rd.salt1 = salt1;
        rd.salt2 = salt2;

        let frame_size = WAL_FRAME_HEADER_SIZE + rd.page_size as u64;
        if !(offset - WAL_HEADER_SIZE).is_multiple_of(frame_size) {
            return Err(WalError::UnalignedOffset { offset, page_size: rd.page_size });
        }
        rd.frame_n = (offset - WAL_HEADER_SIZE) / frame_size;

        // Read the previous frame without checksum verification to seed the
        // rolling checksum; its salts must match. (wal_reader.go:69)
        rd.frame_n -= 1;
        let mut data = vec![0u8; rd.page_size as usize];
        match rd.read_frame_inner(&mut data, false) {
            Ok(Some(_)) => Ok(rd),
            Ok(None) | Err(_) => Err(WalError::PrevFrameMismatch),
        }
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// File offset of the start of the last-read frame; 0 if none read yet.
    /// (wal_reader.go:82)
    pub fn offset(&self) -> u64 {
        if self.frame_n == 0 {
            return 0;
        }
        WAL_HEADER_SIZE + (self.frame_n - 1) * (WAL_FRAME_HEADER_SIZE + self.page_size as u64)
    }

    /// (wal_reader.go:90-129)
    fn read_header(&mut self) -> Result<(), WalError> {
        let mut hdr = [0u8; WAL_HEADER_SIZE as usize];
        let n = self.r.read_at(&mut hdr, 0)?;
        if n < hdr.len() {
            return Err(WalError::EmptyWal);
        }

        let magic = u32::from_be_bytes(hdr[0..4].try_into().unwrap());
        self.big_endian = match magic {
            WAL_MAGIC_LE => false,
            WAL_MAGIC_BE => true,
            _ => return Err(WalError::InvalidMagic(magic)),
        };

        // A torn header write (mid-checkpoint crash) reads as an empty WAL.
        let chksum1 = u32::from_be_bytes(hdr[24..28].try_into().unwrap());
        let chksum2 = u32::from_be_bytes(hdr[28..32].try_into().unwrap());
        let (v0, v1) = wal_checksum(self.big_endian, 0, 0, &hdr[..24]);
        if v0 != chksum1 || v1 != chksum2 {
            return Err(WalError::EmptyWal);
        }

        let version = u32::from_be_bytes(hdr[4..8].try_into().unwrap());
        if version != WAL_VERSION {
            return Err(WalError::UnsupportedVersion(version));
        }

        self.page_size = u32::from_be_bytes(hdr[8..12].try_into().unwrap());
        self.seq = u32::from_be_bytes(hdr[12..16].try_into().unwrap());
        self.salt1 = u32::from_be_bytes(hdr[16..20].try_into().unwrap());
        self.salt2 = u32::from_be_bytes(hdr[20..24].try_into().unwrap());
        self.chksum1 = chksum1;
        self.chksum2 = chksum2;
        Ok(())
    }

    /// Reads the next frame into `data` (page-size bytes). Returns `Ok(None)`
    /// at the end of the valid WAL: short read, salt mismatch, or cumulative
    /// checksum mismatch. (wal_reader.go:133)
    pub fn read_frame(&mut self, data: &mut [u8]) -> Result<Option<Frame>, WalError> {
        self.read_frame_inner(data, true)
    }

    fn read_frame_inner(
        &mut self,
        data: &mut [u8],
        verify_checksum: bool,
    ) -> Result<Option<Frame>, WalError> {
        assert_eq!(
            data.len(),
            self.page_size as usize,
            "WalReader::read_frame: buffer size ({}) must match page size ({})",
            data.len(),
            self.page_size
        );

        let frame_size = WAL_FRAME_HEADER_SIZE + self.page_size as u64;
        let offset = WAL_HEADER_SIZE + self.frame_n * frame_size;

        let mut hdr = [0u8; WAL_FRAME_HEADER_SIZE as usize];
        if self.r.read_at(&mut hdr, offset)? != hdr.len() {
            return Ok(None);
        }
        if self.r.read_at(data, offset + WAL_FRAME_HEADER_SIZE)? != data.len() {
            return Ok(None);
        }

        // Salts must match the expected generation. (wal_reader.go:161)
        let salt1 = u32::from_be_bytes(hdr[8..12].try_into().unwrap());
        let salt2 = u32::from_be_bytes(hdr[12..16].try_into().unwrap());
        if self.salt1 != salt1 || self.salt2 != salt2 {
            return Ok(None);
        }

        let chksum1 = u32::from_be_bytes(hdr[16..20].try_into().unwrap());
        let chksum2 = u32::from_be_bytes(hdr[20..24].try_into().unwrap());
        if verify_checksum {
            // The cumulative checksum covers frame-header bytes [0,8) (pgno +
            // commit — not the salts) then the page data. (wal_reader.go:172)
            let (s0, s1) = wal_checksum(self.big_endian, self.chksum1, self.chksum2, &hdr[..8]);
            let (s0, s1) = wal_checksum(self.big_endian, s0, s1, data);
            self.chksum1 = s0;
            self.chksum2 = s1;
            if self.chksum1 != chksum1 || self.chksum2 != chksum2 {
                return Ok(None);
            }
        } else {
            // Trust the stored checksum when seeding from a resume offset.
            self.chksum1 = chksum1;
            self.chksum2 = chksum2;
        }

        let frame = Frame {
            pgno: u32::from_be_bytes(hdr[0..4].try_into().unwrap()),
            commit: u32::from_be_bytes(hdr[4..8].try_into().unwrap()),
        };
        self.frame_n += 1;
        Ok(Some(frame))
    }

    /// Scans all remaining frames, returning the latest committed version of
    /// each page. Uncommitted trailing frames are excluded; pages beyond the
    /// final commit size (VACUUM shrink) are dropped. (wal_reader.go:192-244)
    pub fn page_map(&mut self) -> Result<PageMap, WalError> {
        self.page_map_until(u64::MAX)
    }

    /// Like [`WalReader::page_map`], but stops before any frame starting at
    /// or beyond `end_offset`. Used to snapshot a database at an exact
    /// already-synced WAL position while writers keep appending.
    pub fn page_map_until(&mut self, end_offset: u64) -> Result<PageMap, WalError> {
        let mut m: HashMap<u32, u64> = HashMap::new();
        let mut tx_map: HashMap<u32, u64> = HashMap::new();
        let mut commit = 0u32;
        let mut data = vec![0u8; self.page_size as usize];
        let frame_size = WAL_FRAME_HEADER_SIZE + self.page_size as u64;

        loop {
            let next_start = WAL_HEADER_SIZE + self.frame_n * frame_size;
            if next_start >= end_offset {
                break;
            }
            let Some(frame) = self.read_frame(&mut data)? else { break };

            // Pages are not visible until their transaction commits.
            tx_map.insert(frame.pgno, self.offset());

            if frame.commit != 0 {
                for (&pgno, &offset) in &tx_map {
                    m.insert(pgno, offset);
                }
                commit = frame.commit;
            }
        }

        // Drop pages beyond the final database size (shrunk between
        // transactions within this WAL).
        m.retain(|&pgno, _| pgno <= commit);

        if m.is_empty() {
            return Ok(PageMap::default());
        }

        let end = m.values().copied().max().unwrap_or(0)
            + WAL_FRAME_HEADER_SIZE
            + self.page_size as u64;

        Ok(PageMap { pages: m, max_offset: end, commit })
    }

    /// Collects the distinct frame salts from the top of the WAL until (and
    /// including) `until`, without checksum verification. Used to detect
    /// multiple unseen checkpoint generations. (wal_reader.go:247-270)
    pub fn frame_salts_until(&self, until: (u32, u32)) -> Result<HashSet<(u32, u32)>, WalError> {
        let mut m = HashSet::new();
        let frame_size = WAL_FRAME_HEADER_SIZE + self.page_size as u64;
        let mut offset = WAL_HEADER_SIZE;
        loop {
            let mut hdr = [0u8; WAL_FRAME_HEADER_SIZE as usize];
            if self.r.read_at(&mut hdr, offset)? != hdr.len() {
                break;
            }
            let salt1 = u32::from_be_bytes(hdr[8..12].try_into().unwrap());
            let salt2 = u32::from_be_bytes(hdr[12..16].try_into().unwrap());
            m.insert((salt1, salt2));
            if (salt1, salt2) == until {
                break;
            }
            offset += frame_size;
        }
        Ok(m)
    }
}
