//! Streaming LTX decoder, mirroring Go's `ltx.Decoder`. (decoder.go)

use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};

use crc::Digest;

use crate::checksum::CRC64;
use crate::page_index::{decode_page_index, PageIndexElem};
use crate::{
    checksum_page, lz4f, xor_page_checksum, Checksum, Error, Header, PageHeader, Pos, Result,
    Trailer, CHECKSUM_FLAG, CHECKSUM_SIZE, HEADER_SIZE, PAGE_HEADER_SIZE, TRAILER_SIZE,
};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum State {
    Header,
    Page,
    Close,
    Closed,
}

/// Decodes an LTX file from an `io::Read`.
///
/// Call order: [`Decoder::decode_header`], then [`Decoder::decode_page`] until
/// it returns `Ok(None)`, then [`Decoder::finish`] (which verifies the file
/// checksum). [`Decoder::verify`] runs the whole sequence, discarding data.
pub struct Decoder<R: Read> {
    r: R,
    state: State,
    header: Header,
    trailer: Trailer,
    page_index: BTreeMap<u32, PageIndexElem>,
    /// Rolling database checksum, tracked for snapshots when enabled.
    chksum: Checksum,
    hash: Digest<'static, u64>,
    page_n: usize,
    n: u64,
}

impl<R: Read> Decoder<R> {
    pub fn new(r: R) -> Decoder<R> {
        Decoder {
            r,
            state: State::Header,
            header: Header::default(),
            trailer: Trailer::default(),
            page_index: BTreeMap::new(),
            chksum: Checksum(0),
            hash: CRC64.digest(),
            page_n: 0,
            n: 0,
        }
    }

    /// Bytes read so far (compressed page data counted at uncompressed size,
    /// mirroring Go's hash-centric counter).
    pub fn n(&self) -> u64 {
        self.n
    }

    pub fn page_n(&self) -> usize {
        self.page_n
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Valid after [`Decoder::finish`].
    pub fn trailer(&self) -> &Trailer {
        &self.trailer
    }

    /// The database position after this file is applied; valid after `finish`.
    pub fn post_apply_pos(&self) -> Pos {
        Pos {
            txid: self.header.max_txid,
            post_apply_checksum: self.trailer.post_apply_checksum,
        }
    }

    /// The page index; populated by `finish`.
    pub fn page_index(&self) -> &BTreeMap<u32, PageIndexElem> {
        &self.page_index
    }

    /// Reads and validates the file header. (decoder.go:120)
    pub fn decode_header(&mut self) -> Result<()> {
        let mut b = [0u8; HEADER_SIZE];
        self.r.read_exact(&mut b)?;
        let header = Header::decode(&b)?;
        self.write_to_hash(&b);
        self.state = State::Page;
        header.validate()?;
        self.header = header;

        // Initialize the rolling checksum if tracking is enabled.
        if !self.header.no_checksum() {
            self.chksum = Checksum(CHECKSUM_FLAG);
        }
        Ok(())
    }

    /// Reads the next page frame into `data` (which must be exactly
    /// `page_size` bytes). Returns `Ok(None)` at the end of the page block.
    /// (decoder.go:144)
    pub fn decode_page(&mut self, data: &mut [u8]) -> Result<Option<PageHeader>> {
        match self.state {
            State::Closed => return Err(Error::invalid("ltx decoder closed")),
            State::Close => return Ok(None),
            State::Header => return Err(Error::invalid("cannot read page header, expected header")),
            State::Page => {}
        }
        if data.len() != self.header.page_size as usize {
            return Err(Error::invalid(format!(
                "invalid page buffer size: {}, expecting {}",
                data.len(),
                self.header.page_size
            )));
        }

        let mut b = [0u8; PAGE_HEADER_SIZE];
        self.r.read_exact(&mut b)?;
        let hdr = PageHeader::decode(&b)?;
        self.write_to_hash(&b);

        // An all-zero page header marks the end of the page block.
        if hdr.is_zero() {
            self.state = State::Close;
            return Ok(None);
        }
        hdr.validate()?;

        // Decompress exactly one LZ4 frame; the file hash covers the
        // uncompressed bytes.
        lz4f::decompress_frame(&mut self.r, data)?;
        self.hash.update(data);
        self.n += data.len() as u64;
        self.page_n += 1;

        // Fold into the rolling snapshot checksum when tracking. (decoder.go:189)
        if self.header.is_snapshot() && !self.header.no_checksum() && hdr.pgno != self.header.lock_pgno()
        {
            self.chksum = xor_page_checksum(self.chksum, checksum_page(hdr.pgno, data));
        }

        Ok(Some(hdr))
    }

    /// Reads the page index and trailer, then verifies the file checksum (and,
    /// for snapshots with checksum tracking, the post-apply checksum).
    /// (decoder.go:68-116)
    pub fn finish(mut self) -> Result<(R, Header, Trailer, BTreeMap<u32, PageIndexElem>)> {
        match self.state {
            State::Closed => return Err(Error::invalid("ltx decoder closed")),
            State::Header | State::Page => {
                return Err(Error::invalid("cannot close, expected end of page block"))
            }
            State::Close => {}
        }

        // Slurp the remainder: page index + trailer.
        let mut remaining = Vec::new();
        self.r.read_to_end(&mut remaining)?;
        if remaining.len() < TRAILER_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short read: missing trailer",
            )));
        }

        // Everything except the trailing file checksum is hashed.
        self.write_to_hash(&remaining[..remaining.len() - CHECKSUM_SIZE]);

        let mut cur = Cursor::new(&remaining[..]);
        self.page_index =
            decode_page_index(&mut cur, 0, self.header.min_txid, self.header.max_txid)?;

        let trailer_off = cur.position() as usize;
        if remaining.len() - trailer_off != TRAILER_SIZE {
            return Err(Error::invalid(format!(
                "unexpected {} trailing bytes after page index",
                remaining.len() - trailer_off
            )));
        }
        self.trailer = Trailer::decode(&remaining[trailer_off..])?;

        let hash = std::mem::replace(&mut self.hash, CRC64.digest());
        let computed = Checksum(CHECKSUM_FLAG | hash.finalize());
        if computed != self.trailer.file_checksum {
            return Err(Error::ChecksumMismatch);
        }

        if self.header.is_snapshot()
            && !self.header.no_checksum()
            && self.trailer.post_apply_checksum != self.chksum
        {
            return Err(Error::PostApplyChecksumMismatch {
                trailer: self.trailer.post_apply_checksum,
                computed: self.chksum,
            });
        }

        self.state = State::Closed;
        Ok((self.r, self.header, self.trailer, std::mem::take(&mut self.page_index)))
    }

    /// Reads and verifies the entire file, returning header/trailer/index.
    /// (decoder.go:200)
    pub fn verify(mut self) -> Result<(Header, Trailer, BTreeMap<u32, PageIndexElem>)> {
        self.decode_header()?;
        let mut data = vec![0u8; self.header.page_size as usize];
        while self.decode_page(&mut data)?.is_some() {}
        let (_, header, trailer, index) = self.finish()?;
        Ok((header, trailer, index))
    }

    /// Materializes a snapshot LTX file as a SQLite database image, writing
    /// pages `1..=commit` in order and zero-filling the lock page.
    /// (decoder.go:223)
    pub fn decode_database_to<W: Write>(mut self, w: &mut W) -> Result<()> {
        self.decode_header()?;
        let hdr = self.header;
        let lock_pgno = hdr.lock_pgno();
        if !hdr.is_snapshot() {
            return Err(Error::invalid(
                "cannot decode non-snapshot LTX file to SQLite database",
            ));
        }

        let mut data = vec![0u8; hdr.page_size as usize];
        for pgno in 1..=hdr.commit {
            if pgno == lock_pgno {
                data.fill(0);
            } else {
                let page_hdr = self.decode_page(&mut data)?.ok_or_else(|| {
                    Error::invalid(format!("unexpected end of page block at page {pgno}"))
                })?;
                if page_hdr.pgno != pgno {
                    return Err(Error::invalid(format!(
                        "unexpected pgno while decoding page: read {}, expected {pgno}",
                        page_hdr.pgno
                    )));
                }
            }
            w.write_all(&data)?;
        }

        // One more read must hit the end-of-pages marker. (decoder.go:258)
        if let Some(page_hdr) = self.decode_page(&mut data)? {
            return Err(Error::invalid(format!(
                "unexpected page {} after commit {}",
                page_hdr.pgno, hdr.commit
            )));
        }

        self.finish()?;
        Ok(())
    }

    fn write_to_hash(&mut self, b: &[u8]) {
        self.hash.update(b);
        self.n += b.len() as u64;
    }
}

/// Decodes a single page frame fetched via the page index: a 6-byte page
/// header followed by one LZ4 frame. `page_size` is the database page size.
/// (decoder.go:296 `DecodePageData`)
pub fn decode_page_data(frame: &[u8], page_size: u32) -> Result<(PageHeader, Vec<u8>)> {
    let hdr = PageHeader::decode(frame)?;
    if hdr.is_zero() {
        return Ok((hdr, Vec::new()));
    }
    let mut data = vec![0u8; page_size as usize];
    let mut cur = Cursor::new(&frame[PAGE_HEADER_SIZE..]);
    lz4f::decompress_frame(&mut cur, &mut data)?;
    Ok((hdr, data))
}
