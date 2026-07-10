//! Streaming LTX encoder, mirroring Go's `ltx.Encoder`. (encoder.go)

use std::collections::BTreeMap;
use std::io::Write;

use crc::Digest;

use crate::checksum::CRC64;
use crate::page_index;
use crate::{
    lock_pgno, lz4f, Checksum, Error, Header, PageHeader, Pos, Result, Trailer, CHECKSUM_FLAG,
    TRAILER_CHECKSUM_OFFSET,
};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum State {
    Header,
    Page,
    Closed,
}

/// Encodes an LTX file to an `io::Write`.
///
/// Call order: [`Encoder::encode_header`], then [`Encoder::encode_page`] for
/// each page (ascending pgno; snapshots must cover every page), then
/// optionally [`Encoder::set_post_apply_checksum`], then [`Encoder::finish`].
pub struct Encoder<W: Write> {
    w: W,
    state: State,
    header: Header,
    trailer: Trailer,
    hash: Digest<'static, u64>,
    /// pgno → (absolute offset of page frame, frame size incl. 6-byte header).
    index: BTreeMap<u32, (u64, u64)>,
    /// Total bytes written (compressed page data counted at compressed size).
    n: u64,
    prev_pgno: u32,
    pages_written: u32,
    frame_buf: Vec<u8>,
}

impl<W: Write> Encoder<W> {
    pub fn new(w: W) -> Encoder<W> {
        Encoder {
            w,
            state: State::Header,
            header: Header::default(),
            trailer: Trailer::default(),
            hash: CRC64.digest(),
            index: BTreeMap::new(),
            n: 0,
            prev_pgno: 0,
            pages_written: 0,
            frame_buf: Vec::new(),
        }
    }

    /// Number of bytes written so far.
    pub fn n(&self) -> u64 {
        self.n
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Trailer with checksums; valid after [`Encoder::finish`].
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

    /// Sets the post-apply rolling checksum. Must be called before `finish`
    /// when checksum tracking is enabled. (encoder.go:75)
    pub fn set_post_apply_checksum(&mut self, chksum: Checksum) {
        self.trailer.post_apply_checksum = chksum;
    }

    /// Validates and writes the file header. (encoder.go:177)
    pub fn encode_header(&mut self, hdr: Header) -> Result<()> {
        match self.state {
            State::Closed => return Err(Error::invalid("ltx encoder closed")),
            State::Page => return Err(Error::invalid("cannot encode header frame, expected page")),
            State::Header => {}
        }
        hdr.validate()?;
        self.header = hdr;
        let b = hdr.encode();
        self.write(&b)?;
        self.state = State::Page;
        Ok(())
    }

    /// Writes one page frame. Pages must be `page_size` bytes and appear in
    /// ascending pgno order; snapshot files must cover pages `1..=commit`
    /// sequentially, skipping exactly the lock page. (encoder.go:206-267)
    pub fn encode_page(&mut self, pgno: u32, data: &[u8]) -> Result<()> {
        match self.state {
            State::Closed => return Err(Error::invalid("ltx encoder closed")),
            State::Header => return Err(Error::invalid("cannot encode page header, expected header")),
            State::Page => {}
        }
        if pgno > self.header.commit {
            return Err(Error::invalid(format!(
                "page number {pgno} out-of-bounds for commit size {}",
                self.header.commit
            )));
        }
        let hdr = PageHeader { pgno, flags: 0 };
        hdr.validate()?;
        if data.len() != self.header.page_size as usize {
            return Err(Error::invalid(format!(
                "invalid page buffer size: {}, expecting {}",
                data.len(),
                self.header.page_size
            )));
        }

        let lock_pgno = lock_pgno(self.header.page_size);
        if pgno == lock_pgno {
            return Err(Error::invalid(format!("cannot encode lock page: pgno={pgno}")));
        }

        if self.header.is_snapshot() {
            if self.prev_pgno == 0 && pgno != 1 {
                return Err(Error::invalid(
                    "snapshot transaction file must start with page number 1",
                ));
            }
            if self.prev_pgno == lock_pgno - 1 {
                if pgno != self.prev_pgno + 2 {
                    // skip lock page
                    return Err(Error::invalid(format!(
                        "nonsequential page numbers in snapshot transaction (skip lock page): {},{pgno}",
                        self.prev_pgno
                    )));
                }
            } else if self.prev_pgno != 0 && pgno != self.prev_pgno + 1 {
                return Err(Error::invalid(format!(
                    "nonsequential page numbers in snapshot transaction: {},{pgno}",
                    self.prev_pgno
                )));
            }
        } else if self.prev_pgno >= pgno {
            return Err(Error::invalid(format!(
                "out-of-order page numbers: {},{pgno}",
                self.prev_pgno
            )));
        }

        let offset = self.n;

        self.write(&hdr.encode())?;

        // Compress the page as a standalone LZ4 frame. The file hash covers
        // the *uncompressed* bytes; the byte counter advances by the
        // compressed size. (encoder.go:278-302)
        self.frame_buf.clear();
        lz4f::compress_frame(data, &mut self.frame_buf);
        self.w.write_all(&self.frame_buf)?;
        self.hash.update(data);
        self.n += self.frame_buf.len() as u64;

        self.pages_written += 1;
        self.prev_pgno = pgno;
        self.index.insert(pgno, (offset, self.n - offset));

        Ok(())
    }

    /// Writes the end-of-pages marker, page index, and trailer, and returns
    /// the writer. (encoder.go:80-135)
    pub fn finish(mut self) -> Result<(W, Header, Trailer)> {
        match self.state {
            State::Closed => return Err(Error::invalid("ltx encoder closed")),
            State::Header => return Err(Error::invalid("cannot close, expected header")),
            State::Page => {}
        }

        // End-of-pages marker: an all-zero page header.
        self.write(&PageHeader::default().encode())?;

        // Page index (elements + end marker), then its size as u64 BE — all
        // hashed and counted.
        let mut index_bytes = Vec::new();
        page_index::encode_page_index(&self.index, &mut index_bytes);
        self.write(&index_bytes)?;

        // Hash the first 8 trailer bytes (post-apply checksum), then compute
        // the file checksum over everything hashed so far.
        let trailer_prefix = self.trailer.post_apply_checksum.0.to_be_bytes();
        debug_assert_eq!(trailer_prefix.len(), TRAILER_CHECKSUM_OFFSET);
        self.hash.update(&trailer_prefix);
        self.n += trailer_prefix.len() as u64;
        let hash = std::mem::replace(&mut self.hash, CRC64.digest());
        self.trailer.file_checksum = Checksum(CHECKSUM_FLAG | hash.finalize());

        self.trailer.validate(&self.header)?;

        // Deletion files must carry the empty checksum. (encoder.go:119)
        if self.header.commit == 0 && self.trailer.post_apply_checksum != Checksum::EMPTY {
            return Err(Error::invalid(
                "post-apply checksum must be empty for zero-length database",
            ));
        }

        self.w.write_all(&self.trailer.encode())?;
        self.n += (crate::TRAILER_SIZE - TRAILER_CHECKSUM_OFFSET) as u64;
        self.w.flush()?;

        self.state = State::Closed;
        Ok((self.w, self.header, self.trailer))
    }

    /// Writes raw bytes, feeding the file hash and byte counter.
    fn write(&mut self, b: &[u8]) -> Result<()> {
        self.w.write_all(b)?;
        self.hash.update(b);
        self.n += b.len() as u64;
        Ok(())
    }
}
