//! K-way merge of ordered LTX files into a single file, mirroring Go's
//! `ltx.Compactor`. (compactor.go)
//!
//! Inputs must be ordered oldest→newest with matching page sizes and
//! contiguous (or overlapping-but-extending) TXID ranges. For each page
//! number, the newest input's version wins; pages beyond the final commit
//! size are dropped (truncation). Compacting a chain that starts at a
//! snapshot yields a snapshot.

use std::io::{Read, Write};

use crate::{is_contiguous, Decoder, Encoder, Error, Header, PageHeader, Result, Trailer};

struct Input<R: Read> {
    dec: Decoder<R>,
    /// Buffered frame: header of the pending page, or None if exhausted /
    /// needing refill.
    hdr: Option<PageHeader>,
    data: Vec<u8>,
    done: bool,
}

/// Merges LTX inputs (oldest→newest) into one output file.
pub struct Compactor<R: Read> {
    inputs: Vec<Input<R>>,
    /// Flags for the output header (litestream passes
    /// [`crate::HEADER_FLAG_NO_CHECKSUM`]). (compactor.go:39)
    pub header_flags: u32,
    /// Skip TXID-contiguity validation of inputs (used when rebuilding
    /// snapshots with missing transactions). (compactor.go:44)
    pub allow_non_contiguous_txids: bool,
}

impl<R: Read> Compactor<R> {
    pub fn new(rdrs: Vec<R>) -> Compactor<R> {
        Compactor {
            inputs: rdrs
                .into_iter()
                .map(|r| Input { dec: Decoder::new(r), hdr: None, data: Vec::new(), done: false })
                .collect(),
            header_flags: 0,
            allow_non_contiguous_txids: false,
        }
    }

    /// Runs the merge, writing the compacted LTX file to `w`. Returns the
    /// output header and trailer. (compactor.go:78)
    pub fn compact<W: Write>(mut self, w: W) -> Result<(Header, Trailer)> {
        if self.inputs.is_empty() {
            return Err(Error::invalid("at least one input reader required"));
        }

        for input in &mut self.inputs {
            input.dec.decode_header()?;
        }

        // Validate page-size equality and TXID contiguity. (compactor.go:91)
        for i in 1..self.inputs.len() {
            let prev = *self.inputs[i - 1].dec.header();
            let curr = *self.inputs[i].dec.header();
            if prev.page_size != curr.page_size {
                return Err(Error::invalid(format!(
                    "input files have mismatched page sizes: {} != {}",
                    prev.page_size, curr.page_size
                )));
            }
            if !self.allow_non_contiguous_txids
                && !is_contiguous(prev.max_txid, curr.min_txid, curr.max_txid)
            {
                return Err(Error::invalid(format!(
                    "non-contiguous transaction ids in input files: ({},{}) -> ({},{})",
                    prev.min_txid, prev.max_txid, curr.min_txid, curr.max_txid
                )));
            }
        }

        let min_hdr = *self.inputs[0].dec.header();
        let max_hdr = *self.inputs[self.inputs.len() - 1].dec.header();

        // Output header: page size / MinTXID / pre-apply checksum from the
        // first input; commit / MaxTXID / timestamp from the last. NodeID is
        // dropped. (compactor.go:111)
        let out_hdr = Header {
            flags: self.header_flags,
            page_size: min_hdr.page_size,
            commit: max_hdr.commit,
            min_txid: min_hdr.min_txid,
            max_txid: max_hdr.max_txid,
            timestamp: max_hdr.timestamp,
            pre_apply_checksum: min_hdr.pre_apply_checksum,
            wal_offset: 0,
            wal_size: 0,
            wal_salt1: 0,
            wal_salt2: 0,
            node_id: 0,
        };

        let mut enc = Encoder::new(w);
        enc.encode_header(out_hdr)?;

        for input in &mut self.inputs {
            input.data = vec![0u8; out_hdr.page_size as usize];
        }

        // K-way merge: repeatedly emit the lowest pending page number, taking
        // the newest input's copy. (compactor.go:148-228)
        loop {
            let pgno = self.fill_page_buffers()?;
            if pgno == 0 {
                break;
            }
            self.write_page_buffer(&mut enc, pgno)?;
        }

        // Closing each input decoder verifies its file checksum. (compactor.go:133)
        let mut post_apply = None;
        let n = self.inputs.len();
        for (i, input) in self.inputs.into_iter().enumerate() {
            let (_, _, trailer, _) = input
                .dec
                .finish()
                .map_err(|e| Error::invalid(format!("close reader {i}: {e}")))?;
            if i == n - 1 {
                post_apply = Some(trailer.post_apply_checksum);
            }
        }

        // Output post-apply checksum comes from the last input. (compactor.go:140)
        enc.set_post_apply_checksum(post_apply.unwrap());
        let (_, header, trailer) = enc.finish()?;
        Ok((header, trailer))
    }

    /// Fills each empty input buffer with its next page frame and returns the
    /// lowest pending page number, or 0 when all inputs are exhausted.
    /// (compactor.go:177)
    fn fill_page_buffers(&mut self) -> Result<u32> {
        let mut pgno = 0u32;
        for (i, input) in self.inputs.iter_mut().enumerate() {
            if input.hdr.is_none() && !input.done {
                match input.dec.decode_page(&mut input.data) {
                    Ok(Some(hdr)) => input.hdr = Some(hdr),
                    Ok(None) => input.done = true,
                    Err(e) => return Err(Error::invalid(format!("read page header {i}: {e}"))),
                }
            }
            if let Some(hdr) = &input.hdr {
                if pgno == 0 || hdr.pgno < pgno {
                    pgno = hdr.pgno;
                }
            }
        }
        Ok(pgno)
    }

    /// Emits page `pgno` from the newest input holding it, consuming that
    /// page from every input. Pages beyond the output commit are dropped.
    /// (compactor.go:199)
    fn write_page_buffer<W: Write>(&mut self, enc: &mut Encoder<W>, pgno: u32) -> Result<()> {
        let commit = enc.header().commit;
        let mut page_written = false;
        for input in self.inputs.iter_mut().rev() {
            if input.hdr.map(|h| h.pgno) != Some(pgno) {
                continue;
            }
            let hdr = input.hdr.take().unwrap();
            if page_written || pgno > commit {
                continue;
            }
            page_written = true;
            enc.encode_page(hdr.pgno, &input.data)
                .map_err(|e| Error::invalid(format!("copy page {pgno} header: {e}")))?;
        }
        Ok(())
    }
}
