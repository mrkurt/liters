//! The page index: a random-access map from page number to the byte span of
//! that page's frame within the LTX file. Written between the end-of-pages
//! marker and the trailer. (encoder.go:137-174, decoder.go:309-346)
//!
//! Wire form:
//!
//! ```text
//! repeat per page (ascending pgno): uvarint(pgno) uvarint(offset) uvarint(size)
//! end marker:                       uvarint(0)
//! index size:                       u64 BE — length of the bytes above
//! ```
//!
//! `offset` is the absolute file offset of the page's 6-byte header; `size`
//! covers the header plus the compressed LZ4 frame.

use std::collections::BTreeMap;
use std::io::Read;

use crate::{Error, Result, Txid};

/// An entry in the page index. `level`/TXIDs are caller-side context (they are
/// not on the wire) identifying which LTX file the span points into.
/// (encoder.go:309-316)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageIndexElem {
    pub level: u8,
    pub min_txid: Txid,
    pub max_txid: Txid,
    /// Absolute byte offset of the page frame (its 6-byte header) in the file.
    pub offset: u64,
    /// Byte length of the frame: header + compressed data.
    pub size: u64,
}

pub(crate) fn append_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

pub(crate) fn read_uvarint<R: Read>(r: &mut R) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let mut b = [0u8; 1];
        r.read_exact(&mut b).map_err(Error::Io)?;
        let byte = b[0];
        if shift == 63 && byte > 1 {
            return Err(Error::invalid("uvarint overflows 64 bits"));
        }
        result |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(Error::invalid("uvarint overflows 64 bits"));
        }
    }
}

/// Serializes the index (sorted by pgno) plus the end marker and trailing
/// size field, appending to `out`.
pub(crate) fn encode_page_index(index: &BTreeMap<u32, (u64, u64)>, out: &mut Vec<u8>) {
    let start = out.len();
    for (&pgno, &(offset, size)) in index {
        append_uvarint(out, u64::from(pgno));
        append_uvarint(out, offset);
        append_uvarint(out, size);
    }
    append_uvarint(out, 0); // end marker
    let index_size = (out.len() - start) as u64;
    out.extend_from_slice(&index_size.to_be_bytes());
}

/// Decodes a page index from `r`, tagging each element with the given
/// level/TXID context. Reads the tuples, the end marker, and the trailing
/// 8-byte size field (mirroring Go, which reads but does not verify it).
/// (decoder.go:310-346)
pub fn decode_page_index<R: Read>(
    r: &mut R,
    level: u8,
    min_txid: Txid,
    max_txid: Txid,
) -> Result<BTreeMap<u32, PageIndexElem>> {
    let mut index = BTreeMap::new();
    loop {
        let pgno = read_uvarint(r)?;
        if pgno == 0 {
            break;
        }
        let offset = read_uvarint(r)?;
        let size = read_uvarint(r)?;
        index.insert(
            pgno as u32,
            PageIndexElem { level, min_txid, max_txid, offset, size },
        );
    }

    // Trailing size field.
    let mut b = [0u8; 8];
    r.read_exact(&mut b).map_err(Error::Io)?;

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn uvarint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, 16383, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            append_uvarint(&mut buf, v);
            assert_eq!(read_uvarint(&mut Cursor::new(&buf)).unwrap(), v, "{v}");
        }
    }

    #[test]
    fn index_roundtrip() {
        let mut index = BTreeMap::new();
        index.insert(1u32, (100u64, 456u64));
        index.insert(7u32, (556u64, 89u64));
        index.insert(300u32, (645u64, 4102u64));

        let mut buf = Vec::new();
        encode_page_index(&index, &mut buf);

        // Size field covers everything before it.
        let size = u64::from_be_bytes(buf[buf.len() - 8..].try_into().unwrap());
        assert_eq!(size as usize, buf.len() - 8);

        let decoded =
            decode_page_index(&mut Cursor::new(&buf), 0, Txid(5), Txid(9)).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[&7].offset, 556);
        assert_eq!(decoded[&7].size, 89);
        assert_eq!(decoded[&7].min_txid, Txid(5));
        assert_eq!(decoded[&300].offset, 645);
    }
}
