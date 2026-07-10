//! Minimal LZ4 *frame* format codec with exact byte-boundary semantics.
//!
//! Each LTX page is stored as one standalone LZ4 frame (Go: pierrec/lz4 v4
//! writer with 64KB blocks, fast level, content checksum on, block
//! independence on — encoder.go:43-49). The Go decoder requires each frame to
//! decode to exactly one page and to end exactly at the frame boundary
//! (decoder.go:176-185), so we parse the frame structure ourselves instead of
//! using a buffered frame reader that could over-read into the next page
//! header. Block bodies are (de)compressed with `lz4_flex`'s raw block codec.
//!
//! Compressed output is spec-compliant but not guaranteed byte-identical to
//! pierrec's (LZ4 encoders may differ); LTX file checksums cover the
//! *uncompressed* page bytes, so this does not affect wire compatibility.

use std::io::Read;

use twox_hash::XxHash32;

use crate::{Error, Result};

const FRAME_MAGIC: u32 = 0x184D_2204;

/// FLG byte: version=01, block-independence=1, block-checksum=0,
/// content-size=0, content-checksum=1, dictID=0 — pierrec/lz4 defaults with
/// litestream's options.
const FLG: u8 = 0b0110_0100;

/// BD byte: max block size = 64KB.
const BD: u8 = 0x40;

fn xxh32(data: &[u8]) -> u32 {
    XxHash32::oneshot(0, data)
}

/// Header-checksum byte: second byte of xxh32 over the frame descriptor.
fn header_checksum(desc: &[u8]) -> u8 {
    (xxh32(desc) >> 8) as u8
}

/// Compresses `data` as one complete LZ4 frame, appending to `out`.
pub fn compress_frame(data: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
    out.push(FLG);
    out.push(BD);
    out.push(header_checksum(&[FLG, BD]));

    for block in data.chunks(64 * 1024) {
        let compressed = lz4_flex::block::compress(block);
        if compressed.len() >= block.len() {
            // Incompressible: store raw with the high bit set on the size.
            out.extend_from_slice(&((block.len() as u32) | 0x8000_0000).to_le_bytes());
            out.extend_from_slice(block);
        } else {
            out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
            out.extend_from_slice(&compressed);
        }
    }

    out.extend_from_slice(&0u32.to_le_bytes()); // end mark
    out.extend_from_slice(&xxh32(data).to_le_bytes()); // content checksum
}

fn read_exact_into<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<()> {
    r.read_exact(buf).map_err(Error::Io)
}

fn read_u32_le<R: Read>(r: &mut R) -> Result<u32> {
    let mut b = [0u8; 4];
    read_exact_into(r, &mut b)?;
    Ok(u32::from_le_bytes(b))
}

/// Decompresses exactly one LZ4 frame from `r` into `dst`, consuming exactly
/// the frame's bytes from the reader. Errors unless the frame's uncompressed
/// content is exactly `dst.len()` bytes (mirrors the Go decoder's
/// read-page-then-expect-EOF contract).
pub fn decompress_frame<R: Read>(r: &mut R, dst: &mut [u8]) -> Result<()> {
    if read_u32_le(r)? != FRAME_MAGIC {
        return Err(Error::Lz4("bad frame magic".into()));
    }

    let mut desc = [0u8; 2];
    read_exact_into(r, &mut desc)?;
    let (flg, bd) = (desc[0], desc[1]);

    if flg >> 6 != 0b01 {
        return Err(Error::Lz4(format!("unsupported frame version: FLG=0x{flg:02x}")));
    }
    let block_checksum = flg & 0b0001_0000 != 0;
    let content_size_present = flg & 0b0000_1000 != 0;
    let content_checksum = flg & 0b0000_0100 != 0;
    let dict_id_present = flg & 0b0000_0001 != 0;

    let max_block_size = match bd & 0x70 {
        0x40 => 64 * 1024,
        0x50 => 256 * 1024,
        0x60 => 1024 * 1024,
        0x70 => 4 * 1024 * 1024,
        _ => return Err(Error::Lz4(format!("invalid BD byte: 0x{bd:02x}"))),
    };

    // Optional descriptor fields precede the header checksum.
    let mut desc_full = vec![flg, bd];
    if content_size_present {
        let mut b = [0u8; 8];
        read_exact_into(r, &mut b)?;
        desc_full.extend_from_slice(&b);
    }
    if dict_id_present {
        let mut b = [0u8; 4];
        read_exact_into(r, &mut b)?;
        desc_full.extend_from_slice(&b);
    }
    let mut hc = [0u8; 1];
    read_exact_into(r, &mut hc)?;
    if hc[0] != header_checksum(&desc_full) {
        return Err(Error::Lz4("frame descriptor checksum mismatch".into()));
    }

    let mut filled = 0usize;
    let mut block_buf = Vec::new();
    loop {
        let raw_size = read_u32_le(r)?;
        if raw_size == 0 {
            break; // end mark
        }
        let uncompressed = raw_size & 0x8000_0000 != 0;
        let size = (raw_size & 0x7FFF_FFFF) as usize;
        if size > max_block_size {
            return Err(Error::Lz4(format!("block size {size} exceeds frame max {max_block_size}")));
        }

        block_buf.resize(size, 0);
        read_exact_into(r, &mut block_buf)?;
        if block_checksum {
            let mut b = [0u8; 4];
            read_exact_into(r, &mut b)?;
            if u32::from_le_bytes(b) != xxh32(&block_buf) {
                return Err(Error::Lz4("block checksum mismatch".into()));
            }
        }

        if uncompressed {
            if filled + size > dst.len() {
                return Err(Error::Lz4("frame content longer than expected page size".into()));
            }
            dst[filled..filled + size].copy_from_slice(&block_buf);
            filled += size;
        } else {
            let n = lz4_flex::block::decompress_into(&block_buf, &mut dst[filled..])
                .map_err(|e| Error::Lz4(format!("block decompress: {e}")))?;
            filled += n;
        }
    }

    if content_checksum {
        let mut b = [0u8; 4];
        read_exact_into(r, &mut b)?;
        if u32::from_le_bytes(b) != xxh32(&dst[..filled]) {
            return Err(Error::Lz4("content checksum mismatch".into()));
        }
    }

    if filled != dst.len() {
        return Err(Error::Lz4(format!(
            "frame content length mismatch: got {filled}, expected {}",
            dst.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn roundtrip_compressible() {
        let data = vec![7u8; 4096];
        let mut frame = Vec::new();
        compress_frame(&data, &mut frame);
        assert!(frame.len() < data.len());

        let mut out = vec![0u8; 4096];
        let mut cur = Cursor::new(&frame);
        decompress_frame(&mut cur, &mut out).unwrap();
        assert_eq!(out, data);
        assert_eq!(cur.position() as usize, frame.len()); // consumed exactly the frame
    }

    #[test]
    fn roundtrip_incompressible() {
        // Pseudo-random bytes don't compress; must take the raw-block path.
        let mut data = vec![0u8; 4096];
        let mut state = 0x12345678u32;
        for b in &mut data {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            *b = (state >> 24) as u8;
        }
        let mut frame = Vec::new();
        compress_frame(&data, &mut frame);

        let mut out = vec![0u8; 4096];
        decompress_frame(&mut Cursor::new(&frame), &mut out).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn roundtrip_max_page() {
        let data = vec![3u8; 65536]; // exactly one 64KB block
        let mut frame = Vec::new();
        compress_frame(&data, &mut frame);
        let mut out = vec![0u8; 65536];
        decompress_frame(&mut Cursor::new(&frame), &mut out).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn exact_boundary_with_trailing_data() {
        // Frame followed by other bytes: decoder must not consume past the frame.
        let data = vec![9u8; 512];
        let mut buf = Vec::new();
        compress_frame(&data, &mut buf);
        let frame_len = buf.len();
        buf.extend_from_slice(b"NEXTPAGEHDR");

        let mut out = vec![0u8; 512];
        let mut cur = Cursor::new(&buf);
        decompress_frame(&mut cur, &mut out).unwrap();
        assert_eq!(cur.position() as usize, frame_len);
    }

    #[test]
    fn length_mismatch_rejected() {
        let data = vec![1u8; 512];
        let mut frame = Vec::new();
        compress_frame(&data, &mut frame);
        let mut out = vec![0u8; 1024]; // expecting more than the frame holds
        assert!(decompress_frame(&mut Cursor::new(&frame), &mut out).is_err());
        let mut out = vec![0u8; 256]; // expecting less
        assert!(decompress_frame(&mut Cursor::new(&frame), &mut out).is_err());
    }

    #[test]
    fn corrupt_content_checksum_rejected() {
        let data = vec![5u8; 512];
        let mut frame = Vec::new();
        compress_frame(&data, &mut frame);
        let n = frame.len();
        frame[n - 1] ^= 0xFF;
        let mut out = vec![0u8; 512];
        assert!(decompress_frame(&mut Cursor::new(&frame), &mut out).is_err());
    }
}
