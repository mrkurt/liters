//! The pre-sync `verify()` decision tree: determines where in the WAL to
//! resume copying, or that a full snapshot is required. A faithful port of
//! db.go:1499-1704 — every branch here is load-bearing for correctness.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use liters_wal::{ReadAt, WalReader, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};
use ltx::{Decoder, Txid};

use crate::meta::MetaDir;
use crate::{Error, Result};

/// Outcome of verification: where to resume, with what salts, and whether a
/// full snapshot is required. (db.go:1706-1713)
#[derive(Debug, Clone, Copy)]
pub struct SyncInfo {
    /// WAL byte offset where copying resumes.
    pub offset: u64,
    pub salt1: u32,
    pub salt2: u32,
    /// True if a full database snapshot must be written.
    pub snapshotting: bool,
    /// Human-readable reason when snapshotting.
    pub reason: Option<&'static str>,
    /// Clear the synced-to-WAL-end flag (expected-truncation path, issue #927).
    pub clear_synced_to_wal_end: bool,
}

/// Persistent-ish sync state carried between pushes. (db.go's syncState)
#[derive(Debug, Clone, Copy, Default)]
pub struct SyncState {
    /// Logical end of WAL content at last sync (LTX WALOffset+WALSize) —
    /// used for checkpoint thresholds instead of file size (issue #997).
    pub last_synced_wal_offset: u64,
    /// Whether the last sync consumed the WAL exactly to its end — makes a
    /// subsequent truncation "expected" (issue #927).
    pub synced_to_wal_end: bool,
    /// Whether any data has been synced since the last checkpoint (issue #896).
    pub synced_since_checkpoint: bool,
}

/// Reads the 32-byte WAL header. (litestream.go readWALHeader)
pub fn read_wal_header(wal_path: &Path) -> Result<[u8; 32]> {
    let f = File::open(wal_path)?;
    let mut hdr = [0u8; 32];
    let n = f.read_at(&mut hdr, 0)?;
    if n < 32 {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "short wal header",
        )));
    }
    Ok(hdr)
}

fn wal_file_size(wal_path: &Path) -> Result<u64> {
    match std::fs::metadata(wal_path) {
        Ok(m) => Ok(m.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e.into()),
    }
}

/// The verify decision tree. (db.go:1499-1633)
pub fn verify(
    meta: &MetaDir,
    wal_path: &Path,
    page_size: u32,
    pos_txid: Txid,
    state: &SyncState,
) -> Result<SyncInfo> {
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size as u64;
    let mut info = SyncInfo {
        offset: 0,
        salt1: 0,
        salt2: 0,
        snapshotting: true,
        reason: None,
        clear_synced_to_wal_end: false,
    };

    // First sync ever: snapshot from the top of the WAL. (db.go:1503)
    if pos_txid.is_zero() {
        info.offset = WAL_HEADER_SIZE;
        return Ok(info);
    }

    // Resume point comes from the newest local L0 file's header. (db.go:1509-1524)
    let ltx_path = meta.l0_path(pos_txid);
    let ltx_file = File::open(&ltx_path).map_err(|e| Error::LocalLtx {
        txid: pos_txid,
        msg: format!("open {ltx_path:?}: {e}"),
    })?;
    let mut dec = Decoder::new(BufReader::new(ltx_file));
    dec.decode_header().map_err(|e| Error::LocalLtx {
        txid: pos_txid,
        msg: format!("decode {ltx_path:?}: {e}"),
    })?;
    let ltx_hdr = *dec.header();
    info.offset = (ltx_hdr.wal_offset + ltx_hdr.wal_size) as u64;
    info.salt1 = ltx_hdr.wal_salt1;
    info.salt2 = ltx_hdr.wal_salt2;

    // WAL shorter than our resume point: it was truncated. (db.go:1526-1556)
    let wal_size = wal_file_size(wal_path)?;
    if info.offset > wal_size {
        if state.synced_to_wal_end {
            // Expected checkpoint truncation (issue #927): restart from the
            // new WAL header without snapshotting.
            let hdr = read_wal_header(wal_path)?;
            info.offset = WAL_HEADER_SIZE;
            info.salt1 = u32::from_be_bytes(hdr[16..20].try_into().unwrap());
            info.salt2 = u32::from_be_bytes(hdr[20..24].try_into().unwrap());
            info.snapshotting = false;
            info.reason = None;
            info.clear_synced_to_wal_end = true;
            return Ok(info);
        }
        info.reason = Some("wal truncated by another process");
        return Ok(info);
    }

    // Compare live WAL header salts to the LTX's. (db.go:1558-1565)
    let hdr0 = read_wal_header(wal_path)?;
    let salt1 = u32::from_be_bytes(hdr0[16..20].try_into().unwrap());
    let salt2 = u32::from_be_bytes(hdr0[20..24].try_into().unwrap());
    let salt_match = salt1 == ltx_hdr.wal_salt1 && salt2 == ltx_hdr.wal_salt2;

    // Edge: resume point is the WAL header itself (WALSize=0) — avoid the
    // prev-frame underflow (issue #900). (db.go:1567-1580)
    if info.offset == WAL_HEADER_SIZE {
        if salt_match {
            info.snapshotting = false;
            return Ok(info);
        }
        info.reason = Some("wal header salt reset, snapshotting");
        return Ok(info);
    }

    // Edge: exactly one frame synced ever. (db.go:1582-1596)
    let prev_wal_offset = info.offset - frame_size;
    if prev_wal_offset == WAL_HEADER_SIZE {
        if salt_match {
            info.snapshotting = false;
            return Ok(info);
        }
        info.reason = Some("wal header salt reset, snapshotting");
        return Ok(info);
    } else if prev_wal_offset < WAL_HEADER_SIZE {
        return Err(Error::Other(format!(
            "prev WAL offset is less than the header size: {prev_wal_offset}"
        )));
    }

    // The frame we last synced must still exist, with matching salts and
    // byte-identical page content in the newest L0 file. (db.go:1598-1605)
    if !last_page_match(&mut dec, &ltx_hdr, wal_path, prev_wal_offset, frame_size)? {
        info.reason =
            Some("last page does not exist in last ltx file, wal overwritten by another process");
        return Ok(info);
    }

    // Salt changed but the last page checks out: the WAL restarted under us
    // (e.g. an app checkpoint). Resume from the top with the new salts unless
    // more than one unseen generation cycled through (frames were missed).
    // (db.go:1609-1628)
    if !salt_match {
        info.offset = WAL_HEADER_SIZE;
        info.salt1 = salt1;
        info.salt2 = salt2;

        if detect_full_checkpoint(
            wal_path,
            &[(salt1, salt2), (ltx_hdr.wal_salt1, ltx_hdr.wal_salt2)],
        )? {
            info.reason = Some("full or restart checkpoint detected, snapshotting");
        } else {
            info.snapshotting = false;
        }
        return Ok(info);
    }

    info.snapshotting = false;
    Ok(info)
}

/// Checks that the frame at `prev_wal_offset` carries the expected salts and
/// that its (pgno, page bytes) appear in the newest L0 file. `dec` is
/// positioned after its header; this scans its pages. (db.go:1635-1672)
fn last_page_match(
    dec: &mut Decoder<BufReader<File>>,
    ltx_hdr: &ltx::Header,
    wal_path: &Path,
    prev_wal_offset: u64,
    frame_size: u64,
) -> Result<bool> {
    if prev_wal_offset <= WAL_HEADER_SIZE {
        return Ok(false);
    }

    // Read the raw frame from the live WAL.
    let f = File::open(wal_path)?;
    let mut frame = vec![0u8; frame_size as usize];
    let n = f.read_at(&mut frame, prev_wal_offset)?;
    if n < frame.len() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "cannot read last synced wal page",
        )));
    }
    let pgno = u32::from_be_bytes(frame[0..4].try_into().unwrap());
    let fsalt1 = u32::from_be_bytes(frame[8..12].try_into().unwrap());
    let fsalt2 = u32::from_be_bytes(frame[12..16].try_into().unwrap());
    let data = &frame[WAL_FRAME_HEADER_SIZE as usize..];

    if fsalt1 != ltx_hdr.wal_salt1 || fsalt2 != ltx_hdr.wal_salt2 {
        return Ok(false);
    }

    let mut buf = vec![0u8; ltx_hdr.page_size as usize];
    loop {
        match dec.decode_page(&mut buf) {
            Ok(Some(page_hdr)) => {
                if page_hdr.pgno != pgno {
                    continue;
                }
                if buf != data {
                    continue;
                }
                return Ok(true);
            }
            Ok(None) => return Ok(false), // page not found in LTX file
            Err(e) => return Err(e.into()),
        }
    }
}

/// Detects whether a FULL/RESTART checkpoint cycled the WAL more than once
/// since we last read it: any frame salt other than the known pair means
/// frames were missed. (db.go:1674-1704)
fn detect_full_checkpoint(wal_path: &Path, known_salts: &[(u32, u32)]) -> Result<bool> {
    let f = File::open(wal_path)?;
    let last_known = known_salts.last().copied().unwrap_or((0, 0));

    let rd = WalReader::new(&f)?;
    let mut salts = rd.frame_salts_until(last_known)?;
    for salt in known_salts {
        salts.remove(salt);
    }
    Ok(!salts.is_empty())
}
