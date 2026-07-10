//! Ports of litestream's `wal_reader_test.go` cases, run against the same
//! fixture files in reference/litestream/testdata/wal-reader/.

use std::path::PathBuf;

use liters_wal::{WalError, WalReader};

fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../reference/litestream/testdata/wal-reader")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read fixture {path:?}: {e}"))
}

#[test]
fn ok() {
    let b = fixture("ok/wal");
    let mut buf = vec![0u8; 4096];
    let mut r = WalReader::new(b.as_slice()).unwrap();
    assert_eq!(r.page_size(), 4096);
    assert_eq!(r.offset(), 0);

    // First frame: pgno 1, not a commit.
    let f = r.read_frame(&mut buf).unwrap().unwrap();
    assert_eq!((f.pgno, f.commit), (1, 0));
    assert_eq!(&buf[..], &b[56..4152]);
    assert_eq!(r.offset(), 32);

    // Second frame: pgno 2, commit at db size 2.
    let f = r.read_frame(&mut buf).unwrap().unwrap();
    assert_eq!((f.pgno, f.commit), (2, 2));
    assert_eq!(&buf[..], &b[4176..8272]);
    assert_eq!(r.offset(), 4152);

    // Third frame: pgno 2 again, commit 2.
    let f = r.read_frame(&mut buf).unwrap().unwrap();
    assert_eq!((f.pgno, f.commit), (2, 2));
    assert_eq!(&buf[..], &b[8296..12392]);
    assert_eq!(r.offset(), 8272);

    // End of WAL.
    assert!(r.read_frame(&mut buf).unwrap().is_none());
}

#[test]
fn page_map_ok() {
    let b = fixture("ok/wal");
    let mut r = WalReader::new(b.as_slice()).unwrap();
    let pm = r.page_map().unwrap();
    // Frame starts: 32 (pgno 1), 4152 (pgno 2 commit), 8272 (pgno 2 commit).
    assert_eq!(pm.pages.len(), 2);
    assert_eq!(pm.pages[&1], 32);
    assert_eq!(pm.pages[&2], 8272);
    assert_eq!(pm.commit, 2);
    assert_eq!(pm.max_offset, 8272 + 24 + 4096);
}

#[test]
fn salt_mismatch() {
    let b = fixture("salt-mismatch/wal");
    let mut buf = vec![0u8; 4096];
    let mut r = WalReader::new(b.as_slice()).unwrap();
    assert_eq!(r.page_size(), 4096);

    let f = r.read_frame(&mut buf).unwrap().unwrap();
    assert_eq!((f.pgno, f.commit), (1, 0));
    assert_eq!(&buf[..], &b[56..4152]);

    // Second frame's salt was altered: end of valid WAL.
    assert!(r.read_frame(&mut buf).unwrap().is_none());
}

#[test]
fn frame_checksum_mismatch() {
    let b = fixture("frame-checksum-mismatch/wal");
    let mut buf = vec![0u8; 4096];
    let mut r = WalReader::new(b.as_slice()).unwrap();

    let f = r.read_frame(&mut buf).unwrap().unwrap();
    assert_eq!((f.pgno, f.commit), (1, 0));

    // Second frame's checksum was altered: end of valid WAL.
    assert!(r.read_frame(&mut buf).unwrap().is_none());
}

#[test]
fn zero_length_and_partial_header() {
    assert!(matches!(WalReader::new(&[][..]), Err(WalError::EmptyWal)));
    assert!(matches!(WalReader::new(&[0u8; 10][..]), Err(WalError::EmptyWal)));
}

#[test]
fn bad_magic() {
    match WalReader::new(&[0u8; 32][..]) {
        Err(WalError::InvalidMagic(0)) => {}
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn bad_header_checksum_is_empty_wal() {
    let mut data = [0u8; 32];
    data[..4].copy_from_slice(&0x377F_0683u32.to_be_bytes());
    assert!(matches!(WalReader::new(&data[..]), Err(WalError::EmptyWal)));
}

#[test]
fn bad_header_version() {
    // Valid magic + valid header checksum but version 1.
    let mut data = [0u8; 32];
    data[..4].copy_from_slice(&0x377F_0683u32.to_be_bytes());
    data[4..8].copy_from_slice(&1u32.to_be_bytes());
    let (s0, s1) = liters_wal::wal_checksum(true, 0, 0, &data[..24]);
    data[24..28].copy_from_slice(&s0.to_be_bytes());
    data[28..32].copy_from_slice(&s1.to_be_bytes());
    match WalReader::new(&data[..]) {
        Err(WalError::UnsupportedVersion(1)) => {}
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn truncated_frames_end_iteration() {
    let b = fixture("ok/wal");
    let mut buf = vec![0u8; 4096];

    // Partial frame header.
    let mut r = WalReader::new(&b[..40]).unwrap();
    assert!(r.read_frame(&mut buf).unwrap().is_none());

    // Frame header only.
    let mut r = WalReader::new(&b[..56]).unwrap();
    assert!(r.read_frame(&mut buf).unwrap().is_none());

    // Partial frame data.
    let mut r = WalReader::new(&b[..1000]).unwrap();
    assert!(r.read_frame(&mut buf).unwrap().is_none());
}

#[test]
fn frame_salts_until() {
    let b = fixture("frame-salts/wal");
    let r = WalReader::new(b.as_slice()).unwrap();
    let m = r.frame_salts_until((0, 0)).unwrap();
    assert_eq!(m.len(), 3);
    assert!(m.contains(&(0x1b9a294b, 0x37f91916)));
    assert!(m.contains(&(0x1b9a294a, 0x031f195e)));
    assert!(m.contains(&(0x1b9a2949, 0x13b3dd67)));
}

#[test]
fn with_offset_resume() {
    let b = fixture("ok/wal");
    let mut r0 = WalReader::new(b.as_slice()).unwrap();
    let (salt1, salt2) = (r0.salt1, r0.salt2);
    let mut buf = vec![0u8; 4096];
    // Advance past frame 1 to learn the running checksum context.
    r0.read_frame(&mut buf).unwrap().unwrap();

    // Resume at frame 2 (offset 32 + 4120 = 4152).
    let mut r = WalReader::with_offset(b.as_slice(), 4152, salt1, salt2).unwrap();
    let f = r.read_frame(&mut buf).unwrap().unwrap();
    assert_eq!((f.pgno, f.commit), (2, 2));
    assert_eq!(&buf[..], &b[4176..8272]);

    // Offset below/at the header is rejected.
    assert!(matches!(
        WalReader::with_offset(b.as_slice(), 32, salt1, salt2),
        Err(WalError::OffsetTooSmall(32))
    ));
    // Unaligned offset rejected.
    assert!(matches!(
        WalReader::with_offset(b.as_slice(), 4153, salt1, salt2),
        Err(WalError::UnalignedOffset { .. })
    ));
    // Wrong salts: previous frame mismatch.
    assert!(matches!(
        WalReader::with_offset(b.as_slice(), 4152, salt1 ^ 1, salt2),
        Err(WalError::PrevFrameMismatch)
    ));
}
