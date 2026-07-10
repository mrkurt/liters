//! Pure-Rust property tests: encode→decode roundtrips and compactor
//! equivalence over arbitrary page sets.

use std::collections::BTreeMap;
use std::io::Cursor;

use ltx::{
    checksum_page, xor_page_checksum, Checksum, Compactor, Decoder, Encoder, Header, Txid,
    HEADER_FLAG_NO_CHECKSUM,
};
use proptest::prelude::*;

/// Encodes a non-snapshot NoChecksum file (litestream L0 style) from a sparse
/// page map.
fn encode_l0(page_size: u32, commit: u32, txid: u64, pages: &BTreeMap<u32, Vec<u8>>) -> Vec<u8> {
    let mut enc = Encoder::new(Vec::new());
    enc.encode_header(Header {
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size,
        commit,
        min_txid: Txid(txid),
        max_txid: Txid(txid),
        ..Default::default()
    })
    .unwrap();
    for (&pgno, data) in pages {
        enc.encode_page(pgno, data).unwrap();
    }
    let (buf, _, _) = enc.finish().unwrap();
    buf
}

fn decode_all(bytes: &[u8]) -> (Header, BTreeMap<u32, Vec<u8>>) {
    let mut dec = Decoder::new(Cursor::new(bytes));
    dec.decode_header().unwrap();
    let hdr = *dec.header();
    let mut pages = BTreeMap::new();
    let mut data = vec![0u8; hdr.page_size as usize];
    while let Some(ph) = dec.decode_page(&mut data).unwrap() {
        pages.insert(ph.pgno, data.clone());
    }
    dec.finish().unwrap();
    (hdr, pages)
}

/// Strategy: valid page size + sparse ascending page set with random data.
fn page_set(page_size: u32, max_commit: u32) -> impl Strategy<Value = (u32, BTreeMap<u32, Vec<u8>>)> {
    let lock = ltx::lock_pgno(page_size);
    (1..=max_commit).prop_flat_map(move |commit| {
        let pgnos = proptest::collection::btree_set(1..=commit, 1..=(commit.min(16) as usize));
        pgnos
            .prop_flat_map(move |set| {
                let pgnos: Vec<u32> = set.into_iter().filter(|&p| p != lock).collect();
                let n = pgnos.len();
                (
                    Just(pgnos),
                    proptest::collection::vec(
                        proptest::collection::vec(any::<u8>(), page_size as usize),
                        n,
                    ),
                )
            })
            .prop_map(move |(pgnos, datas)| {
                (commit, pgnos.into_iter().zip(datas).collect::<BTreeMap<_, _>>())
            })
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn l0_roundtrip((commit, pages) in page_set(512, 32)) {
        prop_assume!(!pages.is_empty());
        let bytes = encode_l0(512, commit, 2, &pages);
        let (hdr, decoded) = decode_all(&bytes);
        prop_assert_eq!(hdr.commit, commit);
        prop_assert_eq!(&decoded, &pages);

        // The page index must locate every page frame exactly.
        let dec = Decoder::new(Cursor::new(&bytes));
        let (_, _, index) = dec.verify().unwrap();
        prop_assert_eq!(index.len(), pages.len());
        for (pgno, elem) in &index {
            let frame = &bytes[elem.offset as usize..(elem.offset + elem.size) as usize];
            let (ph, data) = ltx::decode_page_data(frame, 512).unwrap();
            prop_assert_eq!(ph.pgno, *pgno);
            prop_assert_eq!(&data, &pages[pgno]);
        }
    }

    #[test]
    fn snapshot_roundtrip_with_checksums(commit in 1u32..24, seed in any::<u64>()) {
        let page_size = 512u32;
        // Deterministic page content from seed.
        let mut state = seed | 1;
        let mut pages = Vec::new();
        for _ in 0..commit {
            let mut page = vec![0u8; page_size as usize];
            for b in &mut page {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                *b = (state >> 56) as u8;
            }
            pages.push(page);
        }

        let mut enc = Encoder::new(Vec::new());
        enc.encode_header(Header {
            flags: 0,
            page_size,
            commit,
            min_txid: Txid(1),
            max_txid: Txid(1),
            ..Default::default()
        }).unwrap();
        let mut chksum = Checksum(0);
        for (i, page) in pages.iter().enumerate() {
            enc.encode_page((i + 1) as u32, page).unwrap();
            chksum = xor_page_checksum(chksum, checksum_page((i + 1) as u32, page));
        }
        enc.set_post_apply_checksum(chksum);
        let (bytes, _, trailer) = enc.finish().unwrap();
        prop_assert_eq!(trailer.post_apply_checksum, chksum);

        // decode_database_to reproduces the exact image.
        let dec = Decoder::new(Cursor::new(&bytes));
        let mut out = Vec::new();
        dec.decode_database_to(&mut out).unwrap();
        prop_assert_eq!(out, pages.concat());

        // A wrong post-apply checksum must be rejected at decode time.
        let mut enc = Encoder::new(Vec::new());
        enc.encode_header(Header {
            flags: 0,
            page_size,
            commit,
            min_txid: Txid(1),
            max_txid: Txid(1),
            ..Default::default()
        }).unwrap();
        for (i, page) in pages.iter().enumerate() {
            enc.encode_page((i + 1) as u32, page).unwrap();
        }
        enc.set_post_apply_checksum(Checksum(ltx::CHECKSUM_FLAG | 0xdead));
        let (bad, _, _) = enc.finish().unwrap();
        let dec = Decoder::new(Cursor::new(&bad));
        prop_assert!(dec.verify().is_err());
    }

    #[test]
    fn compactor_equals_sequential_apply(
        (commit, base) in page_set(512, 24),
        (_, delta1) in page_set(512, 24),
        (_, delta2) in page_set(512, 24),
    ) {
        prop_assume!(!base.is_empty());
        // Build a full snapshot at TXID 1 covering `commit` pages.
        let page_size = 512u32;
        let mut full = BTreeMap::new();
        for pgno in 1..=commit {
            full.insert(
                pgno,
                base.get(&pgno).cloned().unwrap_or_else(|| vec![pgno as u8; page_size as usize]),
            );
        }
        let mut enc = Encoder::new(Vec::new());
        enc.encode_header(Header {
            flags: HEADER_FLAG_NO_CHECKSUM,
            page_size,
            commit,
            min_txid: Txid(1),
            max_txid: Txid(1),
            ..Default::default()
        }).unwrap();
        for (&pgno, data) in &full {
            enc.encode_page(pgno, data).unwrap();
        }
        let (f1, _, _) = enc.finish().unwrap();

        // Two delta files at TXIDs 2 and 3, restricted to pages <= commit.
        let d1: BTreeMap<u32, Vec<u8>> =
            delta1.into_iter().filter(|(p, _)| *p <= commit).collect();
        let d2: BTreeMap<u32, Vec<u8>> =
            delta2.into_iter().filter(|(p, _)| *p <= commit).collect();
        prop_assume!(!d1.is_empty() && !d2.is_empty());
        let f2 = encode_l0(page_size, commit, 2, &d1);
        let f3 = encode_l0(page_size, commit, 3, &d2);

        // Compact. NoChecksum inputs require NoChecksum output flags, exactly
        // as litestream does (its compactor always sets HeaderFlagNoChecksum).
        let mut compactor = Compactor::new(vec![
            Cursor::new(f1.as_slice()),
            Cursor::new(f2.as_slice()),
            Cursor::new(f3.as_slice()),
        ]);
        compactor.header_flags = HEADER_FLAG_NO_CHECKSUM;
        let mut compacted = Vec::new();
        let (hdr, _) = compactor.compact(&mut compacted).unwrap();
        prop_assert!(hdr.is_snapshot());
        prop_assert_eq!(hdr.max_txid, Txid(3));

        // Sequential apply: newest version of each page wins.
        let mut expected = full.clone();
        for (p, d) in &d1 {
            expected.insert(*p, d.clone());
        }
        for (p, d) in &d2 {
            expected.insert(*p, d.clone());
        }

        let (_, decoded) = decode_all(&compacted);
        prop_assert_eq!(decoded, expected);
    }
}

#[test]
fn deletion_file_roundtrip() {
    // Commit=0, zero pages, post-apply checksum must be exactly the empty
    // checksum. (encoder.go:119)
    let mut enc = Encoder::new(Vec::new());
    enc.encode_header(Header {
        flags: 0,
        page_size: 4096,
        commit: 0,
        min_txid: Txid(5),
        max_txid: Txid(5),
        pre_apply_checksum: Checksum(ltx::CHECKSUM_FLAG | 0x1234),
        ..Default::default()
    })
    .unwrap();
    enc.set_post_apply_checksum(Checksum::EMPTY);
    let (bytes, _, trailer) = enc.finish().unwrap();
    assert_eq!(trailer.post_apply_checksum, Checksum::EMPTY);

    let dec = Decoder::new(Cursor::new(&bytes));
    let (hdr, _, index) = dec.verify().unwrap();
    assert_eq!(hdr.commit, 0);
    assert!(index.is_empty());

    // A wrong post-apply checksum on a deletion file must fail at encode time.
    let mut enc = Encoder::new(Vec::new());
    enc.encode_header(Header {
        flags: 0,
        page_size: 4096,
        commit: 0,
        min_txid: Txid(5),
        max_txid: Txid(5),
        pre_apply_checksum: Checksum(ltx::CHECKSUM_FLAG | 0x1234),
        ..Default::default()
    })
    .unwrap();
    enc.set_post_apply_checksum(Checksum(ltx::CHECKSUM_FLAG | 0x99));
    assert!(enc.finish().is_err());
}

#[test]
fn lock_page_skipped_in_giant_snapshot() {
    // With 512-byte pages the lock page is pgno 2097153 — too big to test
    // directly, so exercise the encoder rule with a synthetic small case:
    // encoding the lock page must fail.
    let page_size = 512u32;
    let lock = ltx::lock_pgno(page_size);
    let mut enc = Encoder::new(Vec::new());
    enc.encode_header(Header {
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size,
        commit: lock + 1,
        min_txid: Txid(2),
        max_txid: Txid(2),
        ..Default::default()
    })
    .unwrap();
    let err = enc.encode_page(lock, &vec![0u8; page_size as usize]).unwrap_err();
    assert!(err.to_string().contains("cannot encode lock page"), "{err}");
}
