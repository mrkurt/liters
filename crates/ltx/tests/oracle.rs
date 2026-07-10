//! Interop tests against the Go reference implementation ("the oracle"):
//! the `ltx` CLI at the exact version Litestream v0.5.x pins.
//!
//! Build the binaries with `make oracle`; tests skip if they are absent.

mod support;

use std::fs::File;
use std::io::BufReader;

use ltx::{
    checksum_page, xor_page_checksum, Checksum, Decoder, Encoder, Header, Txid,
    HEADER_FLAG_NO_CHECKSUM,
};
use support::*;

/// Encodes a snapshot LTX from a SQLite db file the same way `ltx encode-db`
/// does: checksum tracking on, MinTXID=MaxTXID=1, lock page skipped.
fn rust_encode_db(db_path: &std::path::Path, out_path: &std::path::Path) -> Checksum {
    let (page_size, pages) = read_db_pages(db_path);
    let lock_pgno = ltx::lock_pgno(page_size);

    let mut enc = Encoder::new(File::create(out_path).unwrap());
    enc.encode_header(Header {
        flags: 0,
        page_size,
        commit: pages.len() as u32,
        min_txid: Txid(1),
        max_txid: Txid(1),
        timestamp: 0,
        ..Default::default()
    })
    .unwrap();

    let mut post_apply = Checksum(0);
    for (i, page) in pages.iter().enumerate() {
        let pgno = (i + 1) as u32;
        if pgno == lock_pgno {
            continue;
        }
        enc.encode_page(pgno, page).unwrap();
        post_apply = xor_page_checksum(post_apply, checksum_page(pgno, page));
    }
    enc.set_post_apply_checksum(post_apply);
    enc.finish().unwrap();
    post_apply
}

#[test]
fn go_encode_db_rust_decode() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("test.db");
    let ltx_path = tmp.path().join("test.ltx");
    make_test_db(&db, 500);

    // Note: the stock `ltx encode-db` at v0.5.1 is broken (sets Version: 1),
    // so the encode oracle is our pinned-version Go helper.
    run_oracle(
        &oracle,
        "oracle-helper",
        &["encode-db", ltx_path.to_str().unwrap(), db.to_str().unwrap()],
    );

    // Full verification decode (checks file checksum AND, because encode-db
    // tracks checksums, the snapshot post-apply checksum).
    let dec = Decoder::new(BufReader::new(File::open(&ltx_path).unwrap()));
    let (header, trailer, index) = dec.verify().unwrap();
    let (page_size, pages) = read_db_pages(&db);
    assert_eq!(header.page_size, page_size);
    assert_eq!(header.commit as usize, pages.len());
    assert_eq!(header.min_txid, Txid(1));
    assert_eq!(header.max_txid, Txid(1));
    assert!(header.is_snapshot());
    assert_eq!(header.flags, 0);
    assert!(!index.is_empty());

    // Materialize the database from the Go-encoded LTX; must be byte-identical
    // to the source database file.
    let dec = Decoder::new(BufReader::new(File::open(&ltx_path).unwrap()));
    let mut out = Vec::new();
    dec.decode_database_to(&mut out).unwrap();
    assert_eq!(out, std::fs::read(&db).unwrap(), "materialized db differs from source");

    // Our independently computed rolling checksum must equal the Go trailer's.
    let mut post_apply = Checksum(0);
    for (i, page) in pages.iter().enumerate() {
        post_apply = xor_page_checksum(post_apply, checksum_page((i + 1) as u32, page));
    }
    assert_eq!(post_apply, trailer.post_apply_checksum);
}

#[test]
fn rust_encode_go_verify_and_apply() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("test.db");
    let ltx_path = tmp.path().join("test.ltx");
    make_test_db(&db, 500);

    rust_encode_db(&db, &ltx_path);

    // Go must verify our file (structure + CRC64 + snapshot checksum).
    run_oracle(&oracle, "ltx", &["verify", ltx_path.to_str().unwrap()]);

    // Go `ltx apply` onto an empty db must reproduce the source bytes.
    let applied = tmp.path().join("applied.db");
    run_oracle(
        &oracle,
        "ltx",
        &["apply", "-db", applied.to_str().unwrap(), ltx_path.to_str().unwrap()],
    );
    assert_eq!(
        std::fs::read(&applied).unwrap(),
        std::fs::read(&db).unwrap(),
        "Go-applied db differs from source"
    );
}

#[test]
fn checksum_parity_with_go() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("test.db");
    make_test_db(&db, 100);

    let out = run_oracle(&oracle, "ltx", &["checksum", db.to_str().unwrap()]);
    let go_checksum = out.trim();

    let (_, pages) = read_db_pages(&db);
    let mut chksum = Checksum(0);
    for (i, page) in pages.iter().enumerate() {
        chksum = xor_page_checksum(chksum, checksum_page((i + 1) as u32, page));
    }
    assert_eq!(chksum.to_string(), go_checksum, "rolling db checksum mismatch");
}

#[test]
fn rust_nochecksum_l0_style_file_go_verifies() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("test.db");
    let ltx_path = tmp.path().join("l0.ltx");
    make_test_db(&db, 200);
    let (page_size, pages) = read_db_pages(&db);

    // A non-snapshot file the way litestream writes L0: NoChecksum flag,
    // sparse ascending page subset, single TXID, WAL linkage fields set.
    let mut enc = Encoder::new(File::create(&ltx_path).unwrap());
    enc.encode_header(Header {
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size,
        commit: pages.len() as u32,
        min_txid: Txid(2),
        max_txid: Txid(2),
        timestamp: 1_720_000_000_123,
        wal_offset: 32,
        wal_size: (24 + page_size as i64) * 3,
        wal_salt1: 0xdead_beef,
        wal_salt2: 0x0123_4567,
        ..Default::default()
    })
    .unwrap();
    // Pick a sparse subset of pages, ascending.
    let picks: Vec<u32> = vec![1, 3, (pages.len() as u32).min(7), pages.len() as u32]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    for pgno in &picks {
        enc.encode_page(*pgno, &pages[(*pgno - 1) as usize]).unwrap();
    }
    enc.finish().unwrap();

    run_oracle(&oracle, "ltx", &["verify", ltx_path.to_str().unwrap()]);

    // `ltx dump` must list exactly our pages.
    let dump = run_oracle(&oracle, "ltx", &["dump", ltx_path.to_str().unwrap()]);
    for pgno in &picks {
        assert!(
            dump.contains(&format!("pgno={pgno}")),
            "page {pgno} missing from dump:\n{dump}"
        );
    }
}

#[test]
fn rust_compactor_output_go_verifies_and_applies() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("test.db");
    make_test_db(&db, 300);
    let (page_size, pages) = read_db_pages(&db);
    let commit = pages.len() as u32;

    // Build a chain: snapshot at TXID 1 (old content), then two "transactions"
    // that mutate pages. Compact them; Go must verify and apply to the same
    // final state as applying the chain file-by-file.
    let mut old_pages = pages.clone();
    for p in old_pages.iter_mut().skip(1) {
        for b in p.iter_mut().take(32) {
            *b = b.wrapping_add(1); // perturb non-header pages' leading bytes
        }
    }

    let write_snapshot = |path: &std::path::Path, pages: &[Vec<u8>]| {
        let mut enc = Encoder::new(File::create(path).unwrap());
        enc.encode_header(Header {
            flags: 0,
            page_size,
            commit,
            min_txid: Txid(1),
            max_txid: Txid(1),
            ..Default::default()
        })
        .unwrap();
        let mut chksum = Checksum(0);
        for (i, page) in pages.iter().enumerate() {
            enc.encode_page((i + 1) as u32, page).unwrap();
            chksum = xor_page_checksum(chksum, checksum_page((i + 1) as u32, page));
        }
        enc.set_post_apply_checksum(chksum);
        enc.finish().unwrap();
        chksum
    };

    let f1 = tmp.path().join("1.ltx");
    let chksum1 = write_snapshot(&f1, &old_pages);

    // TXID 2: replace pages 2 and 4 with the real content.
    let f2 = tmp.path().join("2.ltx");
    let mut chksum2 = chksum1;
    {
        let mut enc = Encoder::new(File::create(&f2).unwrap());
        enc.encode_header(Header {
            flags: 0,
            page_size,
            commit,
            min_txid: Txid(2),
            max_txid: Txid(2),
            pre_apply_checksum: chksum1,
            ..Default::default()
        })
        .unwrap();
        for pgno in [2u32, 4] {
            enc.encode_page(pgno, &pages[(pgno - 1) as usize]).unwrap();
            chksum2 = xor_page_checksum(chksum2, checksum_page(pgno, &old_pages[(pgno - 1) as usize]));
            chksum2 = xor_page_checksum(chksum2, checksum_page(pgno, &pages[(pgno - 1) as usize]));
        }
        enc.set_post_apply_checksum(chksum2);
        enc.finish().unwrap();
    }

    // TXID 3: replace every remaining old page with real content.
    let f3 = tmp.path().join("3.ltx");
    let mut chksum3 = chksum2;
    {
        let mut enc = Encoder::new(File::create(&f3).unwrap());
        enc.encode_header(Header {
            flags: 0,
            page_size,
            commit,
            min_txid: Txid(3),
            max_txid: Txid(3),
            pre_apply_checksum: chksum2,
            ..Default::default()
        })
        .unwrap();
        for pgno in 2..=commit {
            if pgno == 2 || pgno == 4 {
                continue;
            }
            enc.encode_page(pgno, &pages[(pgno - 1) as usize]).unwrap();
            chksum3 =
                xor_page_checksum(chksum3, checksum_page(pgno, &old_pages[(pgno - 1) as usize]));
            chksum3 = xor_page_checksum(chksum3, checksum_page(pgno, &pages[(pgno - 1) as usize]));
        }
        enc.set_post_apply_checksum(chksum3);
        enc.finish().unwrap();
    }

    // Compact 1+2+3 into one snapshot.
    let compacted = tmp.path().join("compacted.ltx");
    let readers = vec![
        BufReader::new(File::open(&f1).unwrap()),
        BufReader::new(File::open(&f2).unwrap()),
        BufReader::new(File::open(&f3).unwrap()),
    ];
    let compactor = ltx::Compactor::new(readers);
    let (hdr, trailer) = compactor.compact(File::create(&compacted).unwrap()).unwrap();
    assert_eq!(hdr.min_txid, Txid(1));
    assert_eq!(hdr.max_txid, Txid(3));
    assert!(hdr.is_snapshot());
    assert_eq!(trailer.post_apply_checksum, chksum3);

    // Go verifies the compacted file...
    run_oracle(&oracle, "ltx", &["verify", compacted.to_str().unwrap()]);

    // ...and applying it must equal page 1 of old content? No: pages[0] was
    // never touched, so the final image is exactly the real db except page 1
    // stayed old (we perturbed only pages >= 2 in old_pages). Compare against
    // Go applying the three files in sequence.
    let go_applied = tmp.path().join("go-applied.db");
    run_oracle(
        &oracle,
        "ltx",
        &[
            "apply",
            "-db",
            go_applied.to_str().unwrap(),
            f1.to_str().unwrap(),
            f2.to_str().unwrap(),
            f3.to_str().unwrap(),
        ],
    );
    let rust_applied = tmp.path().join("rust-applied.db");
    run_oracle(
        &oracle,
        "ltx",
        &["apply", "-db", rust_applied.to_str().unwrap(), compacted.to_str().unwrap()],
    );
    assert_eq!(
        std::fs::read(&go_applied).unwrap(),
        std::fs::read(&rust_applied).unwrap(),
        "compacted apply differs from sequential apply"
    );
}

#[test]
fn corrupted_file_rejected_by_both() {
    let Some(oracle) = oracle_dir() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("test.db");
    let ltx_path = tmp.path().join("test.ltx");
    make_test_db(&db, 50);
    rust_encode_db(&db, &ltx_path);

    // Flip a byte in the middle of the page block.
    let mut bytes = std::fs::read(&ltx_path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    let corrupt = tmp.path().join("corrupt.ltx");
    std::fs::write(&corrupt, &bytes).unwrap();

    assert!(
        try_run_oracle(&oracle, "ltx", &["verify", corrupt.to_str().unwrap()]).is_err(),
        "Go accepted corrupted file"
    );
    let dec = Decoder::new(BufReader::new(File::open(&corrupt).unwrap()));
    assert!(dec.verify().is_err(), "Rust accepted corrupted file");
}
