//! The read side: a local, read-only materialization of a replica bucket,
//! kept current by applying LTX files incrementally. Ports litestream's
//! restore + follow machinery (replica.go:544-994) with two hardening
//! changes for mobile:
//!
//! - every fetched file's CRC is verified *before* its pages touch the live
//!   replica (litestream verifies after writing);
//! - a stalled L0/L1-8 chain falls back to applying a newer snapshot in
//!   place, and a bucket whose max TXID went backwards is detected as
//!   divergence (litestream's follow mode stalls forever on both).

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use liters_storage::{ReplicaClient, SNAPSHOT_LEVEL};
use ltx::{Compactor, Decoder, FileInfo, Txid, HEADER_FLAG_NO_CHECKSUM, HEADER_SIZE};
use rand::RngCore;

use crate::{Error, Result, StorageError};

/// Post-restore integrity checking. (replica.go IntegrityCheck modes)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityCheck {
    None,
    Quick,
    Full,
}

#[derive(Debug, Clone)]
pub struct ReplicaOptions {
    /// Integrity check to run after a full restore.
    pub integrity_check: IntegrityCheck,
    /// Take SQLite-compatible fcntl locks on the replica file during page
    /// application. Protects readers in *other processes*; fcntl locks
    /// cannot exclude readers in this process (POSIX locks are per-process),
    /// so same-process readers must not hold read transactions across
    /// `sync()`.
    pub use_file_locks: bool,
    /// On divergence (bucket reseeded below our position) or unresumable
    /// local state, delete the local replica and restore from scratch
    /// instead of returning [`Error::Diverged`].
    pub auto_reset: bool,
}

impl Default for ReplicaOptions {
    fn default() -> Self {
        ReplicaOptions {
            integrity_check: IntegrityCheck::Quick,
            use_file_locks: true,
            auto_reset: false,
        }
    }
}

/// Result of a [`Replica::sync`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncResult {
    /// Whether a full restore (not an incremental apply) ran.
    pub restored: bool,
    pub from_txid: Txid,
    pub to_txid: Txid,
}

/// A local materialized read replica of one database's bucket.
pub struct Replica {
    db_path: PathBuf,
    client: Box<dyn ReplicaClient>,
    opts: ReplicaOptions,
}

impl Replica {
    pub fn open(
        db_path: impl Into<PathBuf>,
        client: Box<dyn ReplicaClient>,
        opts: ReplicaOptions,
    ) -> Replica {
        Replica { db_path: db_path.into(), client, opts }
    }

    /// The local database file. Open it read-only with plain SQLite; it is a
    /// rollback-journal-mode file (no -wal/-shm needed).
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Last applied TXID from the sidecar; zero if never synced.
    pub fn position(&self) -> Result<Txid> {
        read_txid_file(&self.db_path)
    }

    /// Deletes the local replica and its sidecar.
    pub fn reset(&self) -> Result<()> {
        for suffix in ["", "-txid", ".tmp", ".apply.tmp"] {
            let mut p = self.db_path.as_os_str().to_owned();
            p.push(suffix);
            match fs::remove_file(PathBuf::from(p)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Brings the local replica up to date: a full restore when the local
    /// file is missing, otherwise incremental application of new LTX files.
    pub fn sync(&mut self) -> Result<SyncResult> {
        if !self.db_path.exists() {
            let to = self.full_restore()?;
            return Ok(SyncResult { restored: true, from_txid: Txid(0), to_txid: to });
        }

        let from = read_txid_file(&self.db_path)?;
        if from.is_zero() {
            // Local file without a sidecar: unresumable. (replica.go:566-568)
            if self.opts.auto_reset {
                self.reset()?;
                let to = self.full_restore()?;
                return Ok(SyncResult { restored: true, from_txid: Txid(0), to_txid: to });
            }
            return Err(Error::Other(format!(
                "replica exists but has no -txid sidecar; delete {} to re-restore",
                self.db_path.display()
            )));
        }

        match self.incremental_sync(from) {
            Ok(to) => Ok(SyncResult { restored: false, from_txid: from, to_txid: to }),
            Err(Error::Diverged { .. }) if self.opts.auto_reset => {
                self.reset()?;
                let to = self.full_restore()?;
                Ok(SyncResult { restored: true, from_txid: from, to_txid: to })
            }
            Err(e) => Err(e),
        }
    }

    /// Full restore: plan → k-way merge → materialize → rename → verify.
    /// (replica.go:622-731)
    fn full_restore(&mut self) -> Result<Txid> {
        let plan = crate::plan::calc_restore_plan(self.client.as_ref(), Txid(0))?;

        // Minimum plausible size check. (replica.go:640-643)
        for info in &plan {
            if info.size < HEADER_SIZE as u64 {
                return Err(Error::Other(format!(
                    "invalid ltx file: level={} min={} max={} has size {} bytes",
                    info.level, info.min_txid, info.max_txid, info.size
                )));
            }
        }

        let mut rdrs: Vec<Box<dyn Read + Send>> = Vec::with_capacity(plan.len());
        for info in &plan {
            rdrs.push(self.client.open_ltx_file(info.level, info.min_txid, info.max_txid, 0, 0)?);
        }

        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = tmp_sibling(&self.db_path, ".tmp");
        let cleanup = TmpGuard(&tmp_path);

        // Merge the chain and materialize the database image in one pass:
        // Compactor -> pipe -> DecodeDatabaseTo in Go; here the compactor
        // writes to an in-file buffer first (single-threaded).
        let compacted_path = tmp_sibling(&self.db_path, ".compact.tmp");
        let compact_cleanup = TmpGuard(&compacted_path);
        {
            let mut compactor = Compactor::new(rdrs);
            compactor.header_flags = HEADER_FLAG_NO_CHECKSUM;
            let out = File::create(&compacted_path)?;
            compactor.compact(std::io::BufWriter::new(out))?;
        }

        let to_txid = plan.last().unwrap().max_txid;
        {
            let mut f = File::create(&tmp_path)?;
            let dec = Decoder::new(BufReader::new(File::open(&compacted_path)?));
            dec.decode_database_to(&mut f)?;
            f.sync_all()?;
        }
        drop(compact_cleanup);
        fs::rename(&tmp_path, &self.db_path)?;
        drop(cleanup);
        fsync_parent(&self.db_path);

        // Never leave stale sqlite side files next to a fresh image.
        // (replica.go:1258-1293)
        for suffix in ["-wal", "-shm"] {
            let mut p = self.db_path.as_os_str().to_owned();
            p.push(suffix);
            let _ = fs::remove_file(PathBuf::from(p));
        }

        self.check_integrity()?;
        write_txid_file(&self.db_path, to_txid)?;
        Ok(to_txid)
    }

    /// One round of follow-mode application: L0 from our position, bridging
    /// gaps through levels 1..8, with snapshot fallback and divergence
    /// detection. (replica.go:798-869 + hardening)
    fn incremental_sync(&mut self, from: Txid) -> Result<Txid> {
        let mut f = OpenOptions::new().read(true).write(true).open(&self.db_path)?;
        let page_size = read_page_size(&mut f)?;

        let mut current = from;

        // Poll L0 for incremental files. (replica.go:801-851)
        let l0 = self.client.ltx_files(0, Txid(current.0 + 1), false)?;

        let saw_level0 = !l0.is_empty();
        for info in l0 {
            if info.min_txid.0 > current.0 + 1 {
                current = self.fill_follow_gap(&f, current, info.min_txid, page_size)?;
                if info.max_txid <= current {
                    continue;
                }
                if info.min_txid.0 > current.0 + 1 {
                    // Still gapped; try again next sync (or snapshot below).
                    return self.finish_incremental(&f, from, current, page_size);
                }
            }
            if info.max_txid <= current {
                continue;
            }
            match self.apply_ltx_file(&f, &info, page_size) {
                Ok(()) => current = info.max_txid,
                // A listed file may 404 mid-race with compaction/GC:
                // re-list next sync; never advance past it. (resumable_reader.go:75)
                Err(Error::Storage(StorageError::NotFound { .. })) => {
                    return self.finish_incremental(&f, from, current, page_size)
                }
                Err(e) => return Err(e),
            }
        }

        if !saw_level0 {
            current = self.fill_follow_gap(&f, current, Txid(current.0 + 1), page_size)?;
        }

        self.finish_incremental(&f, from, current, page_size)
    }

    /// Post-pass: snapshot fallback and divergence detection, then persist
    /// the new position.
    fn finish_incremental(
        &mut self,
        f: &File,
        from: Txid,
        mut current: Txid,
        page_size: u32,
    ) -> Result<Txid> {
        if current == from {
            // No progress. Distinguish up-to-date / snapshot-only-newer /
            // diverged via the bucket-wide max TXID.
            let mut bucket_max = Txid(0);
            let mut newest_snapshot: Option<FileInfo> = None;
            for level in (0..=SNAPSHOT_LEVEL).rev() {
                for info in self.client.ltx_files(level, Txid(0), false)? {
                    if info.max_txid > bucket_max {
                        bucket_max = info.max_txid;
                    }
                    if level == SNAPSHOT_LEVEL
                        && newest_snapshot.as_ref().is_none_or(|s| info.max_txid > s.max_txid)
                    {
                        newest_snapshot = Some(info);
                    }
                }
            }

            if bucket_max < current && !bucket_max.is_zero() {
                return Err(Error::Diverged { local: current, remote: bucket_max });
            }
            if bucket_max.is_zero() && !current.is_zero() {
                // Bucket wiped entirely.
                return Err(Error::Diverged { local: current, remote: Txid(0) });
            }

            // Newer data reachable only via a snapshot (levels 0-8 pruned):
            // a snapshot is contiguous with any position (min=1) and contains
            // every page, so apply it in place. (Litestream's follow mode
            // stalls here; see docs/research/restore-read-path.md §9.)
            if let Some(snap) = newest_snapshot {
                if snap.max_txid > current {
                    self.apply_ltx_file(f, &snap, page_size)?;
                    current = snap.max_txid;
                }
            }
        }

        if current > from {
            write_txid_file(&self.db_path, current)?;
        }
        Ok(current)
    }

    /// Bridges an L0 gap through levels 1..8 (never the snapshot level).
    /// (replica.go:932-994)
    fn fill_follow_gap(
        &mut self,
        f: &File,
        after: Txid,
        gap_min: Txid,
        page_size: u32,
    ) -> Result<Txid> {
        let mut current = after;
        for level in 1..SNAPSHOT_LEVEL {
            for info in self.client.ltx_files(level, Txid(0), false)? {
                if info.min_txid.0 > current.0 + 1 {
                    break; // gap at this level too
                }
                if info.max_txid <= current {
                    continue;
                }
                match self.apply_ltx_file(f, &info, page_size) {
                    Ok(()) => current = info.max_txid,
                    Err(Error::Storage(StorageError::NotFound { .. })) => return Ok(current),
                    Err(e) => return Err(e),
                }
                if current.0 + 1 >= gap_min.0 {
                    return Ok(current);
                }
            }
            // Progress at this level: let the caller re-evaluate L0.
            if current > after {
                return Ok(current);
            }
        }
        Ok(current)
    }

    /// Applies one LTX file's pages to the replica in place. The file is
    /// downloaded and CRC-verified in full *before* any page is written
    /// (hardening over Go, which verifies after). Page 1 gets the journal
    /// mode + change counter fixups; the file is truncated to the commit
    /// size. (replica.go:879-930)
    fn apply_ltx_file(&mut self, f: &File, info: &FileInfo, page_size: u32) -> Result<()> {
        // Fetch to a temp file and verify end-to-end.
        let spool_path = tmp_sibling(&self.db_path, ".apply.tmp");
        let _cleanup = TmpGuard(&spool_path);
        {
            let mut rc = self.client.open_ltx_file(info.level, info.min_txid, info.max_txid, 0, 0)?;
            let mut spool = File::create(&spool_path)?;
            std::io::copy(&mut rc, &mut spool)?;
            spool.sync_all()?;
        }
        {
            let dec = Decoder::new(BufReader::new(File::open(&spool_path)?));
            dec.verify()?;
        }

        let mut dec = Decoder::new(BufReader::new(File::open(&spool_path)?));
        dec.decode_header()?;
        let hdr = *dec.header();
        if hdr.page_size != page_size {
            // Page-size change implies a bucket reset. (compat: ltx
            // compactor rejects mismatched page sizes)
            return Err(Error::Diverged { local: info.min_txid, remote: info.max_txid });
        }

        let _lock = if self.opts.use_file_locks { Some(FcntlLock::exclusive(f)?) } else { None };

        let mut data = vec![0u8; page_size as usize];
        while let Some(phdr) = dec.decode_page(&mut data)? {
            if phdr.pgno == 1 && data.len() >= 28 {
                // Present as a rollback-journal database and invalidate other
                // connections' caches. (replica.go:907-910)
                data[18] = 0x01;
                data[19] = 0x01;
                rand::rng().fill_bytes(&mut data[24..28]);
            }
            let off = (phdr.pgno as u64 - 1) * page_size as u64;
            std::os::unix::fs::FileExt::write_all_at(f, &data, off)?;
        }

        if hdr.commit > 0 {
            f.set_len(hdr.commit as u64 * page_size as u64)?;
        }

        dec.finish()?;
        f.sync_all()?;
        Ok(())
    }

    fn check_integrity(&self) -> Result<()> {
        let pragma = match self.opts.integrity_check {
            IntegrityCheck::None => return Ok(()),
            IntegrityCheck::Quick => "quick_check",
            IntegrityCheck::Full => "integrity_check",
        };
        let conn = rusqlite::Connection::open_with_flags(
            &self.db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let result: String = conn.query_row(&format!("PRAGMA {pragma}"), [], |r| r.get(0))?;
        if result != "ok" {
            let _ = fs::remove_file(&self.db_path);
            return Err(Error::Other(format!("post-restore integrity check failed: {result}")));
        }
        Ok(())
    }
}

/// SQLite-compatible exclusive byte-range locks on the replica file, taken
/// for the duration of a page application. (internal/lock_unix.go)
struct FcntlLock<'a> {
    f: &'a File,
}

const SQLITE_PENDING_BYTE: i64 = 0x4000_0000;
const SQLITE_SHARED_FIRST: i64 = SQLITE_PENDING_BYTE + 2;
const SQLITE_SHARED_SIZE: i64 = 510;

fn set_fcntl_lock(f: &File, lock_type: libc::c_short, start: i64, len: i64) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    let fl = libc::flock {
        l_start: start,
        l_len: len,
        l_pid: 0,
        l_type: lock_type,
        l_whence: libc::SEEK_SET as libc::c_short,
    };
    let rc = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_SETLKW, &fl) };
    if rc == -1 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

impl<'a> FcntlLock<'a> {
    fn exclusive(f: &'a File) -> Result<FcntlLock<'a>> {
        set_fcntl_lock(f, libc::F_WRLCK as libc::c_short, SQLITE_PENDING_BYTE, 1)?;
        if let Err(e) =
            set_fcntl_lock(f, libc::F_WRLCK as libc::c_short, SQLITE_SHARED_FIRST, SQLITE_SHARED_SIZE)
        {
            let _ = set_fcntl_lock(f, libc::F_UNLCK as libc::c_short, SQLITE_PENDING_BYTE, 1);
            return Err(e);
        }
        Ok(FcntlLock { f })
    }
}

impl Drop for FcntlLock<'_> {
    fn drop(&mut self) {
        let _ =
            set_fcntl_lock(self.f, libc::F_UNLCK as libc::c_short, SQLITE_SHARED_FIRST, SQLITE_SHARED_SIZE);
        let _ = set_fcntl_lock(self.f, libc::F_UNLCK as libc::c_short, SQLITE_PENDING_BYTE, 1);
    }
}

/// Reads the page size from the SQLite header. (replica.go:747-755)
fn read_page_size(f: &mut File) -> Result<u32> {
    let mut buf = [0u8; 2];
    f.seek(SeekFrom::Start(16))?;
    f.read_exact(&mut buf)?;
    let ps = u32::from(u16::from_be_bytes(buf));
    Ok(if ps == 1 { 65536 } else { ps })
}

/// `{db}-txid` sidecar: 16-hex TXID + newline, written atomically.
/// Byte-compatible with litestream's follow-mode sidecar. (replica.go:1645-1703)
pub fn read_txid_file(db_path: &Path) -> Result<Txid> {
    let path = txid_path(db_path);
    match fs::read_to_string(&path) {
        Ok(s) => Txid::parse(s.trim())
            .ok_or_else(|| Error::Other(format!("parse txid file {path:?}: {s:?}"))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Txid(0)),
        Err(e) => Err(e.into()),
    }
}

pub fn write_txid_file(db_path: &Path, txid: Txid) -> Result<()> {
    let path = txid_path(db_path);
    let tmp = tmp_sibling(&path, ".tmp");
    {
        let mut f = File::create(&tmp)?;
        writeln!(f, "{txid}")?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    fsync_parent(&path);
    Ok(())
}

fn txid_path(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-txid");
    PathBuf::from(p)
}

fn tmp_sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(suffix);
    PathBuf::from(p)
}

fn fsync_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        if let Ok(d) = File::open(parent) {
            let _ = d.sync_all();
        }
    }
}

struct TmpGuard<'a>(&'a Path);

impl Drop for TmpGuard<'_> {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0);
    }
}
