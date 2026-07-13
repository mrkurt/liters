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
use std::time::{Duration, Instant};

use liters_storage::{CancelToken, ReplicaClient, StreamEvent, SNAPSHOT_LEVEL};
use ltx::{is_contiguous, Compactor, Decoder, FileInfo, Txid, HEADER_FLAG_NO_CHECKSUM, HEADER_SIZE};
use rand::RngCore;

use crate::{Backoff, Error, Result, StorageError};

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
    /// local state, restore from scratch instead of returning
    /// [`Error::Diverged`]. The restore materializes to a temp file and
    /// atomically replaces the local replica, so the existing replica
    /// survives if the restore fails partway.
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

/// Options for [`Replica::follow`].
#[derive(Debug, Clone)]
pub struct FollowOptions {
    /// Cadence for backends without streaming support, for waiting out an
    /// empty (not-yet-seeded) bucket, and the backoff after a round that
    /// made no progress (prevents hot resync loops against a pruned or
    /// stalled bucket).
    pub poll_interval: Duration,
    /// `Some(backoff)`: transient storage/network errors (per
    /// [`Error::is_transient`]) sleep `backoff.delay(n)` — where `n` counts
    /// consecutive failures and resets to zero whenever a round makes
    /// progress — and retry instead of returning. For followers that must
    /// survive server restarts and flaky links. `None`: fail fast on the
    /// first error. Non-transient errors always return immediately.
    pub retry: Option<Backoff>,
}

impl Default for FollowOptions {
    fn default() -> Self {
        FollowOptions { poll_interval: Duration::from_secs(1), retry: None }
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
    ///
    /// Equal to [`Replica::sync_with`] with a token that never cancels.
    pub fn sync(&mut self) -> Result<SyncResult> {
        self.sync_with(&CancelToken::new())
    }

    /// [`Replica::sync`], cancellable: the token is installed on the storage
    /// client and checked between fetched files — never mid-apply. A started
    /// page application always runs to completion: a torn apply is healed by
    /// re-apply anyway, but deliberately widening that window buys nothing.
    /// A cancelled sync returns [`Error::Cancelled`]; the local replica is
    /// exactly as consistent as after a kill (tmp spools removed, position
    /// sidecar only ever advanced after a completed apply).
    pub fn sync_with(&mut self, cancel: &CancelToken) -> Result<SyncResult> {
        self.client.set_cancel(cancel.clone());

        if !self.db_path.exists() {
            let to = self.full_restore(cancel)?;
            return Ok(SyncResult { restored: true, from_txid: Txid(0), to_txid: to });
        }

        let from = read_txid_file(&self.db_path)?;
        if from.is_zero() {
            // Local file without a sidecar: unresumable. (replica.go:566-568)
            if self.opts.auto_reset {
                let to = self.full_restore(cancel)?;
                return Ok(SyncResult { restored: true, from_txid: Txid(0), to_txid: to });
            }
            return Err(Error::Other(format!(
                "replica exists but has no -txid sidecar; delete {} to re-restore",
                self.db_path.display()
            )));
        }

        match self.incremental_sync(from, cancel) {
            Ok(to) => Ok(SyncResult { restored: false, from_txid: from, to_txid: to }),
            // The healthy replica is never deleted up front: full_restore
            // replaces it atomically only once a complete new image exists,
            // so a failed restore leaves the old replica readable.
            Err(Error::Diverged { .. }) if self.opts.auto_reset => {
                let to = self.full_restore(cancel)?;
                Ok(SyncResult { restored: true, from_txid: from, to_txid: to })
            }
            Err(e) => Err(e),
        }
    }

    /// Follows the bucket continuously until `cancel` is cancelled or a
    /// fatal error occurs. Uses the backend's live stream
    /// ([`ReplicaClient::open_ltx_stream`], e.g. a liters HTTP server's
    /// `/stream`) when available — new transactions apply as they arrive
    /// over a single connection — and falls back to polling [`Replica::sync`]
    /// otherwise.
    ///
    /// Every stream anomaly (gap, reseed, non-contiguous frame, corrupt
    /// file) routes back through `sync()`, which owns the hardened
    /// restore/bridge/divergence logic; `follow` never invents its own
    /// recovery. The position sidecar advances after every applied
    /// transaction, so a killed follower resumes exactly where it stopped.
    ///
    /// Blocking: run it on a dedicated thread and cancel the token to end
    /// it. Cancellation is a clean stop — follow returns `Ok(())`, never
    /// [`Error::Cancelled`] — normally observed within ~a second (or after
    /// the in-flight file finishes applying); the worst case is the stream
    /// dead-man bound (~45s) if a frame stalls mid-transfer on a dead link
    /// and the backend is not token-aware.
    pub fn follow(&mut self, cancel: &CancelToken, opts: &FollowOptions) -> Result<()> {
        self.client.set_cancel(cancel.clone());

        // Consecutive transient-failure count driving the `retry` backoff;
        // any progress resets it. Empty-bucket waits use poll_interval and
        // never touch it (an unseeded bucket is not a failure).
        let mut attempt: u32 = 0;
        while !cancel.is_cancelled() {
            let before = self.position()?;
            match self.sync_with(cancel) {
                Ok(_) => attempt = 0,
                // Cancellation is a clean stop, not an error.
                Err(Error::Cancelled) => return Ok(()),
                // Empty bucket: the writer has not seeded it yet. Wait,
                // matching finish_incremental's empty-is-not-divergence
                // stance.
                Err(Error::TxNotAvailable) => {
                    if sleep_checking(cancel, opts.poll_interval) {
                        return Ok(());
                    }
                    continue;
                }
                Err(e) if e.is_transient() && opts.retry.is_some() => {
                    if sleep_checking(cancel, opts.retry.as_ref().unwrap().delay(attempt)) {
                        return Ok(());
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e),
            }
            let mut position = self.position()?;
            let mut made_progress = position > before;

            let stream = match self.client.open_ltx_stream(Txid(position.0 + 1)) {
                Ok(stream) => stream,
                Err(StorageError::Cancelled) => return Ok(()),
                Err(e) if e.is_transient() && opts.retry.is_some() => {
                    if sleep_checking(cancel, opts.retry.as_ref().unwrap().delay(attempt)) {
                        return Ok(());
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            let Some(mut stream) = stream else {
                // No streaming support: plain sync() polling.
                if sleep_checking(cancel, opts.poll_interval) {
                    return Ok(());
                }
                continue;
            };

            // (Re)open the database and page size after every sync():
            // a full restore replaces the inode, and a reseeded bucket may
            // change the page size.
            let (db, page_size) = match open_db_for_apply(&self.db_path) {
                Ok(pair) => pair,
                Err(e) if e.is_transient() && opts.retry.is_some() => {
                    if sleep_checking(cancel, opts.retry.as_ref().unwrap().delay(attempt)) {
                        return Ok(());
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e),
            };
            let spool_path = tmp_sibling(&self.db_path, ".apply.tmp");

            // Runs until the stream ends or misbehaves. `None` = fall back
            // to sync() (the stream told us to, or a frame didn't fit);
            // `Some(e)` = this session failed — the error goes through the
            // same transient/retry classification as sync() errors, so
            // local I/O hiccups honor `retry` too.
            let stream_err: Option<Error> = loop {
                if cancel.is_cancelled() {
                    let _ = fs::remove_file(&spool_path);
                    return Ok(());
                }
                let mut spool = match File::create(&spool_path) {
                    Ok(f) => f,
                    Err(e) => break Some(e.into()),
                };
                match stream.next(&mut spool) {
                    Ok(StreamEvent::Ltx(info)) => {
                        if info.max_txid <= position {
                            continue; // stale frame, already applied
                        }
                        if !is_contiguous(position, info.min_txid, info.max_txid) {
                            break None; // gapped frame: bridge via sync()
                        }
                        if let Err(e) = spool.sync_all() {
                            break Some(e.into());
                        }
                        drop(spool);
                        match self.apply_spooled(&db, &spool_path, page_size) {
                            Ok(()) => {
                                position = info.max_txid;
                                if let Err(e) = write_txid_file(&self.db_path, position) {
                                    break Some(e);
                                }
                                made_progress = true;
                            }
                            // Divergence, CRC/decode failures, and storage
                            // errors re-run through sync(), which owns the
                            // hardened routing (auto_reset, re-fetch by
                            // listing, ...).
                            Err(Error::Diverged { .. } | Error::Ltx(_) | Error::Storage(_)) => {
                                break None
                            }
                            Err(e) => break Some(e),
                        }
                    }
                    // An idle ping carrying a non-empty bucket max below our
                    // position is positive divergence evidence: let sync()
                    // confirm and route it. (Empty buckets are a
                    // wipe-then-reseed window, not divergence.)
                    Ok(StreamEvent::Idle { bucket_max: Some(m) })
                        if !m.is_zero() && m < position =>
                    {
                        break None
                    }
                    Ok(StreamEvent::Idle { .. }) => continue,
                    Ok(StreamEvent::Gap { .. } | StreamEvent::Reset { .. } | StreamEvent::Closed) => {
                        break None
                    }
                    // Unknown future event (StreamEvent is non-exhaustive):
                    // safest response is drop-the-stream and resync.
                    Ok(_) => break None,
                    Err(e) => break Some(e.into()),
                }
            };
            let _ = fs::remove_file(&spool_path);

            if made_progress {
                attempt = 0;
            }
            if let Some(e) = stream_err {
                // A cancelled stream (token-aware backend) is a clean stop.
                if matches!(e, Error::Cancelled) {
                    return Ok(());
                }
                match &opts.retry {
                    Some(backoff) if e.is_transient() => {
                        if sleep_checking(cancel, backoff.delay(attempt)) {
                            return Ok(());
                        }
                        attempt = attempt.saturating_add(1);
                    }
                    _ => return Err(e),
                }
            } else if !made_progress {
                // The whole round moved nothing: back off before resyncing
                // so a pruned/stalled bucket can't induce a hot spin.
                if sleep_checking(cancel, opts.poll_interval) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Full restore: plan → k-way merge → materialize → rename → verify.
    /// (replica.go:622-731)
    fn full_restore(&mut self, cancel: &CancelToken) -> Result<Txid> {
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
            cancel.check()?;
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
        cancel.check()?;
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
    fn incremental_sync(&mut self, from: Txid, cancel: &CancelToken) -> Result<Txid> {
        let mut f = OpenOptions::new().read(true).write(true).open(&self.db_path)?;
        let page_size = read_page_size(&mut f)?;

        let mut current = from;
        // A 404 on a file we just listed is a race with compaction/GC, not a
        // pruned chain: suppress the snapshot fallback for this sync and
        // re-list next time, like Go's retry-next-tick. (replica.go:844-849)
        let mut saw_404 = false;

        // Poll L0 for incremental files. (replica.go:801-851)
        let l0 = self.client.ltx_files(0, Txid(current.0 + 1), false)?;

        let saw_level0 = !l0.is_empty();
        for info in l0 {
            cancel.check()?;
            if info.min_txid.0 > current.0 + 1 {
                current =
                    self.fill_follow_gap(&f, current, info.min_txid, page_size, &mut saw_404, cancel)?;
                if info.max_txid <= current {
                    continue;
                }
                if info.min_txid.0 > current.0 + 1 {
                    // Still gapped; try again next sync (or snapshot below).
                    return self.finish_incremental(&f, from, current, page_size, !saw_404, cancel);
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
                    return self.finish_incremental(&f, from, current, page_size, false, cancel)
                }
                Err(e) => return Err(e),
            }
        }

        if !saw_level0 {
            current =
                self.fill_follow_gap(&f, current, Txid(current.0 + 1), page_size, &mut saw_404, cancel)?;
        }

        self.finish_incremental(&f, from, current, page_size, !saw_404, cancel)
    }

    /// Post-pass: snapshot fallback and divergence detection, then persist
    /// the new position. `allow_snapshot_fallback` is false when this sync
    /// hit a transient 404 — no-progress then means "retry next sync", not
    /// "the chain was pruned".
    fn finish_incremental(
        &mut self,
        f: &File,
        from: Txid,
        mut current: Txid,
        page_size: u32,
        allow_snapshot_fallback: bool,
        cancel: &CancelToken,
    ) -> Result<Txid> {
        if current == from && allow_snapshot_fallback {
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

            // A completely empty bucket is a no-op sync, not divergence: it
            // is the transient window of a wipe-then-reseed, and there is
            // nothing to restore from anyway. Go's follow loop likewise
            // no-ops on empty listings. Divergence is only declared on
            // positive evidence: files present whose max is below ours.
            if bucket_max.is_zero() {
                return Ok(current);
            }
            if bucket_max < current {
                return Err(Error::Diverged { local: current, remote: bucket_max });
            }

            // Newer data reachable only via a snapshot (levels 0-8 pruned):
            // a snapshot is contiguous with any position (min=1) and contains
            // every page, so apply it in place. (Litestream's follow mode
            // stalls here; see docs/research/restore-read-path.md §9.)
            if let Some(snap) = newest_snapshot {
                if snap.max_txid > current {
                    cancel.check()?;
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
        saw_404: &mut bool,
        cancel: &CancelToken,
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
                cancel.check()?;
                match self.apply_ltx_file(f, &info, page_size) {
                    Ok(()) => current = info.max_txid,
                    Err(Error::Storage(StorageError::NotFound { .. })) => {
                        *saw_404 = true;
                        return Ok(current);
                    }
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

    /// Applies one LTX file's pages to the replica in place: fetches to the
    /// spool file, then [`Replica::apply_spooled`]. (replica.go:879-930)
    fn apply_ltx_file(&mut self, f: &File, info: &FileInfo, page_size: u32) -> Result<()> {
        let spool_path = tmp_sibling(&self.db_path, ".apply.tmp");
        let _cleanup = TmpGuard(&spool_path);
        {
            let mut rc = self.client.open_ltx_file(info.level, info.min_txid, info.max_txid, 0, 0)?;
            let mut spool = File::create(&spool_path)?;
            std::io::copy(&mut rc, &mut spool)?;
            spool.sync_all()?;
        }
        self.apply_spooled(f, &spool_path, page_size)
    }

    /// Applies one complete, already-spooled LTX file. The spool is
    /// CRC-verified in full *before* any page is written (hardening over Go,
    /// which verifies after). Page 1 gets the journal mode + change counter
    /// fixups; the file is truncated to the commit size. Shared by the
    /// fetch-by-listing path and streaming follow.
    fn apply_spooled(&mut self, f: &File, spool_path: &Path, page_size: u32) -> Result<()> {
        {
            let dec = Decoder::new(BufReader::new(File::open(spool_path)?));
            dec.verify()?;
        }

        let mut dec = Decoder::new(BufReader::new(File::open(spool_path)?));
        dec.decode_header()?;
        let hdr = *dec.header();
        if hdr.page_size != page_size {
            // Page-size change implies a bucket reset. (compat: ltx
            // compactor rejects mismatched page sizes)
            return Err(Error::Diverged { local: hdr.min_txid, remote: hdr.max_txid });
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

/// Fresh handle + page size for in-place page application.
fn open_db_for_apply(db_path: &Path) -> Result<(File, u32)> {
    let mut db = OpenOptions::new().read(true).write(true).open(db_path)?;
    let page_size = read_page_size(&mut db)?;
    Ok((db, page_size))
}

/// Sleeps `total` in short slices so cancellation stays responsive; returns
/// true if cancelled.
fn sleep_checking(cancel: &CancelToken, total: Duration) -> bool {
    let deadline = Instant::now() + total;
    loop {
        if cancel.is_cancelled() {
            return true;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return false;
        }
        std::thread::sleep(left.min(Duration::from_millis(100)));
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

pub(crate) fn txid_path(db_path: &Path) -> PathBuf {
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
