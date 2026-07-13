//! Device-side compaction, snapshots, and retention. The device is the sole
//! writer of its bucket prefix, so it is also the sole compactor — no
//! coordination needed. Ports litestream's store/compactor logic
//! (compactor.go, store.go:745-823, db.go:2337-2667) with push-relative
//! triggers instead of wall-clock-aligned timers (sporadic app usage makes
//! aligned windows meaningless).
//!
//! Invariants preserved for stock readers (litestream restore & follow):
//! - the newest snapshot is never deleted;
//! - every level keeps at least one file;
//! - L0 files are deleted only once covered by L1 AND older than the grace
//!   period, never leaving gaps (deletion stops at the first retained file).

use std::fs::File;
use std::time::{Duration, SystemTime};

use liters_wal::WalReader;
use liters_storage::{CancelToken, SNAPSHOT_LEVEL};
use ltx::{FileInfo, Header, Txid, HEADER_FLAG_NO_CHECKSUM};

use crate::writer::{TmpGuard, Writer};
use crate::{Error, Result};

/// Maintenance configuration, defaulted to litestream's. (compaction_level.go:14,
/// store.go:60-72)
#[derive(Debug, Clone)]
pub struct MaintenanceOptions {
    /// Compaction interval per level; index 0 is L1. A level is compacted on
    /// maintain() when its newest file is older than its interval.
    pub level_intervals: Vec<Duration>,
    /// How often to write a full snapshot to level 9.
    pub snapshot_interval: Duration,
    /// How long snapshots are retained (the newest always survives).
    pub snapshot_retention: Duration,
    /// Grace period before compacted L0 files are deleted, protecting
    /// readers mid-application. (store.go:68)
    pub l0_retention: Duration,
    /// Disable to leave deletion to bucket lifecycle policies.
    pub retention_enabled: bool,
}

impl Default for MaintenanceOptions {
    fn default() -> Self {
        MaintenanceOptions {
            level_intervals: vec![
                Duration::from_secs(30),
                Duration::from_secs(5 * 60),
                Duration::from_secs(60 * 60),
            ],
            snapshot_interval: Duration::from_secs(24 * 60 * 60),
            snapshot_retention: Duration::from_secs(24 * 60 * 60),
            l0_retention: Duration::from_secs(5 * 60),
            retention_enabled: true,
        }
    }
}

/// What a [`Writer::maintain`] run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    /// Levels that received a new compacted file.
    pub compacted_levels: Vec<u8>,
    /// TXID of a snapshot written to level 9, if any.
    pub snapshot: Option<Txid>,
    /// Number of files deleted by retention.
    pub deleted: usize,
}

impl Writer {
    /// Runs due compactions, snapshots, and retention against the bucket.
    /// Call opportunistically (after pushes, on wifi+charging, etc.); every
    /// step is independently resumable and crash-safe (write-then-delete
    /// ordering throughout).
    ///
    /// Equal to [`Writer::maintain_with`] with a token that never cancels.
    pub fn maintain(&mut self, opts: &MaintenanceOptions) -> Result<MaintenanceReport> {
        self.maintain_with(&CancelToken::new(), opts)
    }

    /// [`Writer::maintain`], cancellable. The token is installed on the
    /// storage client and checked between levels, between compaction source
    /// files, through the snapshot encode, and between retention deletes.
    /// A cancelled maintain returns [`Error::Cancelled`]; because every step
    /// is write-then-delete, the abandoned run left the bucket valid and the
    /// next maintain simply redoes whatever was cut short.
    pub fn maintain_with(
        &mut self,
        cancel: &CancelToken,
        opts: &MaintenanceOptions,
    ) -> Result<MaintenanceReport> {
        self.client.set_cancel(cancel.clone());
        // Maintenance mutates the bucket (compaction PUTs, retention
        // DELETEs): the session lineage check must pass first.
        self.ensure_lineage_checked(cancel)?;

        let mut report = MaintenanceReport::default();
        let now = SystemTime::now();

        // Compact L(n-1)→L(n) for each configured level that is due.
        for (i, interval) in opts.level_intervals.iter().enumerate() {
            cancel.check()?;
            let level = (i + 1) as u8;
            if self.level_due(level, *interval, now)? && self.compact_level_with(cancel, level)? {
                report.compacted_levels.push(level);
                if level == 1 && opts.retention_enabled {
                    report.deleted += self.enforce_l0_retention(opts.l0_retention, now, cancel)?;
                }
            }
        }

        // Snapshot when due.
        cancel.check()?;
        if self.snapshot_due(opts.snapshot_interval, now)? {
            if let Some(txid) = self.snapshot_with(cancel)? {
                report.snapshot = Some(txid);
            }
        }

        // Snapshot retention + cascade to lower levels.
        if opts.retention_enabled {
            report.deleted += self.enforce_retention(opts, now, cancel)?;
        }

        Ok(report)
    }

    /// A level is due when it has no file yet (and sources exist) or its
    /// newest file is older than the interval. (store.go:745-759 adapted to
    /// push-relative time)
    fn level_due(&self, level: u8, interval: Duration, now: SystemTime) -> Result<bool> {
        match liters_storage::max_ltx_file_info(self.client.as_ref(), level)? {
            None => Ok(true),
            Some(newest) => match newest.created_at {
                Some(at) => Ok(now.duration_since(at).unwrap_or_default() >= interval),
                None => Ok(true),
            },
        }
    }

    fn snapshot_due(&self, interval: Duration, now: SystemTime) -> Result<bool> {
        match liters_storage::max_ltx_file_info(self.client.as_ref(), SNAPSHOT_LEVEL)? {
            None => Ok(true),
            Some(newest) => match newest.created_at {
                Some(at) => Ok(now.duration_since(at).unwrap_or_default() >= interval),
                None => Ok(true),
            },
        }
    }

    /// Merges new source-level files into one file at `dst_level`. Returns
    /// false when there is nothing to compact. (compactor.go:104-192)
    pub fn compact_level(&mut self, dst_level: u8) -> Result<bool> {
        self.compact_level_with(&CancelToken::new(), dst_level)
    }

    pub(crate) fn compact_level_with(&mut self, cancel: &CancelToken, dst_level: u8) -> Result<bool> {
        // Install THIS operation's token on the client, like every other
        // cancellable entry point. Idempotent when reached via maintain_with
        // (the same token is re-installed); load-bearing for the direct
        // public compact_level path: a token-aware backend still holds the
        // previous operation's token, and if that one was cancelled, every
        // storage call here would return Cancelled forever.
        self.client.set_cancel(cancel.clone());
        // No-op when maintain_with already checked; guards the direct
        // public-entry path (a compaction PUT mutates the bucket).
        self.ensure_lineage_checked(cancel)?;

        let src_level = dst_level - 1;
        let seek = liters_storage::max_ltx_file_info(self.client.as_ref(), dst_level)?
            .map(|f| Txid(f.max_txid.0 + 1))
            .unwrap_or(Txid(0));

        let srcs = self.client.ltx_files(src_level, seek, false)?;
        if srcs.is_empty() {
            return Ok(false);
        }

        let min_txid = srcs.iter().map(|f| f.min_txid).min().unwrap();
        let max_txid = srcs.iter().map(|f| f.max_txid).max().unwrap();

        let mut readers = Vec::with_capacity(srcs.len());
        for info in &srcs {
            cancel.check()?;
            readers.push(self.client.open_ltx_file(
                src_level,
                info.min_txid,
                info.max_txid,
                0,
                0,
            )?);
        }

        // Merge into an unlinked spool file, not RAM: device memory stays
        // O(buffer) however large the merged level grows.
        let mut compactor = ltx::Compactor::new(readers);
        compactor.header_flags = HEADER_FLAG_NO_CHECKSUM;
        let mut spool = std::io::BufWriter::new(temp_spool()?);
        compactor.compact(&mut spool)?;
        use std::io::{Seek, SeekFrom, Write};
        spool.flush()?;
        let mut spool = spool.into_inner().map_err(|e| Error::Other(e.to_string()))?;
        spool.seek(SeekFrom::Start(0))?;

        cancel.check()?;
        self.client
            .write_ltx_file(dst_level, min_txid, max_txid, &mut std::io::BufReader::new(spool))?;
        Ok(true)
    }

    /// Writes a full-image snapshot of the database at its current synced
    /// position to level 9. Returns None when there is nothing to snapshot
    /// (position zero). (db.go:2337-2480)
    ///
    /// Equal to [`Writer::snapshot_with`] with a token that never cancels.
    pub fn snapshot(&mut self) -> Result<Option<Txid>> {
        self.snapshot_with(&CancelToken::new())
    }

    /// [`Writer::snapshot`], cancellable: the token is checked before and
    /// through the encode (every 256 pages) and installed on the storage
    /// client for the upload. A cancelled snapshot leaves nothing behind
    /// (spool removed, PUT atomicity is the backend's).
    pub fn snapshot_with(&mut self, cancel: &CancelToken) -> Result<Option<Txid>> {
        self.client.set_cancel(cancel.clone());
        // The snapshot PUT mutates the bucket: lineage check first.
        self.ensure_lineage_checked(cancel)?;

        // Sync first so the snapshot position equals the WAL's committed end.
        let outcome = self.verify_and_sync(false, cancel)?;
        let synced_end = outcome.new_wal_size;
        self.apply_sync_outcome(outcome);

        // Upload the pending L0 backlog (the sync above may have grown it)
        // BEFORE the snapshot PUT: an L9 file summarizes history through its
        // max TXID, so the bucket's L0 chain must already reach that TXID or
        // bucket state regresses relative to the snapshot — readers planning
        // from the L0 listing would miss transactions the snapshot claims,
        // and a fenced liters HTTP server correctly 409s such a PUT
        // (docs/http-protocol.md "Fencing": L1..L9 max may not exceed the
        // bucket max).
        self.upload(cancel)?;

        let pos = self.pos()?;
        if pos.txid.is_zero() {
            return Ok(None);
        }

        let commit_fallback = (self.db_file.metadata()?.len() / self.page_size as u64) as u32;

        // Scan the WAL only up to the synced end: commits appended after our
        // sync must not leak into a snapshot labeled MaxTXID = pos. (The
        // read-lock transaction prevents checkpoints from moving post-sync
        // frames into the database file meanwhile.)
        let wal_file = File::open(&self.wal_path)?;
        let mut rd = WalReader::new(&wal_file)?;
        let page_map = rd.page_map_until(synced_end)?;
        let commit = if page_map.commit > 0 { page_map.commit } else { commit_fallback };

        let wal_offset = rd.offset();
        let wal_size = page_map.max_offset.saturating_sub(wal_offset);
        let (salt1, salt2) =
            if wal_offset == 0 { (0, 0) } else { (rd.salt1, rd.salt2) };

        // Encode to a local spool file, then upload. (Go streams through a
        // pipe; a spool keeps the storage call resumable/retryable.) The
        // guard removes the spool on every exit path, including cancellation.
        cancel.check()?;
        let spool = self.meta.root().join("snapshot.ltx.tmp");
        let _spool_guard = TmpGuard(&spool);
        {
            let f = File::create(&spool)?;
            let mut enc = ltx::Encoder::new(std::io::BufWriter::new(f));
            enc.encode_header(Header {
                flags: HEADER_FLAG_NO_CHECKSUM,
                page_size: self.page_size,
                commit,
                min_txid: Txid(1),
                max_txid: pos.txid,
                timestamp: crate::writer::now_unix_millis(),
                wal_offset: wal_offset as i64,
                wal_size: wal_size as i64,
                wal_salt1: salt1,
                wal_salt2: salt2,
                ..Default::default()
            })?;
            self.write_snapshot_pages(&mut enc, &wal_file, commit, &page_map.pages, cancel)?;
            let (mut w, _, _) = enc.finish()?;
            use std::io::Write;
            w.flush()?;
            w.into_inner().map_err(|e| Error::Other(e.to_string()))?.sync_all()?;
        }

        cancel.check()?;
        {
            let mut f = std::io::BufReader::new(File::open(&spool)?);
            self.client.write_ltx_file(SNAPSHOT_LEVEL, Txid(1), pos.txid, &mut f)?;
        }

        Ok(Some(pos.txid))
    }

    /// Snapshot retention by age (keep the newest even if expired), then a
    /// TXID-floor cascade over levels 1..8, then L0 grace-based deletion.
    /// (db.go:2483-2667, store.go:801-823)
    fn enforce_retention(
        &mut self,
        opts: &MaintenanceOptions,
        now: SystemTime,
        cancel: &CancelToken,
    ) -> Result<usize> {
        let mut deleted = 0usize;
        let cutoff = now.checked_sub(opts.snapshot_retention).unwrap_or(SystemTime::UNIX_EPOCH);

        let snapshots = self.client.ltx_files(SNAPSHOT_LEVEL, Txid(0), false)?;
        let Some(newest) = snapshots.iter().map(|f| f.max_txid).max() else {
            return Ok(0); // no snapshots: nothing to anchor retention on
        };

        // Delete expired snapshots, always keeping the newest. Track the
        // floor: the MaxTXID of the snapshot immediately *preceding* the
        // oldest retained one, so plans computed against a just-deleted
        // snapshot still find their chain. (db.go:2509-2521, #1325)
        let mut expired: Vec<FileInfo> = Vec::new();
        let mut floor = Txid(0);
        let mut prev_max = Txid(0);
        for info in &snapshots {
            let is_expired =
                info.created_at.map(|at| at < cutoff).unwrap_or(false) && info.max_txid != newest;
            if is_expired {
                expired.push(info.clone());
                prev_max = info.max_txid;
            } else {
                // Oldest retained snapshot: floor is the previous one's max.
                floor = prev_max;
                break;
            }
        }
        deleted += expired.len();
        cancel.check()?;
        self.client.delete_ltx_files(&expired)?;

        // Cascade: delete level 1..8 files wholly below the floor, keeping at
        // least one file per level. (compactor.go:291-337)
        if !floor.is_zero() {
            for level in 1..SNAPSHOT_LEVEL {
                cancel.check()?;
                let files = self.client.ltx_files(level, Txid(0), false)?;
                if files.len() <= 1 {
                    continue;
                }
                let deletable: Vec<FileInfo> = files[..files.len() - 1]
                    .iter()
                    .filter(|f| f.max_txid < floor)
                    .cloned()
                    .collect();
                deleted += deletable.len();
                self.client.delete_ltx_files(&deletable)?;
            }
        }

        deleted += self.enforce_l0_retention(opts.l0_retention, now, cancel)?;
        Ok(deleted)
    }

    /// Deletes L0 files that are (a) fully covered by L1 and (b) older than
    /// the grace period, stopping at the first file that fails either test so
    /// coverage never gains a gap. The newest L0 file always survives.
    /// (db.go:2545-2667)
    fn enforce_l0_retention(
        &mut self,
        grace: Duration,
        now: SystemTime,
        cancel: &CancelToken,
    ) -> Result<usize> {
        let max_l1 = liters_storage::max_ltx_file_info(self.client.as_ref(), 1)?
            .map(|f| f.max_txid)
            .unwrap_or_default();
        if max_l1.is_zero() {
            return Ok(0);
        }
        let cutoff = now.checked_sub(grace).unwrap_or(SystemTime::UNIX_EPOCH);

        let files = self.client.ltx_files(0, Txid(0), false)?;
        let Some(newest) = files.iter().map(|f| f.max_txid).max() else { return Ok(0) };

        let mut deletable: Vec<FileInfo> = Vec::new();
        for info in &files {
            if info.max_txid > max_l1 {
                break; // not covered by L1 yet
            }
            if info.max_txid == newest {
                break; // never delete the newest L0
            }
            match info.created_at {
                Some(at) if at <= cutoff => deletable.push(info.clone()),
                // Ordered listing: stop at the first too-recent file so we
                // never create a gap. (db.go:2605-2611)
                _ => break,
            }
        }
        let n = deletable.len();
        cancel.check()?;
        self.client.delete_ltx_files(&deletable)?;
        Ok(n)
    }

    /// Snapshot-page source: WAL overlay first, then the raw database file —
    /// same as the writer's snapshot sync path but against an explicit page
    /// map. (db.go:2063-2108)
    fn write_snapshot_pages<W: std::io::Write>(
        &self,
        enc: &mut ltx::Encoder<W>,
        wal_file: &File,
        commit: u32,
        page_map: &std::collections::HashMap<u32, u64>,
        cancel: &CancelToken,
    ) -> Result<()> {
        use liters_wal::{ReadAt, WAL_FRAME_HEADER_SIZE};
        let lock_pgno = ltx::lock_pgno(self.page_size);
        let mut data = vec![0u8; self.page_size as usize];

        for pgno in 1..=commit {
            if pgno % 256 == 0 {
                cancel.check()?;
            }
            if pgno == lock_pgno {
                continue;
            }
            if let Some(&offset) = page_map.get(&pgno) {
                let n = wal_file.read_at(&mut data, offset + WAL_FRAME_HEADER_SIZE)?;
                if n < data.len() {
                    return Err(Error::Other(format!("short wal read for page {pgno}")));
                }
            } else {
                let n = self.db_file.read_at(&mut data, (pgno as u64 - 1) * self.page_size as u64)?;
                if n < data.len() {
                    return Err(Error::Other(format!("short db read for page {pgno}")));
                }
            }
            enc.encode_page(pgno, &data)?;
        }
        Ok(())
    }
}

/// Anonymous read+write spool file in the OS temp dir, unlinked immediately
/// after creation so the open fd is its only reference and it can never
/// outlive the process (same idiom as the HTTP client's response spool).
fn temp_spool() -> Result<File> {
    let dir = std::env::temp_dir();
    for attempt in 0u32..16 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let path = dir.join(format!(
            "liters-compact-{}-{nanos:x}-{attempt}.spool",
            std::process::id()
        ));
        match std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&path) {
            Ok(f) => {
                let _ = std::fs::remove_file(&path);
                return Ok(f);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err(Error::Other("could not create compaction spool file".into()))
}
