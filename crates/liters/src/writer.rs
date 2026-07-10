//! The write side: converts committed WAL content into single-TXID L0 LTX
//! files and uploads them, driven by explicit [`Writer::push`] calls.
//! A faithful port of litestream's db.go sync pipeline, minus the daemon.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use liters_wal::{calc_wal_size, ReadAt, WalError, WalReader, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};
use liters_storage::ReplicaClient;
use ltx::{Encoder, Header, Pos, Txid, HEADER_FLAG_NO_CHECKSUM};
use rusqlite::Connection;

use crate::meta::MetaDir;
use crate::sqlite::{self, ReadLock};
use crate::verify::{self, SyncInfo, SyncState};
use crate::{Error, Result};

/// Checkpoint modes. RESTART is deliberately absent from automatic use
/// (litestream removed it; see db.go:60-63).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointMode {
    Passive,
    Truncate,
}

impl CheckpointMode {
    pub(crate) fn as_sql(self) -> &'static str {
        match self {
            CheckpointMode::Passive => "PASSIVE",
            CheckpointMode::Truncate => "TRUNCATE",
        }
    }
}

/// Tunables, defaulted to litestream's. (db.go:31-41)
#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub busy_timeout: Duration,
    /// PASSIVE checkpoint threshold, in WAL pages. (db.go:34)
    pub min_checkpoint_page_n: u32,
    /// Emergency TRUNCATE checkpoint threshold, in WAL pages (~500MB @4K).
    /// 0 disables. (db.go:35)
    pub truncate_page_n: u32,
    /// Whether push() runs threshold-based checkpoints automatically.
    pub auto_checkpoint: bool,
}

impl Default for WriterOptions {
    fn default() -> Self {
        WriterOptions {
            busy_timeout: Duration::from_secs(1),
            min_checkpoint_page_n: 1000,
            truncate_page_n: 121_359,
            auto_checkpoint: true,
        }
    }
}

/// Result of a [`Writer::push`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushResult {
    /// Local replication position after the push.
    pub txid: Txid,
    /// Whether a new L0 file was created by this push.
    pub synced: bool,
    /// Number of L0 files uploaded to the bucket by this push.
    pub uploaded: u64,
    /// The bucket's max L0 TXID after the push.
    pub remote_txid: Txid,
    /// Whether a checkpoint ran during this push.
    pub checkpointed: bool,
}

pub(crate) struct SyncOutcome {
    pub synced: bool,
    pub pos: Option<Pos>,
    pub new_wal_size: u64,
    pub synced_to_wal_end: bool,
}

/// Replicates one local SQLite database to a replica bucket.
pub struct Writer {
    pub(crate) db_path: PathBuf,
    pub(crate) wal_path: PathBuf,
    pub(crate) meta: MetaDir,
    /// Main connection: checkpoints, `_litestream_seq` writes, write-lock txs.
    pub(crate) conn: Connection,
    /// Dedicated connection holding the long-running read transaction.
    pub(crate) read_conn: Connection,
    pub(crate) read_lock: ReadLock,
    /// Long-lived raw fd for direct page reads; opening/closing extra fds on
    /// the database file would break POSIX locks. (db.go:1000-1003)
    pub(crate) db_file: File,
    pub(crate) page_size: u32,
    pub(crate) client: Box<dyn ReplicaClient>,
    pub(crate) opts: WriterOptions,
    pub(crate) state: SyncState,
    pub(crate) cached_pos: Option<Pos>,
    /// Bucket-side max L0 TXID; None = unknown, re-derive by listing.
    pub(crate) remote_txid: Option<Txid>,
}

impl Writer {
    /// Opens a writer against a live database. The database file must exist.
    /// (db.go init, 965-1081)
    pub fn open(
        db_path: impl Into<PathBuf>,
        client: Box<dyn ReplicaClient>,
        opts: WriterOptions,
    ) -> Result<Writer> {
        let db_path: PathBuf = db_path.into();
        if !db_path.exists() {
            return Err(Error::Other(format!("database file does not exist: {db_path:?}")));
        }
        let wal_path = wal_path_for(&db_path);
        let meta = MetaDir::for_db(&db_path);
        meta.create_dirs()?;
        meta.remove_tmp_files()?;

        let conn = sqlite::open_conn(&db_path, opts.busy_timeout)?;
        sqlite::enable_wal(&conn)?;
        sqlite::create_meta_tables(&conn)?;

        let read_conn = sqlite::open_conn(&db_path, opts.busy_timeout)?;
        let mut read_lock = ReadLock::new();
        read_lock.acquire(&read_conn)?;

        let db_file = File::open(&db_path)?;

        let page_size: u32 = conn.query_row("PRAGMA page_size;", [], |r| r.get(0))?;
        if page_size == 0 {
            return Err(Error::Other("invalid db page size".into()));
        }

        // Recover the advisory checkpoint-threshold offset from the previous
        // process lifetime. `synced_to_wal_end` deliberately starts false
        // (see MetaDir::write_sync_state).
        let state = SyncState {
            last_synced_wal_offset: meta.read_sync_state(),
            synced_to_wal_end: false,
            synced_since_checkpoint: false,
        };

        let mut w = Writer {
            db_path,
            wal_path,
            meta,
            conn,
            read_conn,
            read_lock,
            db_file,
            page_size,
            client,
            opts,
            state,
            cached_pos: None,
            remote_txid: None,
        };

        w.ensure_wal_exists()?;
        w.check_behind_remote()?;
        Ok(w)
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// The database this writer replicates.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Current local replication position (TXID of the newest local L0).
    pub fn pos(&mut self) -> Result<Pos> {
        if let Some(pos) = self.cached_pos {
            return Ok(pos);
        }
        let pos = self.meta.pos()?;
        self.cached_pos = Some(pos);
        Ok(pos)
    }

    pub(crate) fn invalidate_pos(&mut self) {
        self.cached_pos = None;
    }

    /// Syncs committed WAL content into L0, maybe checkpoints, and uploads
    /// pending L0 files to the bucket. The whole hot path. (db.go syncLocked
    /// + replica.go Sync)
    pub fn push(&mut self) -> Result<PushResult> {
        self.ensure_wal_exists()?;

        // Logical WAL size for checkpoint thresholds (issue #997).
        let orig_wal_size = if self.state.last_synced_wal_offset > 0 {
            self.state.last_synced_wal_offset
        } else {
            self.wal_file_size()?
        };

        let outcome = self.verify_and_sync(false)?;
        let synced = outcome.synced;
        let new_wal_size = outcome.new_wal_size;
        self.apply_sync_outcome(outcome);
        if synced {
            self.state.synced_since_checkpoint = true;
        }

        let mut checkpointed = false;
        if self.opts.auto_checkpoint {
            checkpointed = self.checkpoint_if_needed(orig_wal_size, new_wal_size)?;
        }

        let (uploaded, remote_txid) = self.upload()?;

        // Uploaded L0s are prunable; always keep the newest (verify needs it).
        self.meta.prune_l0(remote_txid)?;

        Ok(PushResult {
            txid: self.pos()?.txid,
            synced,
            uploaded,
            remote_txid,
            checkpointed,
        })
    }

    /// Runs verify() + sync(): the WAL→L0 conversion. State is only mutated
    /// by the caller applying the returned outcome (executor discipline:
    /// failed syncs must not corrupt state; db_internal_test.go:2252).
    pub(crate) fn verify_and_sync(&mut self, checkpointing: bool) -> Result<SyncOutcome> {
        let pos = self.pos()?;
        let info = verify::verify(&self.meta, &self.wal_path, self.page_size, pos.txid, &self.state)?;
        self.sync(checkpointing, pos, info)
    }

    pub(crate) fn apply_sync_outcome(&mut self, outcome: SyncOutcome) {
        self.state.last_synced_wal_offset = outcome.new_wal_size;
        self.state.synced_to_wal_end = outcome.synced_to_wal_end;
        if let Some(pos) = outcome.pos {
            self.cached_pos = Some(pos);
        }
        // Persist across app restarts (advisory; see MetaDir::write_sync_state).
        let _ = self.meta.write_sync_state(self.state.last_synced_wal_offset);
    }

    /// WAL→LTX conversion for one push batch. (db.go sync, 1807-2061)
    fn sync(&mut self, _checkpointing: bool, pos: Pos, mut info: SyncInfo) -> Result<SyncOutcome> {
        let mut result = SyncOutcome {
            synced: false,
            pos: None,
            new_wal_size: self.state.last_synced_wal_offset,
            synced_to_wal_end: self.state.synced_to_wal_end && !info.clear_synced_to_wal_end,
        };

        let txid = Txid(pos.txid.0 + 1);

        // Database size in pages, from the long-lived fd; overridden by the
        // WAL's last commit record below. (db.go:1846-1851)
        let mut commit = (self.db_file.metadata()?.len() / self.page_size as u64) as u32;

        let wal_file = File::open(&self.wal_path)?;
        let mut rd = if info.offset == WAL_HEADER_SIZE {
            WalReader::new(&wal_file)?
        } else {
            match WalReader::with_offset(&wal_file, info.offset, info.salt1, info.salt2) {
                Ok(rd) => rd,
                Err(WalError::PrevFrameMismatch) => {
                    // The resume frame vanished between verify and here; fall
                    // back to reading the whole WAL. (db.go:1866-1877)
                    info.offset = WAL_HEADER_SIZE;
                    WalReader::new(&wal_file)?
                }
                Err(e) => return Err(e.into()),
            }
        };

        let page_map = rd.page_map()?;
        if page_map.commit > 0 {
            commit = page_map.commit;
        }

        let sz = if page_map.max_offset > 0 {
            page_map.max_offset.checked_sub(info.offset).ok_or_else(|| {
                Error::Other(format!(
                    "wal size must be positive: maxOffset={}, offset={}",
                    page_map.max_offset, info.offset
                ))
            })?
        } else {
            0
        };

        // No new committed pages and no snapshot required: no-op push.
        // (db.go:1913-1916)
        if !info.snapshotting && sz == 0 {
            return Ok(result);
        }

        // Encode the L0 file to a temp path, fsync, rename. (db.go:1918-2020)
        let final_path = self.meta.l0_path(txid);
        std::fs::create_dir_all(final_path.parent().unwrap())?;
        let tmp_path = final_path.with_extension("ltx.tmp");
        let tmp_guard = TmpGuard(&tmp_path);

        let ltx_file = File::create(&tmp_path)?;
        let mut enc = Encoder::new(ltx_file);
        enc.encode_header(Header {
            flags: HEADER_FLAG_NO_CHECKSUM,
            page_size: self.page_size,
            commit,
            min_txid: txid,
            max_txid: txid,
            timestamp: now_unix_millis(),
            pre_apply_checksum: ltx::Checksum(0),
            wal_offset: info.offset as i64,
            wal_size: sz as i64,
            wal_salt1: rd.salt1,
            wal_salt2: rd.salt2,
            node_id: 0,
        })?;

        if info.snapshotting {
            self.write_ltx_from_db(&mut enc, &wal_file, commit, &page_map.pages)?;
        } else {
            self.write_ltx_from_wal(&mut enc, &wal_file, &page_map.pages)?;
        }

        let (ltx_file, _, _) = enc.finish()?;
        let n = ltx_file.metadata()?.len();
        ltx_file.sync_all()?;
        drop(ltx_file);

        if let Err(e) = std::fs::rename(&tmp_path, &final_path) {
            self.invalidate_pos();
            return Err(Error::Io(e));
        }
        drop(tmp_guard);

        // Fsync the L0 dir so the rename survives power loss (improvement
        // over Go, which skips this).
        if let Ok(d) = File::open(final_path.parent().unwrap()) {
            let _ = d.sync_all();
        }
        debug_assert!(n > 0);

        result.synced = true;
        result.pos = Some(Pos { txid, post_apply_checksum: ltx::Checksum(0) });

        // Logical end of consumed WAL content (issue #997) and whether we
        // consumed the WAL exactly to its end (issue #927).
        let final_offset = info.offset + sz;
        result.new_wal_size = final_offset;
        result.synced_to_wal_end = final_offset == self.wal_file_size()?;

        Ok(result)
    }

    /// Snapshot page source: WAL page map first, then the raw database file.
    /// The read-lock transaction makes the raw reads consistent. (db.go:2063-2108)
    fn write_ltx_from_db(
        &self,
        enc: &mut Encoder<File>,
        wal_file: &File,
        commit: u32,
        page_map: &std::collections::HashMap<u32, u64>,
    ) -> Result<()> {
        let lock_pgno = ltx::lock_pgno(self.page_size);
        let mut data = vec![0u8; self.page_size as usize];

        for pgno in 1..=commit {
            if pgno == lock_pgno {
                continue;
            }
            if let Some(&offset) = page_map.get(&pgno) {
                read_full_at(wal_file, &mut data, offset + WAL_FRAME_HEADER_SIZE)?;
            } else {
                read_full_at(&self.db_file, &mut data, (pgno as u64 - 1) * self.page_size as u64)?;
            }
            enc.encode_page(pgno, &data)?;
        }
        Ok(())
    }

    /// Incremental page source: just the WAL page map, ascending. (db.go:2110-2137)
    fn write_ltx_from_wal(
        &self,
        enc: &mut Encoder<File>,
        wal_file: &File,
        page_map: &std::collections::HashMap<u32, u64>,
    ) -> Result<()> {
        let mut pgnos: Vec<u32> = page_map.keys().copied().collect();
        pgnos.sort_unstable();

        let mut data = vec![0u8; self.page_size as usize];
        for pgno in pgnos {
            let offset = page_map[&pgno];
            read_full_at(wal_file, &mut data, offset + WAL_FRAME_HEADER_SIZE)?;
            enc.encode_page(pgno, &data)?;
        }
        Ok(())
    }

    /// Uploads local L0 files the bucket is missing, in TXID order.
    /// (replica.go:134-216)
    pub(crate) fn upload(&mut self) -> Result<(u64, Txid)> {
        let remote = match self.remote_txid {
            Some(t) => t,
            None => {
                let t = liters_storage::max_ltx_file_info(self.client.as_ref(), 0)?
                    .map(|f| f.max_txid)
                    .unwrap_or_default();
                self.remote_txid = Some(t);
                t
            }
        };

        let local = self.pos()?.txid;
        let mut uploaded = 0u64;
        let mut cursor = remote;
        while cursor < local {
            let next = Txid(cursor.0 + 1);
            let path = self.meta.l0_path(next);
            let f = match File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    // Local gap: unrecoverable without a reset/snapshot.
                    self.remote_txid = None;
                    return Err(Error::LocalLtx { txid: next, msg: format!("open {path:?}: {e}") });
                }
            };
            let mut rd = std::io::BufReader::new(f);
            if let Err(e) = self.client.write_ltx_file(0, next, next, &mut rd) {
                // Re-derive the remote position on the next push; uploads are
                // idempotent PUTs so double-upload is harmless. (replica.go:198)
                self.remote_txid = None;
                return Err(e.into());
            }
            cursor = next;
            self.remote_txid = Some(cursor);
            uploaded += 1;
        }
        Ok((uploaded, cursor))
    }

    /// If the WAL is missing or headerless, force a write so it exists.
    /// (db.go:1388-1398)
    pub(crate) fn ensure_wal_exists(&self) -> Result<()> {
        if let Ok(m) = std::fs::metadata(&self.wal_path) {
            if m.len() >= WAL_HEADER_SIZE {
                return Ok(());
            }
        }
        sqlite::write_seq(&self.conn)
    }

    /// Device-restore protection: if the bucket is ahead of the local
    /// database (the DB was restored from an older copy), rebaseline local
    /// L0 state from the bucket so the next push snapshots at remoteMax+1
    /// instead of colliding TXIDs. (db.go:1400-1483, issue #781)
    fn check_behind_remote(&mut self) -> Result<()> {
        let db_pos = self.pos()?;
        let Some(remote) = liters_storage::max_ltx_file_info(self.client.as_ref(), 0)? else {
            return Ok(()); // no remote data yet
        };
        if remote.max_txid.is_zero() || db_pos.txid >= remote.max_txid {
            return Ok(());
        }

        // Clear local L0 and adopt the newest remote L0 as the baseline.
        self.meta.clear_l0()?;
        self.invalidate_pos();

        let mut rd = self
            .client
            .open_ltx_file(0, remote.min_txid, remote.max_txid, 0, 0)?;
        let local_path = self.meta.l0_path(remote.max_txid);
        let tmp_path = local_path.with_extension("ltx.tmp");
        {
            let mut f = File::create(&tmp_path)?;
            std::io::copy(&mut rd, &mut f)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp_path, &local_path)?;
        self.invalidate_pos();
        Ok(())
    }

    pub(crate) fn wal_file_size(&self) -> Result<u64> {
        match std::fs::metadata(&self.wal_path) {
            Ok(m) => Ok(m.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    /// Threshold-based checkpointing, in litestream's priority order.
    /// Returns whether a checkpoint ran. (db.go:1281-1345, minus the
    /// time-based tier which explicit pushes make redundant)
    fn checkpoint_if_needed(&mut self, orig_wal_size: u64, new_wal_size: u64) -> Result<bool> {
        if self.page_size == 0 {
            return Ok(false);
        }

        // Priority 1: emergency TRUNCATE (blocking) to bound WAL growth.
        if self.opts.truncate_page_n > 0
            && orig_wal_size >= calc_wal_size(self.page_size, self.opts.truncate_page_n as u64)
        {
            self.checkpoint(CheckpointMode::Truncate)?;
            return Ok(true);
        }

        // Priority 2: PASSIVE at the min threshold; SQLITE_BUSY is expected
        // under contention and swallowed.
        if new_wal_size >= calc_wal_size(self.page_size, self.opts.min_checkpoint_page_n as u64) {
            match self.checkpoint(CheckpointMode::Passive) {
                Ok(()) => return Ok(true),
                Err(e) if sqlite::is_busy_error(&e) => return Ok(false),
                Err(e) => return Err(e),
            }
        }

        Ok(false)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        let _ = self.read_lock.release(&self.read_conn);
    }
}

/// Deletes the tmp file on drop; disarmed by successful rename (the rename
/// makes the delete a no-op on the moved path).
struct TmpGuard<'a>(&'a Path);

impl Drop for TmpGuard<'_> {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0);
    }
}

pub(crate) fn wal_path_for(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_owned();
    s.push("-wal");
    PathBuf::from(s)
}

pub(crate) fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn read_full_at(f: &File, buf: &mut [u8], offset: u64) -> Result<()> {
    let n = f.read_at(buf, offset)?;
    if n < buf.len() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("short read at offset {offset}"),
        )));
    }
    Ok(())
}
