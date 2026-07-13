//! The write side: converts committed WAL content into single-TXID L0 LTX
//! files and uploads them, driven by explicit [`Writer::push`] calls.
//! A faithful port of litestream's db.go sync pipeline, minus the daemon.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use liters_wal::{calc_wal_size, ReadAt, WalError, WalReader, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};
use liters_storage::{CancelToken, ReplicaClient};
use ltx::{Encoder, FileInfo, Header, Pos, Txid, HEADER_FLAG_NO_CHECKSUM};
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
    /// Highest TXID this local lineage KNOWS it has successfully placed in
    /// the bucket (persisted in the meta dir's `verified-pos` file). Drives
    /// the lineage check; see [`Writer::ensure_lineage_checked`].
    pub(crate) verified_pos: Txid,
    /// True when `verified_pos` was seeded from local L0 evidence because the
    /// meta dir predates the `verified-pos` file (litestream-Go handoff, or
    /// pre-verified-pos liters). A provisional baseline may legitimately sit
    /// AHEAD of the bucket (offline backlog), so the lineage check compares
    /// it with litestream's original `remote > local` rule and then migrates
    /// to the persisted scheme; a non-provisional baseline is exact, so any
    /// bucket max other than it (ahead OR behind) is foreign data.
    pub(crate) verified_pos_provisional: bool,
    /// Whether the lineage check has succeeded this session. `upload()` must
    /// never run before it has.
    pub(crate) lineage_checked: bool,
}

impl Writer {
    /// Opens a writer against a live database. The database file must exist.
    /// (db.go init, 965-1081)
    ///
    /// Performs NO network I/O: a writer can be constructed with the bucket
    /// unreachable, and pushes accumulate local L0 files until it becomes
    /// reachable (the first successful push then uploads the backlog). The
    /// device-restore check litestream runs at open (db.go:1400-1483) is
    /// deferred to the first bucket mutation; see
    /// [`Writer::ensure_lineage_checked`].
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

        // Lineage baseline for ensure_lineage_checked. The verified-pos file
        // where present; meta dirs that predate it (litestream's own, or
        // pre-verified-pos liters) fall back to the local L0 evidence — the
        // newest local L0 is the position that lineage pushed (or was about
        // to push), which is exactly what litestream's open-time
        // check_behind_remote compared against, so the Go→Rust handoff does
        // not read its own bucket as foreign. A genuinely FRESH meta dir (no
        // file, no L0s) pins the lineage at zero immediately, so L0s
        // accumulated offline from birth can never masquerade as bucket
        // lineage when the first contact finds a foreign-seeded bucket.
        let (verified_pos, verified_pos_provisional) = match meta.read_verified_pos() {
            Some(t) => (t, false),
            None => match meta.max_l0_txid()? {
                Some(t) => (t, true),
                None => {
                    meta.write_verified_pos(Txid(0))?;
                    (Txid(0), false)
                }
            },
        };

        let w = Writer {
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
            verified_pos,
            verified_pos_provisional,
            lineage_checked: false,
        };

        w.ensure_wal_exists()?;
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
    ///
    /// Equal to [`Writer::push_with`] with a token that never cancels.
    pub fn push(&mut self) -> Result<PushResult> {
        self.push_with(&CancelToken::new())
    }

    /// [`Writer::push`], cancellable. The token is installed on the storage
    /// client (so a token-aware backend can interrupt in-flight transfers)
    /// and checked at every natural boundary: before the WAL sync, every 256
    /// pages while encoding, before checkpointing, and between uploaded
    /// files. A cancelled push returns [`Error::Cancelled`] and is
    /// indistinguishable from a kill-then-restart: staged tmp files are
    /// cleaned up, completed L0 files persist, and the next push resumes
    /// exactly where this one stopped (uploads are idempotent PUTs).
    pub fn push_with(&mut self, cancel: &CancelToken) -> Result<PushResult> {
        self.client.set_cancel(cancel.clone());
        cancel.check()?;
        self.ensure_wal_exists()?;

        // Logical WAL size for checkpoint thresholds (issue #997).
        let orig_wal_size = if self.state.last_synced_wal_offset > 0 {
            self.state.last_synced_wal_offset
        } else {
            self.wal_file_size()?
        };

        // WAL→L0 conversion runs BEFORE the lineage check on purpose: with
        // the bucket unreachable the check fails, but the conversion must
        // still land committed WAL content in local L0 files so a later
        // checkpoint cannot orphan it (offline pushes accumulate a backlog).
        let outcome = self.verify_and_sync(false, cancel)?;
        let synced = outcome.synced;
        let new_wal_size = outcome.new_wal_size;
        self.apply_sync_outcome(outcome);
        if synced {
            self.state.synced_since_checkpoint = true;
        }

        cancel.check()?;
        let mut checkpointed = false;
        if self.opts.auto_checkpoint {
            checkpointed = self.checkpoint_if_needed(orig_wal_size, new_wal_size)?;
        }

        // First bucket mutation of the session: the lineage check must pass
        // before upload() may run (a rebaseline here discards the L0s the
        // conversion above created — see ensure_lineage_checked for why that
        // is the correct outcome).
        self.ensure_lineage_checked(cancel)?;
        let (uploaded, remote_txid) = self.upload(cancel)?;

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

    /// Recovery for [`Error::LocalLtx`]: wipes local L0 state, the cached
    /// position, and the verified position, so the next push re-derives
    /// everything from the bucket — rebaselining onto the bucket head (and
    /// snapshotting right after) when the bucket has data, or snapshotting
    /// from scratch when it is empty.
    pub fn reset_local(&mut self) -> Result<()> {
        // Zero the verified position BEFORE wiping L0: if the wipe fails
        // midway, a stale high verified-pos over a gapped local L0 chain
        // would skip the rebaseline and strand low-TXID L0s below the
        // bucket's cursor forever.
        self.verified_pos = Txid(0);
        self.meta.write_verified_pos(Txid(0))?;
        self.verified_pos_provisional = false;
        self.lineage_checked = false;
        self.meta.clear_l0()?;
        self.invalidate_pos();
        self.remote_txid = None;
        self.state.synced_to_wal_end = false;
        Ok(())
    }

    /// Runs verify() + sync(): the WAL→L0 conversion. State is only mutated
    /// by the caller applying the returned outcome (executor discipline:
    /// failed syncs must not corrupt state; db_internal_test.go:2252) — which
    /// also makes cancellation mid-encode safe: the tmp file is removed and
    /// no state moved.
    pub(crate) fn verify_and_sync(
        &mut self,
        checkpointing: bool,
        cancel: &CancelToken,
    ) -> Result<SyncOutcome> {
        let pos = self.pos()?;
        let info = verify::verify(&self.meta, &self.wal_path, self.page_size, pos.txid, &self.state)?;
        self.sync(checkpointing, pos, info, cancel)
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
    fn sync(
        &mut self,
        _checkpointing: bool,
        pos: Pos,
        mut info: SyncInfo,
        cancel: &CancelToken,
    ) -> Result<SyncOutcome> {
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
        // Even so, report the true consumed end from verify() (derived from
        // the newest L0 header) rather than the advisory persisted value: a
        // stale or missing sync-state file must never shrink the bound that
        // snapshot() scans the WAL to, or committed frames silently fall out
        // of the snapshot. (db.go:1913-1916)
        if !info.snapshotting && sz == 0 {
            result.new_wal_size = info.offset;
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
            self.write_ltx_from_db(&mut enc, &wal_file, commit, &page_map.pages, cancel)?;
        } else {
            self.write_ltx_from_wal(&mut enc, &wal_file, &page_map.pages, cancel)?;
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
        cancel: &CancelToken,
    ) -> Result<()> {
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
        cancel: &CancelToken,
    ) -> Result<()> {
        let mut pgnos: Vec<u32> = page_map.keys().copied().collect();
        pgnos.sort_unstable();

        let mut data = vec![0u8; self.page_size as usize];
        for (i, pgno) in pgnos.into_iter().enumerate() {
            if i % 256 == 0 {
                cancel.check()?;
            }
            let offset = page_map[&pgno];
            read_full_at(wal_file, &mut data, offset + WAL_FRAME_HEADER_SIZE)?;
            enc.encode_page(pgno, &data)?;
        }
        Ok(())
    }

    /// Uploads local L0 files the bucket is missing, in TXID order.
    /// (replica.go:134-216)
    ///
    /// Callers must have passed the session lineage check first
    /// ([`Writer::ensure_lineage_checked`]): appending to an unverified
    /// bucket could interleave this lineage's TXIDs with a foreign writer's.
    pub(crate) fn upload(&mut self, cancel: &CancelToken) -> Result<(u64, Txid)> {
        debug_assert!(self.lineage_checked, "upload() before the session lineage check");
        let remote = match self.remote_txid {
            Some(t) => t,
            None => {
                let t = liters_storage::max_ltx_file_info(self.client.as_ref(), 0)?
                    .map(|f| f.max_txid)
                    .unwrap_or_default();
                // Mid-session re-derivation (the cursor was invalidated by an
                // earlier upload error/cancel). The lineage check ran once at
                // session start, so re-validate here: `verified_pos` advances
                // per successfully uploaded file, meaning a bucket max EQUAL
                // to it is exactly our own data — but a bucket beyond it
                // holds TXIDs we did not put there (a foreign writer appended
                // since the check), and a bucket below it lost files we
                // verified placing (wiped/reseeded). Appending local L0s over
                // either would interleave two lineages into one chain —
                // silent corruption for `litestream restore`. Fail the push
                // (non-transient) and clear the session flag so the NEXT
                // bucket mutation re-runs ensure_lineage_checked, which owns
                // the rebaseline/reseed decision. The lost-ack case (our PUT
                // landed but the response was lost, bucket exactly one ahead
                // of verified) takes this path too and costs one spurious
                // rebaseline+snapshot — the documented price of a lost
                // verified-pos update, never a correctness issue.
                if t != self.verified_pos {
                    self.lineage_checked = false;
                    return Err(Error::Other(format!(
                        "bucket moved outside this session's verified position \
                         (bucket at {t}, session verified {}): foreign writer \
                         suspected; the next push re-runs the lineage check",
                        self.verified_pos
                    )));
                }
                self.remote_txid = Some(t);
                t
            }
        };

        let local = self.pos()?.txid;
        let mut uploaded = 0u64;
        let mut cursor = remote;
        while cursor < local {
            // Cancelled between files: re-derive the remote position on the
            // next push, same as any upload error.
            if let Err(e) = cancel.check() {
                self.remote_txid = None;
                return Err(e.into());
            }
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
            // This lineage just placed `next` in the bucket: advance the
            // verified position immediately (per file, not per push) so that
            // a kill mid-backlog cannot leave the bucket ahead of what the
            // lineage remembers verifying — that would read as foreign data
            // and force a spurious rebaseline next session. Best-effort like
            // write_sync_state; a lost write costs one extra snapshot, never
            // correctness.
            if cursor > self.verified_pos {
                self.verified_pos = cursor;
                let _ = self.meta.write_verified_pos(cursor);
            }
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

    /// Once-per-session lineage check, run lazily before the FIRST bucket
    /// mutation (uploads, compactions, snapshots, retention) — never at
    /// open(), so construction and offline pushes need no network.
    ///
    /// `verified_pos` is the highest TXID this local lineage KNOWS it has
    /// placed in the bucket (advanced per uploaded file, persisted in the
    /// meta dir). Because it only advances after an acknowledged PUT, and
    /// neither liters retention nor litestream ever deletes the newest L0,
    /// a bucket whose max L0 TXID differs from it in EITHER direction holds
    /// a history this lineage did not write:
    ///
    /// - **ahead** (`remote_max > verified_pos`): the device was restored
    ///   from a backup (the bucket kept advancing after the backup was
    ///   taken) or another writer owns the bucket;
    /// - **behind but non-empty** (`0 < remote_max < verified_pos`): the
    ///   bucket was wiped and reseeded by another writer — its files at or
    ///   below remote_max describe the reseeder's transactions, not ours;
    /// - **empty** (`remote_max == 0 < verified_pos`): the bucket was wiped
    ///   outright.
    ///
    /// Comparing against `verified_pos` instead of the local position (as
    /// litestream's open-time check does, db.go:1400-1483) is what keeps the
    /// check sound with offline-accumulated L0s — a local position ahead of
    /// the bucket is normal offline backlog, but a bucket that differs from
    /// what WE verified is always foreign data. (A *provisional* baseline —
    /// legacy meta dir without a verified-pos file, seeded from local L0
    /// evidence at open — may legitimately sit ahead of the bucket, so it
    /// keeps litestream's original `remote > local` rule and is migrated to
    /// the persisted scheme on its first passing check.)
    ///
    /// On foreign data ahead-or-behind (non-empty), rebaseline exactly like
    /// litestream's device-restore path: clear local L0, adopt the newest
    /// remote L0 as the baseline, and let the next verify() force the
    /// snapshot path at remoteMax+1 (the adopted header's wal_offset/salts
    /// describe a WAL this device never had, so verify takes its
    /// truncated-WAL or overwritten-WAL branch; `synced_to_wal_end` is
    /// cleared so the #927 shortcut cannot bypass that). On a wiped-empty
    /// bucket, reseed from scratch: clear local L0 and zero the verified
    /// position, so the next push snapshots the full database from TXID 1.
    /// Either way the rebaseline discards offline-accumulated L0s and the
    /// next push snapshots — the L0s continued a history the bucket no
    /// longer tells, and uploading them as incrementals over a foreign base
    /// would corrupt every restore.
    ///
    /// A listing error propagates (transient: the caller retries on its next
    /// push) and leaves the session unchecked, so upload() still cannot run.
    /// (Replaces the open-time check_behind_remote; issue #781.)
    pub(crate) fn ensure_lineage_checked(&mut self, cancel: &CancelToken) -> Result<()> {
        if self.lineage_checked {
            return Ok(());
        }
        cancel.check()?;
        let remote = liters_storage::max_ltx_file_info(self.client.as_ref(), 0)?;
        let remote_max = remote.as_ref().map(|f| f.max_txid).unwrap_or_default();

        let foreign = if self.verified_pos_provisional {
            // Legacy baseline from local L0 evidence: only a bucket AHEAD of
            // it is provably foreign (litestream's own open-time rule).
            remote_max > self.verified_pos
        } else {
            remote_max != self.verified_pos
        };

        if foreign {
            if let Some(remote) = &remote {
                self.rebaseline_from_remote(cancel, remote)?;
            } else {
                // Wiped bucket (empty, but this lineage verified placing
                // files in it): full reseed. Clearing L0 zeroes the local
                // position, so the next push snapshots from TXID 1.
                self.meta.clear_l0()?;
                self.invalidate_pos();
                self.state.synced_to_wal_end = false;
            }
            self.verified_pos = remote_max;
            // Best-effort like write_sync_state: a lost write can only cause
            // a spurious re-rebaseline next session, never a missed one.
            let _ = self.meta.write_verified_pos(remote_max);
        } else {
            // A passing check proves the bucket's contents (all at or below
            // remote_max) came from this lineage. Align the baseline to what
            // the bucket itself proves — for a provisional (legacy) baseline
            // this lowers the in-memory value to remote_max, so upload()'s
            // per-file `cursor > verified_pos` persistence fires for the
            // backlog (local L0s beyond remote_max are unverified until
            // uploaded) — and migrate/repair the verified-pos file where it
            // is absent or stale.
            self.verified_pos = remote_max;
            if self.meta.read_verified_pos() != Some(remote_max) {
                let _ = self.meta.write_verified_pos(remote_max);
            }
        }
        self.verified_pos_provisional = false;
        self.remote_txid = Some(remote_max);
        self.lineage_checked = true;
        Ok(())
    }

    /// Device-restore rebaseline: wipe local L0 state and adopt the newest
    /// remote L0 as the local baseline so the next push snapshots at
    /// remoteMax+1 instead of colliding TXIDs. (db.go:1400-1483, issue #781)
    fn rebaseline_from_remote(&mut self, cancel: &CancelToken, remote: &FileInfo) -> Result<()> {
        cancel.check()?;
        self.meta.clear_l0()?;
        self.invalidate_pos();

        let mut rd = self
            .client
            .open_ltx_file(0, remote.min_txid, remote.max_txid, 0, 0)?;
        let local_path = self.meta.l0_path(remote.max_txid);
        let tmp_path = local_path.with_extension("ltx.tmp");
        let tmp_guard = TmpGuard(&tmp_path);
        {
            let mut f = File::create(&tmp_path)?;
            std::io::copy(&mut rd, &mut f)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp_path, &local_path)?;
        drop(tmp_guard);
        self.invalidate_pos();

        // The adopted baseline voids the assumption behind the #927
        // expected-truncation shortcut: a `synced_to_wal_end` left true by a
        // sync earlier in this session described the DISCARDED lineage, and
        // carrying it over would let verify() resume incrementally from the
        // local WAL top over a foreign pre-state instead of snapshotting.
        self.state.synced_to_wal_end = false;
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
pub(crate) struct TmpGuard<'a>(pub(crate) &'a Path);

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
