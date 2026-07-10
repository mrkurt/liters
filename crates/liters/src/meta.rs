//! The writer's local meta directory: `.{dbname}-litestream/ltx/0/` next to
//! the database, holding recently created L0 files. The newest L0 file *is*
//! the replication position — there is no cursor file. (db.go:289, 528-590)

use std::fs;
use std::path::{Path, PathBuf};

use ltx::{format_filename, parse_filename, Decoder, Pos, Txid};

use crate::{Error, Result};

/// Suffix for the meta directory name. (litestream.go:20)
pub const META_DIR_SUFFIX: &str = "-litestream";

/// Paths for a database's local replication state.
#[derive(Debug, Clone)]
pub struct MetaDir {
    root: PathBuf,
}

impl MetaDir {
    /// `.{filename}-litestream` in the database's directory. (db.go:283-292)
    pub fn for_db(db_path: &Path) -> MetaDir {
        let dir = db_path.parent().unwrap_or(Path::new("."));
        let name = db_path.file_name().unwrap_or_default().to_string_lossy();
        MetaDir { root: dir.join(format!(".{name}{META_DIR_SUFFIX}")) }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Local L0 directory: `{meta}/ltx/0`. (db.go:295-306)
    pub fn l0_dir(&self) -> PathBuf {
        self.root.join("ltx").join("0")
    }

    pub fn l0_path(&self, txid: Txid) -> PathBuf {
        self.l0_dir().join(format_filename(txid, txid))
    }

    pub fn create_dirs(&self) -> Result<()> {
        fs::create_dir_all(self.l0_dir())?;
        Ok(())
    }

    /// Removes stale `*.tmp` staging files, as litestream does on open.
    /// (litestream.go:169-182)
    pub fn remove_tmp_files(&self) -> Result<()> {
        let Ok(entries) = fs::read_dir(self.l0_dir()) else { return Ok(()) };
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|e| e == "tmp") {
                let _ = fs::remove_file(entry.path());
            }
        }
        Ok(())
    }

    /// The max-TXID L0 file present locally, by filename. (db.go:528-545)
    pub fn max_l0_txid(&self) -> Result<Option<Txid>> {
        let Ok(entries) = fs::read_dir(self.l0_dir()) else { return Ok(None) };
        let mut max: Option<Txid> = None;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some((_, max_txid)) = parse_filename(name) else { continue };
            if max.is_none_or(|m| max_txid > m) {
                max = Some(max_txid);
            }
        }
        Ok(max)
    }

    /// Derives the current replication position by fully verifying the newest
    /// local L0 file (CRC64 included). Returns `Pos::default()` (TXID 0) when
    /// no L0 files exist. (db.go:559-590)
    pub fn pos(&self) -> Result<Pos> {
        let Some(txid) = self.max_l0_txid()? else {
            return Ok(Pos::default());
        };
        let path = self.l0_path(txid);
        let f = fs::File::open(&path)
            .map_err(|e| Error::LocalLtx { txid, msg: format!("open {path:?}: {e}") })?;
        let dec = Decoder::new(std::io::BufReader::new(f));
        let (hdr, trailer, _) = dec
            .verify()
            .map_err(|e| Error::LocalLtx { txid, msg: format!("verify {path:?}: {e}") })?;
        Ok(Pos { txid: hdr.max_txid, post_apply_checksum: trailer.post_apply_checksum })
    }

    /// Deletes local L0 files with `max_txid < before`, always keeping the
    /// newest file (needed by verify() on the next push).
    pub fn prune_l0(&self, before: Txid) -> Result<()> {
        let Some(newest) = self.max_l0_txid()? else { return Ok(()) };
        let Ok(entries) = fs::read_dir(self.l0_dir()) else { return Ok(()) };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some((_, max_txid)) = parse_filename(name) else { continue };
            if max_txid < before && max_txid != newest {
                let _ = fs::remove_file(entry.path());
            }
        }
        Ok(())
    }

    /// Persists the last-synced WAL offset across process restarts (used for
    /// checkpoint thresholds — issue #997). Deliberately does NOT persist
    /// `synced_to_wal_end`: the #927 expected-truncation shortcut is only
    /// sound while the read lock continuously prevents foreign checkpoints,
    /// which is false across a writer close/reopen — a stale `true` could
    /// skip the snapshot that recovers commits checkpointed while closed.
    pub fn write_sync_state(&self, last_synced_wal_offset: u64) -> Result<()> {
        let path = self.root.join("sync-state");
        let tmp = self.root.join("sync-state.tmp");
        {
            use std::io::Write;
            let mut f = fs::File::create(&tmp)?;
            writeln!(f, "{last_synced_wal_offset}")?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Reads the persisted last-synced WAL offset; 0 when absent/malformed.
    pub fn read_sync_state(&self) -> u64 {
        let Ok(s) = fs::read_to_string(self.root.join("sync-state")) else { return 0 };
        s.split_whitespace().next().and_then(|v| v.parse().ok()).unwrap_or(0)
    }

    /// Wipes the local L0 directory (device-restore rebaseline). (db.go:1430-1438)
    pub fn clear_l0(&self) -> Result<()> {
        let dir = self.l0_dir();
        match fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        fs::create_dir_all(&dir)?;
        Ok(())
    }
}
