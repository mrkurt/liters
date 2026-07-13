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

    /// Persists the verified position: the highest TXID this local lineage
    /// KNOWS it has successfully placed in the bucket (see
    /// `Writer::ensure_lineage_checked`). Same atomic tmp+fsync+rename
    /// discipline as the sync-state file; same 16-hex format as the replica's
    /// `{db}-txid` sidecar. Best-effort at the call sites: a lost update can
    /// only cause a spurious rebaseline next session, never a missed one.
    pub fn write_verified_pos(&self, txid: Txid) -> Result<()> {
        let path = self.root.join("verified-pos");
        let tmp = self.root.join("verified-pos.tmp");
        {
            use std::io::Write;
            let mut f = fs::File::create(&tmp)?;
            writeln!(f, "{txid}")?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Reads the persisted verified position; `None` when the file is
    /// absent or malformed, so `Writer::open` can distinguish a meta dir
    /// that predates the file (litestream's own, or older liters) from one
    /// whose lineage is genuinely pinned at zero.
    pub fn read_verified_pos(&self) -> Option<Txid> {
        let s = fs::read_to_string(self.root.join("verified-pos")).ok()?;
        Txid::parse(s.trim())
    }

    /// Returns this database's persisted writer id, generating one (16
    /// random bytes, lowercase hex) on first use. Stored in the meta dir's
    /// `writer-id` file with the same atomic tmp+fsync+rename discipline as
    /// the other meta files, so the id is stable across process restarts and
    /// survives reinstallation exactly as long as the meta dir does — which
    /// is the correct fencing semantic: a device restored without its meta
    /// dir is a new lineage and *should* present as a new writer.
    // Only the manager's HTTP push registration consumes this today.
    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    pub fn writer_id(&self) -> Result<String> {
        let path = self.root.join("writer-id");
        match fs::read_to_string(&path) {
            Ok(s) => {
                let s = s.trim();
                // Visible-ASCII guard matches the HTTP client's header
                // validation; a corrupt file regenerates instead of wedging
                // every future registration.
                if !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| (0x21..=0x7e).contains(&b))
                {
                    return Ok(s.to_string());
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        fs::create_dir_all(&self.root)?;
        let mut bytes = [0u8; 16];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
        let id: String = bytes.iter().map(|b| format!("{b:02x}")).collect();

        let tmp = self.root.join("writer-id.tmp");
        {
            use std::io::Write;
            let mut f = fs::File::create(&tmp)?;
            writeln!(f, "{id}")?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Ok(id)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_id_is_generated_once_and_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = MetaDir::for_db(&tmp.path().join("app.db"));

        let id = meta.writer_id().unwrap();
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));

        // Re-reads (same or a fresh MetaDir over the same db) are stable.
        assert_eq!(meta.writer_id().unwrap(), id);
        let again = MetaDir::for_db(&tmp.path().join("app.db"));
        assert_eq!(again.writer_id().unwrap(), id);
    }
}
