//! Local-directory replica client, byte-identical in layout to litestream's
//! `file` backend (file/replica_client.go): files live at
//! `{root}/ltx/{level decimal}/{min:016x}-{max:016x}.ltx`, with the LTX
//! header timestamp preserved as the file's mtime.
//!
//! A bucket written by this client restores with stock
//! `litestream restore file://{root}`, and this client reads buckets written
//! by `litestream replicate <db> file://{root}`.

use std::fs::{self, File, FileTimes};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use ltx::{format_filename, parse_filename, FileInfo, Header, Txid, HEADER_SIZE};

use crate::{ReplicaClient, Result, StorageError};

/// Litestream `file`-layout replica client rooted at a directory.
pub struct DirReplicaClient {
    root: PathBuf,
}

/// Distinguishes concurrent same-process writers' tmp files (see
/// `write_ltx_file`).
static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl DirReplicaClient {
    pub fn new(root: impl Into<PathBuf>) -> DirReplicaClient {
        DirReplicaClient { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `{root}/ltx/{level}` — decimal level, mirroring `litestream.LTXLevelDir`.
    /// (litestream.go:184-197)
    pub fn level_dir(&self, level: u8) -> PathBuf {
        self.root.join("ltx").join(level.to_string())
    }

    pub fn ltx_path(&self, level: u8, min_txid: Txid, max_txid: Txid) -> PathBuf {
        self.level_dir(level).join(format_filename(min_txid, max_txid))
    }
}

impl ReplicaClient for DirReplicaClient {
    fn client_type(&self) -> &'static str {
        "file"
    }

    fn ltx_files(&self, level: u8, seek: Txid, _use_metadata: bool) -> Result<Vec<FileInfo>> {
        let dir = self.level_dir(level);
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut infos = Vec::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            // Non-LTX filenames (tmp files, strays) are skipped, as in Go.
            let Some((min_txid, max_txid)) = parse_filename(name) else { continue };
            if min_txid < seek {
                continue;
            }
            let meta = entry.metadata()?;
            infos.push(FileInfo {
                level,
                min_txid,
                max_txid,
                size: meta.len(),
                created_at: meta.modified().ok(),
                ..Default::default()
            });
        }

        // Mirror ltx.NewFileInfoSliceIterator ordering: (min_txid, max_txid).
        infos.sort_by_key(|f| (f.min_txid, f.max_txid));
        Ok(infos)
    }

    fn open_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        offset: u64,
        size: u64,
    ) -> Result<Box<dyn Read + Send>> {
        let path = self.ltx_path(level, min_txid, max_txid);
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound { level, min_txid, max_txid })
            }
            Err(e) => return Err(e.into()),
        };
        if offset > 0 {
            f.seek(SeekFrom::Start(offset))?;
        }
        if size > 0 {
            Ok(Box::new(f.take(size)))
        } else {
            Ok(Box::new(f))
        }
    }

    fn write_ltx_file(
        &self,
        level: u8,
        min_txid: Txid,
        max_txid: Txid,
        rd: &mut (dyn Read + Send),
    ) -> Result<FileInfo> {
        // Peek the header to extract the timestamp, preserved as mtime.
        // (file/replica_client.go:163-172)
        let mut hdr_buf = [0u8; HEADER_SIZE];
        rd.read_exact(&mut hdr_buf)?;
        let hdr = Header::decode(&hdr_buf)?;
        let timestamp = if hdr.timestamp >= 0 {
            UNIX_EPOCH + Duration::from_millis(hdr.timestamp as u64)
        } else {
            UNIX_EPOCH
        };

        let path = self.ltx_path(level, min_txid, max_txid);
        fs::create_dir_all(path.parent().unwrap())?;

        // tmp + fsync + rename for atomicity. (file/replica_client.go:183-231)
        // The tmp name is unique per write, not the fixed `<name>.ltx.tmp`:
        // a writable HTTP server makes concurrent same-key writes real (a
        // pusher's retry racing a stalled first attempt), and a shared tmp
        // path would let the loser's cleanup unlink the winner's in-flight
        // file. Listings skip any non-`.ltx` name, so strays are invisible.
        let tmp_path = path.with_extension(format!(
            "ltx.{}-{}.tmp",
            std::process::id(),
            TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let result = (|| -> Result<u64> {
            let mut f = File::create(&tmp_path)?;
            f.write_all(&hdr_buf)?;
            let n = std::io::copy(rd, &mut f)?;
            f.sync_all()?;
            Ok(HEADER_SIZE as u64 + n)
        })();
        let size = match result {
            Ok(size) => size,
            Err(e) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(e);
            }
        };
        fs::rename(&tmp_path, &path)?;

        // Preserve the header timestamp as mtime (Go uses os.Chtimes).
        let f = File::options().write(true).open(&path)?;
        f.set_times(FileTimes::new().set_accessed(timestamp).set_modified(timestamp))?;

        // Fsync the directory so the rename survives power loss (an
        // improvement over Go, which skips this).
        if let Ok(d) = File::open(path.parent().unwrap()) {
            let _ = d.sync_all();
        }

        Ok(FileInfo {
            level,
            min_txid,
            max_txid,
            size,
            created_at: Some(timestamp),
            ..Default::default()
        })
    }

    fn delete_ltx_files(&self, infos: &[FileInfo]) -> Result<()> {
        for info in infos {
            let path = self.ltx_path(info.level, info.min_txid, info.max_txid);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn delete_all(&self) -> Result<()> {
        match fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}
