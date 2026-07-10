//! SQLite connection plumbing for the writer: the exact pragmas, file
//! controls, and lock discipline litestream uses. (db.go:965-1162)

use std::path::Path;
use std::time::Duration;

use rusqlite::Connection;

use crate::Result;

/// Opens a connection configured like litestream's:
/// `busy_timeout`, `wal_autocheckpoint(0)` (disable auto-checkpoints on OUR
/// connection only), and `SQLITE_FCNTL_PERSIST_WAL` (never delete the -wal
/// on close). (db.go:988-998)
pub fn open_conn(path: &Path, busy_timeout: Duration) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(busy_timeout)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)?;
    set_persist_wal(&conn)?;
    Ok(conn)
}

/// Sets SQLITE_FCNTL_PERSIST_WAL=1 so the -wal file survives the last
/// connection closing. (db.go:943-963)
fn set_persist_wal(conn: &Connection) -> Result<()> {
    // SQLITE_FCNTL_PERSIST_WAL = 10.
    const SQLITE_FCNTL_PERSIST_WAL: std::os::raw::c_int = 10;
    let mut arg: std::os::raw::c_int = 1;
    let rc = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            conn.handle(),
            c"main".as_ptr(),
            SQLITE_FCNTL_PERSIST_WAL,
            &mut arg as *mut _ as *mut std::os::raw::c_void,
        )
    };
    if rc != rusqlite::ffi::SQLITE_OK {
        return Err(crate::Error::Other(format!("set PERSIST_WAL: sqlite rc {rc}")));
    }
    Ok(())
}

/// Switches the database to WAL mode, asserting the switch took effect.
/// (db.go:1016-1023)
pub fn enable_wal(conn: &Connection) -> Result<()> {
    let mode: String = conn.query_row("PRAGMA journal_mode = wal;", [], |r| r.get(0))?;
    if mode != "wal" {
        return Err(crate::Error::EnableWalFailed(mode));
    }
    Ok(())
}

/// Creates litestream's bookkeeping tables (same names, for drop-in interop):
/// `_litestream_seq` forces WAL writes on demand; `_litestream_lock` promotes
/// transactions to the write lock. (db.go:1025-1035)
pub fn create_meta_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
         CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);",
    )?;
    Ok(())
}

/// Bumps `_litestream_seq`, forcing at least one frame into the WAL.
/// (db.go:1396, 2231)
pub fn write_seq(conn: &Connection) -> Result<()> {
    conn.execute(
        "INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) \
         ON CONFLICT (id) DO UPDATE SET seq = seq + 1",
        [],
    )?;
    Ok(())
}

/// The long-running read transaction that pins the WAL: no checkpointer (in
/// any process) can restart or truncate the WAL past our read mark while it
/// is held. Held between pushes; released only around our own checkpoints.
/// (db.go:1125-1162)
pub struct ReadLock {
    held: bool,
}

impl ReadLock {
    pub fn new() -> ReadLock {
        ReadLock { held: false }
    }

    /// BEGIN (deferred) + a read of `_litestream_seq` to actually take the
    /// read lock. (db.go:1126-1146)
    pub fn acquire(&mut self, conn: &Connection) -> Result<()> {
        if self.held {
            return Ok(());
        }
        conn.execute_batch("BEGIN")?;
        match conn.query_row("SELECT COUNT(1) FROM _litestream_seq", [], |r| r.get::<_, i64>(0)) {
            Ok(_) => {
                self.held = true;
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e.into())
            }
        }
    }

    /// Rolls back the read transaction. (db.go:1148-1162)
    pub fn release(&mut self, conn: &Connection) -> Result<()> {
        if !self.held {
            return Ok(());
        }
        self.held = false;
        conn.execute_batch("ROLLBACK")?;
        Ok(())
    }
}

/// Runs `PRAGMA wal_checkpoint(MODE)`, returning the (busy, log, checkpointed)
/// triple. (db.go:2321-2327)
pub fn wal_checkpoint(conn: &Connection, mode: &str) -> Result<(i64, i64, i64)> {
    let sql = format!("PRAGMA wal_checkpoint({mode});");
    let row = conn.query_row(&sql, [], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?))
    })?;
    Ok(row)
}

/// Returns true for SQLITE_BUSY-ish errors, which are expected on PASSIVE
/// checkpoints under contention. (db.go:1347-1356)
pub fn is_busy_error(e: &crate::Error) -> bool {
    match e {
        crate::Error::Sqlite(rusqlite::Error::SqliteFailure(f, _)) => {
            f.code == rusqlite::ErrorCode::DatabaseBusy || f.code == rusqlite::ErrorCode::DatabaseLocked
        }
        crate::Error::Sqlite(e) => {
            let s = e.to_string();
            s.contains("database is locked") || s.contains("SQLITE_BUSY")
        }
        _ => false,
    }
}
