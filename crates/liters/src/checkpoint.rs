//! The checkpoint protocol. Because the writer's long-running read
//! transaction starves every other checkpointer (including the app's
//! wal_autocheckpoint), liters MUST checkpoint the database itself.
//! (db.go:2139-2335)
//!
//! Protocol: pre-copy sync (capture every committed frame before the WAL is
//! recycled) → release read lock → `PRAGMA wal_checkpoint(MODE)` → reacquire
//! read lock → force a `_litestream_seq` write (so a restarted WAL gets a
//! valid header immediately) → if the WAL restarted, take the SQLite write
//! lock and run a post-copy sync under it (no writer can commit concurrently),
//! then roll back.

use crate::sqlite;
use crate::verify::read_wal_header;
use crate::writer::{CheckpointMode, Writer};
use crate::Result;

impl Writer {
    /// Runs a checkpoint with the full pre-copy / post-copy protocol.
    /// (db.go:2187-2289)
    pub fn checkpoint(&mut self, mode: CheckpointMode) -> Result<()> {
        // WAL header before, to detect a restart. (db.go:2205)
        let hdr_before = read_wal_header(&self.wal_path)?;

        // Pre-copy: capture everything committed so far. (db.go:2216-2220)
        let outcome = self.verify_and_sync(true)?;
        self.apply_sync_outcome(outcome);

        // The checkpoint itself, bracketed by read-lock release/reacquire.
        // (db.go:2291-2335)
        self.exec_checkpoint(mode)?;

        // Force a write so a restarted WAL immediately has a valid header
        // plus at least one frame. (db.go:2231)
        sqlite::write_seq(&self.conn)?;

        // No restart? Done. (db.go:2241-2246)
        let hdr_after = read_wal_header(&self.wal_path)?;
        if hdr_before == hdr_after {
            self.state.synced_since_checkpoint = false;
            return Ok(());
        }

        // The WAL restarted: capture the boundary under the write lock so no
        // app writer can commit between the restart and our post-copy.
        // (db.go:2248-2285)
        self.conn.execute_batch("BEGIN")?;
        let result = (|| -> Result<()> {
            // The insert grabs the write lock; the transaction always rolls
            // back so no row is ever visible. (db.go:2260-2266)
            self.conn
                .execute("INSERT INTO _litestream_lock (id) VALUES (1)", [])?;
            let outcome = self.verify_and_sync(true)?;
            self.apply_sync_outcome(outcome);
            Ok(())
        })();
        let rollback = self.conn.execute_batch("ROLLBACK");
        result?;
        rollback?;

        self.state.synced_since_checkpoint = false;
        Ok(())
    }

    /// Releases the read lock, runs `PRAGMA wal_checkpoint`, and reacquires
    /// the read lock — including on the error path. (db.go:2291-2335)
    fn exec_checkpoint(&mut self, mode: CheckpointMode) -> Result<()> {
        self.read_lock.release(&self.read_conn)?;
        let checkpoint_result = sqlite::wal_checkpoint(&self.conn, mode.as_sql());
        let reacquire = self.read_lock.acquire(&self.read_conn);
        let _row = checkpoint_result?;
        reacquire?;
        Ok(())
    }
}
