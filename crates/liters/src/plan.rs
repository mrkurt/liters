//! Restore-plan calculation: choose the LTX file chain that reconstructs the
//! database. A faithful port of `CalcRestorePlan`. (replica.go:1441-1637)
//!
//! The plan starts from the newest eligible snapshot (level 9), then greedily
//! extends a contiguous TXID chain with per-level cursors over levels 8..0,
//! always picking the candidate covering the most transactions. Overlap
//! (`min <= current+1 && max > current`) is legal and required — it is what
//! lets restores survive concurrent compaction replacing small files with
//! merged ones.

use liters_storage::{ReplicaClient, SNAPSHOT_LEVEL};
use ltx::{FileInfo, Txid};

use crate::{Error, Result};

/// `ErrTxNotAvailable` equivalent. (store.go:27)
pub fn tx_not_available() -> Error {
    Error::Other("transaction not available".into())
}

/// Whether `next` is a better chain-extension candidate than `curr`:
/// higher MaxTXID, then lower MinTXID, then higher level, then earlier
/// creation. (replica.go:1626-1637)
fn candidate_better(curr: &FileInfo, next: &FileInfo) -> bool {
    if next.max_txid != curr.max_txid {
        return next.max_txid > curr.max_txid;
    }
    if next.min_txid != curr.min_txid {
        return next.min_txid < curr.min_txid;
    }
    if next.level != curr.level {
        return next.level > curr.level;
    }
    match (next.created_at, curr.created_at) {
        (Some(n), Some(c)) => n < c,
        _ => false,
    }
}

struct LevelCursor {
    files: std::vec::IntoIter<FileInfo>,
    current: Option<FileInfo>,
    candidate: Option<FileInfo>,
    done: bool,
}

impl LevelCursor {
    /// Advances past every file that could be contiguous with `current_max`,
    /// keeping the best eligible candidate. (replica.go:1570-1608)
    fn refresh(&mut self, current_max: Txid, txid: Txid) {
        if self.done && self.current.is_none() {
            // Iterator exhausted and no pending file: nothing to do, but a
            // previously stale candidate must still be cleared below.
        }
        if self.candidate.as_ref().is_some_and(|c| c.max_txid <= current_max) {
            self.candidate = None;
        }

        loop {
            if self.current.is_none() {
                match self.files.next() {
                    Some(info) => self.current = Some(info),
                    None => {
                        self.done = true;
                        return;
                    }
                }
            }
            let info = self.current.as_ref().unwrap();
            if info.min_txid.0 > current_max.0.wrapping_add(1) {
                return; // gap at this level; keep for the post-check
            }
            let info = self.current.take().unwrap();

            if info.max_txid <= current_max {
                continue;
            }
            if !txid.is_zero() && info.max_txid > txid {
                continue;
            }
            if self.candidate.is_none() || candidate_better(self.candidate.as_ref().unwrap(), &info)
            {
                self.candidate = Some(info);
            }
        }
    }
}

/// Computes the ordered chain of LTX files reconstructing the database at
/// `txid` (or the latest state when `txid` is zero). (replica.go:1441-1557)
pub fn calc_restore_plan(client: &dyn ReplicaClient, txid: Txid) -> Result<Vec<FileInfo>> {
    let mut infos: Vec<FileInfo> = Vec::new();

    // Newest snapshot at or before the target. (replica.go:1449-1473)
    let mut snapshot: Option<FileInfo> = None;
    for info in client.ltx_files(SNAPSHOT_LEVEL, Txid(0), false)? {
        if !txid.is_zero() && info.max_txid > txid {
            continue;
        }
        snapshot = Some(info);
    }
    if let Some(s) = snapshot {
        infos.push(s);
    }

    let mut current_max = infos.last().map(|f| f.max_txid).unwrap_or_default();
    if !txid.is_zero() && current_max >= txid {
        return Ok(infos);
    }

    // Per-level cursors, highest level first. (replica.go:1484-1494)
    let mut cursors: Vec<LevelCursor> = Vec::new();
    for level in (0..SNAPSHOT_LEVEL).rev() {
        let files = client.ltx_files(level, Txid(0), false)?;
        cursors.push(LevelCursor {
            files: files.into_iter(),
            current: None,
            candidate: None,
            done: false,
        });
    }

    // Greedy chain extension. (replica.go:1503-1536)
    loop {
        for cursor in cursors.iter_mut() {
            cursor.refresh(current_max, txid);
        }
        let mut next: Option<usize> = None;
        for (i, cursor) in cursors.iter().enumerate() {
            let Some(candidate) = &cursor.candidate else { continue };
            match next {
                None => next = Some(i),
                Some(j) => {
                    if candidate_better(cursors[j].candidate.as_ref().unwrap(), candidate) {
                        next = Some(i);
                    }
                }
            }
        }

        let Some(i) = next else { break };
        let candidate = cursors[i].candidate.take().unwrap();
        if candidate.max_txid <= current_max {
            continue;
        }
        current_max = candidate.max_txid;
        infos.push(candidate);

        if !txid.is_zero() && current_max >= txid {
            break;
        }
    }

    // Latest-restore gap check: refuse to serve a stale head when files
    // beyond a hole exist. (replica.go:1538-1547)
    if !infos.is_empty() && txid.is_zero() {
        for cursor in &cursors {
            if let Some(current) = &cursor.current {
                if current.min_txid.0 > current_max.0.wrapping_add(1) {
                    return Err(Error::Other(format!(
                        "non-contiguous ltx files: have up to {} but next file starts at {}",
                        current_max, current.min_txid
                    )));
                }
            }
        }
    }

    if infos.is_empty() {
        return Err(tx_not_available());
    }
    if !txid.is_zero() && infos.last().unwrap().max_txid < txid {
        return Err(tx_not_available());
    }

    Ok(infos)
}

// `candidate_better` requires a strict weak order; sanity-check the tie-break
// chain terminates (equal infos are never "better").
#[cfg(test)]
mod tests {
    use super::*;

    fn info(level: u8, min: u64, max: u64) -> FileInfo {
        FileInfo { level, min_txid: Txid(min), max_txid: Txid(max), ..Default::default() }
    }

    #[test]
    fn equal_candidates_not_better() {
        let a = info(1, 1, 5);
        assert!(!candidate_better(&a, &a.clone()));
    }

    #[test]
    fn prefers_wider_coverage_then_lower_min_then_higher_level() {
        assert!(candidate_better(&info(1, 1, 5), &info(1, 1, 9)));
        assert!(candidate_better(&info(1, 3, 9), &info(1, 1, 9)));
        assert!(candidate_better(&info(1, 1, 9), &info(2, 1, 9)));
    }
}
