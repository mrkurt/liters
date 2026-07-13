//! Cooperative cancellation for blocking storage operations.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::StorageError;

/// Sticky cancellation flag shared between an operation's caller and the
/// code doing the blocking work. Cheap to clone (one `Arc`). A cancelled
/// token stays cancelled forever — "resume" means constructing a fresh
/// token, never resetting an old one.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> CancelToken {
        CancelToken::default()
    }

    /// Requests cancellation. Idempotent; observers see it on their next
    /// [`CancelToken::check`] / [`CancelToken::is_cancelled`].
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// `Err(StorageError::Cancelled)` once cancelled, for `?` at operation
    /// checkpoints.
    pub fn check(&self) -> Result<(), StorageError> {
        if self.is_cancelled() {
            Err(StorageError::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sticky_and_idempotent() {
        let t = CancelToken::new();
        assert!(!t.is_cancelled());
        assert!(t.check().is_ok());

        let clone = t.clone();
        clone.cancel();
        clone.cancel();
        assert!(t.is_cancelled());
        assert!(matches!(t.check(), Err(StorageError::Cancelled)));
    }
}
