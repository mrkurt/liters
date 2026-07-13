//! Retry backoff schedule for reconnect loops.

use std::time::Duration;

use rand::Rng;

/// Exponential backoff with jitter, indexed by consecutive-failure count.
/// The delay grows `initial * multiplier^attempt`, saturates at `max`, and
/// is then scaled by a uniform jitter factor in `[1 - jitter, 1 + jitter]`
/// (so herds of reconnecting followers spread out).
#[derive(Clone, Debug)]
pub struct Backoff {
    /// Delay before the first retry (attempt 0).
    pub initial: Duration,
    /// Pre-jitter ceiling the schedule saturates at.
    pub max: Duration,
    /// Growth factor per consecutive failure.
    pub multiplier: f64,
    /// Jitter fraction in `[0, 1]`; 0 disables jitter.
    pub jitter: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(60),
            multiplier: 2.0,
            jitter: 0.25,
        }
    }
}

impl Backoff {
    /// Delay for the given 0-based consecutive-failure count, jittered.
    pub fn delay(&self, attempt: u32) -> Duration {
        // f64 math throughout: overflow for large attempts lands on
        // infinity, and `min` (which ignores NaN) saturates it at `max`.
        let exp = attempt.min(i32::MAX as u32) as i32;
        let base = (self.initial.as_secs_f64() * self.multiplier.powi(exp))
            .min(self.max.as_secs_f64());
        let jitter = self.jitter.clamp(0.0, 1.0);
        let factor = if jitter > 0.0 {
            rand::rng().random_range(1.0 - jitter..=1.0 + jitter)
        } else {
            1.0
        };
        Duration::try_from_secs_f64((base * factor).max(0.0)).unwrap_or(self.max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unjittered(initial_ms: u64, max_secs: u64, multiplier: f64) -> Backoff {
        Backoff {
            initial: Duration::from_millis(initial_ms),
            max: Duration::from_secs(max_secs),
            multiplier,
            jitter: 0.0,
        }
    }

    #[test]
    fn growth() {
        let b = unjittered(500, 60, 2.0);
        assert_eq!(b.delay(0), Duration::from_millis(500));
        assert_eq!(b.delay(1), Duration::from_secs(1));
        assert_eq!(b.delay(2), Duration::from_secs(2));
        assert_eq!(b.delay(6), Duration::from_secs(32));
    }

    #[test]
    fn saturation() {
        let b = unjittered(500, 60, 2.0);
        // 0.5 * 2^7 = 64s exceeds the 60s cap.
        assert_eq!(b.delay(7), Duration::from_secs(60));
        assert_eq!(b.delay(1000), Duration::from_secs(60));
        // Overflow-safe: the f64 exponent goes to infinity, not UB/panic.
        assert_eq!(b.delay(u32::MAX), Duration::from_secs(60));
    }

    #[test]
    fn jitter_bounds() {
        let b = Backoff {
            initial: Duration::from_secs(8),
            max: Duration::from_secs(60),
            multiplier: 2.0,
            jitter: 0.25,
        };
        for _ in 0..1000 {
            let d = b.delay(0);
            assert!(d >= Duration::from_secs_f64(6.0), "below jitter floor: {d:?}");
            assert!(d <= Duration::from_secs_f64(10.0), "above jitter ceiling: {d:?}");
        }
        // Jitter applies after saturation, so the cap wobbles too.
        for _ in 0..1000 {
            let d = b.delay(50);
            assert!(d >= Duration::from_secs_f64(45.0), "below jitter floor: {d:?}");
            assert!(d <= Duration::from_secs_f64(75.0), "above jitter ceiling: {d:?}");
        }
    }
}
