//! Injected clock. All timing in the engine flows through this trait so tests
//! run on a manually advanced logical timeline and are fully deterministic.

use std::cell::Cell;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of the current time in milliseconds.
pub trait Clock {
    fn now_millis(&self) -> u64;
}

impl<C: Clock + ?Sized> Clock for Rc<C> {
    fn now_millis(&self) -> u64 {
        (**self).now_millis()
    }
}

impl<C: Clock + ?Sized> Clock for &C {
    fn now_millis(&self) -> u64 {
        (**self).now_millis()
    }
}

/// A clock the test (or CLI scenario) drives by hand. Single threaded interior
/// mutability is enough because the engine is single threaded.
pub struct ManualClock {
    now: Cell<u64>,
}

impl ManualClock {
    pub fn new(start_millis: u64) -> Self {
        Self {
            now: Cell::new(start_millis),
        }
    }

    /// Move the timeline forward by `delta` milliseconds.
    pub fn advance(&self, delta: u64) {
        self.now.set(self.now.get().saturating_add(delta));
    }

    /// Jump the timeline to an absolute millisecond value.
    pub fn set(&self, millis: u64) {
        self.now.set(millis);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.now.get()
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(0)
    }
}

/// A clock backed by the real wall clock, for a production style run.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_advances_and_sets() {
        let c = ManualClock::new(1000);
        assert_eq!(c.now_millis(), 1000);
        c.advance(500);
        assert_eq!(c.now_millis(), 1500);
        c.set(42);
        assert_eq!(c.now_millis(), 42);
    }

    #[test]
    fn rc_clock_delegates() {
        let c: Rc<dyn Clock> = Rc::new(ManualClock::new(7));
        assert_eq!(c.now_millis(), 7);
    }
}
