//! Per upstream circuit breaker. A small explicit state machine driven by
//! request outcomes and by the injected clock. The transition rules here are
//! the single source of truth that the property gate checks against an
//! independent reference model.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerState {
    /// Traffic flows. Consecutive failures are counted.
    Closed,
    /// Tripped. No traffic until the cooldown elapses.
    Open,
    /// Trial period after cooldown. A limited number of successes closes it,
    /// a single failure re opens it.
    HalfOpen,
}

#[derive(Clone, Copy, Debug)]
pub struct BreakerConfig {
    /// Consecutive failures in Closed that trip the breaker.
    pub failure_threshold: u32,
    /// Consecutive successes in HalfOpen that close the breaker.
    pub success_threshold: u32,
    /// How long Open lasts before moving to HalfOpen.
    pub cooldown_ms: u64,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            cooldown_ms: 5_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    cfg: BreakerConfig,
    state: BreakerState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    opened_at: u64,
    trips: u64,
}

impl CircuitBreaker {
    pub fn new(cfg: BreakerConfig) -> Self {
        Self {
            cfg,
            state: BreakerState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            opened_at: 0,
            trips: 0,
        }
    }

    pub fn config(&self) -> BreakerConfig {
        self.cfg
    }

    pub fn trips(&self) -> u64 {
        self.trips
    }

    /// Promote Open to HalfOpen once the cooldown has elapsed. Idempotent, must
    /// be called with the current time before reading state or callability.
    pub fn poll(&mut self, now: u64) {
        if self.state == BreakerState::Open && now.saturating_sub(self.opened_at) >= self.cfg.cooldown_ms
        {
            self.state = BreakerState::HalfOpen;
            self.consecutive_successes = 0;
        }
    }

    pub fn state(&self) -> BreakerState {
        self.state
    }

    /// Whether a request may be routed through this breaker right now. `poll`
    /// must have been called with `now` first for a time accurate answer.
    pub fn is_callable(&self, now: u64) -> bool {
        match self.state {
            BreakerState::Closed | BreakerState::HalfOpen => true,
            BreakerState::Open => now.saturating_sub(self.opened_at) >= self.cfg.cooldown_ms,
        }
    }

    pub fn on_success(&mut self, now: u64) {
        self.poll(now);
        match self.state {
            BreakerState::Closed => {
                self.consecutive_failures = 0;
            }
            BreakerState::HalfOpen => {
                self.consecutive_successes += 1;
                if self.consecutive_successes >= self.cfg.success_threshold {
                    self.state = BreakerState::Closed;
                    self.consecutive_failures = 0;
                    self.consecutive_successes = 0;
                }
            }
            BreakerState::Open => {}
        }
    }

    pub fn on_failure(&mut self, now: u64) {
        self.poll(now);
        match self.state {
            BreakerState::Closed => {
                self.consecutive_failures += 1;
                if self.consecutive_failures >= self.cfg.failure_threshold {
                    self.trip(now);
                }
            }
            BreakerState::HalfOpen => {
                self.trip(now);
            }
            BreakerState::Open => {}
        }
    }

    fn trip(&mut self, now: u64) {
        self.state = BreakerState::Open;
        self.opened_at = now;
        self.consecutive_failures = 0;
        self.consecutive_successes = 0;
        self.trips += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BreakerConfig {
        BreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            cooldown_ms: 1000,
        }
    }

    #[test]
    fn closed_to_open_on_threshold() {
        let mut b = CircuitBreaker::new(cfg());
        assert_eq!(b.state(), BreakerState::Closed);
        b.on_failure(0);
        b.on_failure(0);
        assert_eq!(b.state(), BreakerState::Closed);
        b.on_failure(0);
        assert_eq!(b.state(), BreakerState::Open);
        assert!(!b.is_callable(0));
    }

    #[test]
    fn success_resets_failure_run() {
        let mut b = CircuitBreaker::new(cfg());
        b.on_failure(0);
        b.on_failure(0);
        b.on_success(0);
        b.on_failure(0);
        b.on_failure(0);
        assert_eq!(b.state(), BreakerState::Closed);
    }

    #[test]
    fn open_to_half_open_after_cooldown() {
        let mut b = CircuitBreaker::new(cfg());
        for _ in 0..3 {
            b.on_failure(0);
        }
        assert_eq!(b.state(), BreakerState::Open);
        b.poll(999);
        assert_eq!(b.state(), BreakerState::Open);
        b.poll(1000);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        assert!(b.is_callable(1000));
    }

    #[test]
    fn half_open_closes_after_successes() {
        let mut b = CircuitBreaker::new(cfg());
        for _ in 0..3 {
            b.on_failure(0);
        }
        b.poll(1000);
        b.on_success(1000);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.on_success(1000);
        assert_eq!(b.state(), BreakerState::Closed);
    }

    #[test]
    fn half_open_reopens_on_failure() {
        let mut b = CircuitBreaker::new(cfg());
        for _ in 0..3 {
            b.on_failure(0);
        }
        b.poll(1000);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        b.on_failure(1000);
        assert_eq!(b.state(), BreakerState::Open);
        assert_eq!(b.trips(), 2);
    }
}
