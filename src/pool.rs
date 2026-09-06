//! An upstream pool for one service. Owns the backends, their runtime health and
//! circuit state, the load balancing strategy, and selection. Selection never
//! returns a backend that is unhealthy, ejected, or on an open circuit. That is
//! the core invariant the gate checks.

use crate::breaker::{BreakerConfig, BreakerState, CircuitBreaker};
use crate::clock::Clock;
use crate::ewma::{DistributionRow, EwmaConfig};
use crate::hashring::ConsistentHashRing;
use crate::upstream::{Upstream, UpstreamReply};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    RoundRobin,
    Weighted,
    LeastConnections,
    ConsistentHash,
}

impl Strategy {
    pub fn parse(s: &str) -> Option<Strategy> {
        match s.to_ascii_lowercase().as_str() {
            "round-robin" | "roundrobin" | "rr" => Some(Strategy::RoundRobin),
            "weighted" | "wrr" => Some(Strategy::Weighted),
            "least-conn" | "least-connections" | "leastconn" | "lc" => {
                Some(Strategy::LeastConnections)
            }
            "consistent-hash" | "sticky" | "hash" => Some(Strategy::ConsistentHash),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Strategy::RoundRobin => "round-robin",
            Strategy::Weighted => "weighted",
            Strategy::LeastConnections => "least-conn",
            Strategy::ConsistentHash => "consistent-hash",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HealthConfig {
    /// Minimum time between active probes for a backend.
    pub probe_interval_ms: u64,
    /// Consecutive failed probes that flip a backend to unhealthy.
    pub unhealthy_probe_threshold: u32,
    /// Consecutive good probes that flip a backend back to healthy.
    pub healthy_probe_threshold: u32,
    /// Consecutive real request failures that eject a backend as an outlier.
    pub outlier_consecutive_failures: u32,
    /// How long an outlier ejection lasts before reinstatement.
    pub ejection_base_ms: u64,
    pub start_healthy: bool,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            probe_interval_ms: 1_000,
            unhealthy_probe_threshold: 2,
            healthy_probe_threshold: 2,
            outlier_consecutive_failures: 5,
            ejection_base_ms: 5_000,
            start_healthy: true,
        }
    }
}

pub struct Backend {
    pub name: String,
    pub weight: u32,
    upstream: Box<dyn Upstream>,
    pub in_flight: u32,
    healthy: bool,
    probe_failures: u32,
    probe_successes: u32,
    last_probe: Option<u64>,
    outlier_failures: u32,
    ejected_until: Option<u64>,
    ejections: u64,
    breaker: CircuitBreaker,
    current_weight: i64,
    served: u64,
    latency_ewma_milli: Option<u64>,
    ewma_samples: u32,
    over_since: Option<u64>,
    over_since_samples: u32,
    ewma_ejections: u64,
    ramp_percent: u32,
    last_ramp_step: Option<u64>,
}

impl Backend {
    pub fn new(
        name: impl Into<String>,
        weight: u32,
        upstream: Box<dyn Upstream>,
        health: &HealthConfig,
        breaker_cfg: BreakerConfig,
    ) -> Self {
        Self {
            name: name.into(),
            // Weight zero is meaningful: the node gets no traffic under the
            // weighted strategy. Other strategies ignore weight.
            weight,
            upstream,
            in_flight: 0,
            healthy: health.start_healthy,
            probe_failures: 0,
            probe_successes: 0,
            last_probe: None,
            outlier_failures: 0,
            ejected_until: None,
            ejections: 0,
            breaker: CircuitBreaker::new(breaker_cfg),
            current_weight: 0,
            served: 0,
            latency_ewma_milli: None,
            ewma_samples: 0,
            over_since: None,
            over_since_samples: 0,
            ewma_ejections: 0,
            ramp_percent: 100,
            last_ramp_step: None,
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy
    }

    pub fn is_ejected(&self, now: u64) -> bool {
        matches!(self.ejected_until, Some(until) if now < until)
    }

    pub fn ejected_until(&self) -> Option<u64> {
        self.ejected_until
    }

    pub fn breaker_state(&self) -> BreakerState {
        self.breaker.state()
    }

    pub fn served(&self) -> u64 {
        self.served
    }

    pub fn ejections(&self) -> u64 {
        self.ejections
    }

    pub fn ewma_milli(&self) -> Option<u64> {
        self.latency_ewma_milli
    }

    pub fn ewma_samples(&self) -> u32 {
        self.ewma_samples
    }

    pub fn ewma_ejections(&self) -> u64 {
        self.ewma_ejections
    }

    /// Current reinstatement ramp percent, 100 means full weight.
    pub fn ramp_percent(&self) -> u32 {
        self.ramp_percent
    }

    /// Available means routable right now: healthy, not ejected, breaker allows
    /// the call (including the half open trial cap), and under `HalfOpen` a trial
    /// slot is free.
    pub fn is_available(&self, now: u64) -> bool {
        self.healthy && !self.is_ejected(now) && self.breaker.admits(now, self.in_flight)
    }
}

pub struct Pool {
    name: String,
    backends: Vec<Backend>,
    strategy: Strategy,
    health: HealthConfig,
    rr_cursor: usize,
    ring: ConsistentHashRing,
    ring_signature: Vec<usize>,
    ewma: Option<EwmaConfig>,
}

impl Pool {
    pub fn new(name: impl Into<String>, strategy: Strategy, health: HealthConfig) -> Self {
        Self {
            name: name.into(),
            backends: Vec::new(),
            strategy,
            health,
            rr_cursor: 0,
            ring: ConsistentHashRing::new(160),
            ring_signature: Vec::new(),
            ewma: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn strategy(&self) -> Strategy {
        self.strategy
    }

    pub fn set_strategy(&mut self, s: Strategy) {
        self.strategy = s;
    }

    pub fn health_config(&self) -> &HealthConfig {
        &self.health
    }

    pub fn add_backend(&mut self, backend: Backend) {
        self.backends.push(backend);
        self.ring_signature.clear();
    }

    pub fn backends(&self) -> &[Backend] {
        &self.backends
    }

    pub fn backend(&self, idx: usize) -> &Backend {
        &self.backends[idx]
    }

    pub fn len(&self) -> usize {
        self.backends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }

    /// Promote circuit breakers on the clock, reinstate ejected outliers whose
    /// cooldown has elapsed, and when the EWMA layer is enabled, evaluate
    /// latency outliers and drive the reinstatement ramp. Must run before any
    /// selection so state is current.
    pub fn poll(&mut self, now: u64) {
        let ewma = self.ewma;
        let threshold = ewma.map(|cfg| cfg.threshold_milli(self.ewma_mean()));
        for b in &mut self.backends {
            b.breaker.poll(now);
            if let Some(until) = b.ejected_until {
                if now >= until {
                    b.ejected_until = None;
                    b.outlier_failures = 0;
                    // A reinstated node comes back at partial weight and ramps
                    // up only while it stays clean.
                    if let Some(cfg) = ewma {
                        b.ramp_percent = cfg.reinstate_percent.clamp(1, 100);
                        b.last_ramp_step = Some(now);
                    }
                }
            }
            let Some(cfg) = ewma else { continue };
            // Ramp up while no ejection is active.
            if b.ejected_until.is_none() && b.ramp_percent < 100 {
                let due = match b.last_ramp_step {
                    None => true,
                    Some(step) => now.saturating_sub(step) >= cfg.ramp_window_ms,
                };
                if due {
                    b.ramp_percent = (b.ramp_percent.saturating_mul(2)).min(100);
                    b.last_ramp_step = Some(now);
                }
            }
            // Sustained latency outlier evaluation. Only while not already
            // ejected, and the sustained window counts only when fresh
            // evidence arrived, so a reinstated node is never re ejected on
            // stale samples it cannot improve while it receives little traffic.
            if b.ejected_until.is_none() {
                if b.ewma_samples >= cfg.min_samples {
                    let ewma_milli = b.latency_ewma_milli.unwrap_or(0);
                    if ewma_milli > threshold.unwrap_or(0) {
                        match b.over_since {
                            None => {
                                b.over_since = Some(now);
                                b.over_since_samples = b.ewma_samples;
                            }
                            Some(since) => {
                                let fresh =
                                    b.ewma_samples.saturating_sub(b.over_since_samples);
                                if now.saturating_sub(since) >= cfg.window_ms
                                    && fresh >= cfg.min_samples
                                {
                                    b.ejected_until =
                                        Some(now.saturating_add(cfg.ejection_ms));
                                    b.ewma_ejections += 1;
                                    b.over_since = None;
                                }
                            }
                        }
                    } else {
                        b.over_since = None;
                    }
                } else {
                    b.over_since = None;
                }
            }
        }
    }

    /// Mean EWMA over nodes with enough samples, in milli milliseconds.
    fn ewma_mean(&self) -> u64 {
        let Some(cfg) = self.ewma else { return 0 };
        let mut sum: u128 = 0;
        let mut n: u128 = 0;
        for b in &self.backends {
            if b.ewma_samples >= cfg.min_samples {
                sum += u128::from(b.latency_ewma_milli.unwrap_or(0));
                n += 1;
            }
        }
        // When no node qualifies the sum is zero too, so the max keeps the
        // division safe and the mean at zero.
        u64::try_from(sum / n.max(1)).unwrap_or(u64::MAX)
    }

    fn available_indices(&self, now: u64, exclude: &[usize]) -> Vec<usize> {
        (0..self.backends.len())
            .filter(|i| !exclude.contains(i) && self.backends[*i].is_available(now))
            .collect()
    }

    /// Choose a backend for this request, honoring the strategy and skipping any
    /// index in `exclude` (used by failover). Returns None when nothing is
    /// available. `poll(now)` must have been called first.
    pub fn select(&mut self, now: u64, key: Option<&str>, exclude: &[usize]) -> Option<usize> {
        let available = self.available_indices(now, exclude);
        if available.is_empty() {
            return None;
        }
        let chosen = match self.strategy {
            Strategy::RoundRobin => self.select_round_robin(&available),
            Strategy::Weighted => self.select_weighted(&available)?,
            Strategy::LeastConnections => self.select_least_conn(&available),
            Strategy::ConsistentHash => self.select_hash(&available, key),
        };
        debug_assert!(self.backends[chosen].is_available(now));
        Some(chosen)
    }

    fn select_round_robin(&mut self, available: &[usize]) -> usize {
        let pick = available[self.rr_cursor % available.len()];
        self.rr_cursor = self.rr_cursor.wrapping_add(1);
        pick
    }

    fn select_weighted(&mut self, available: &[usize]) -> Option<usize> {
        // Zero weight nodes receive no traffic. If every available node has
        // weight zero the pool has no capacity under this strategy. Nodes on a
        // reinstatement ramp carry a fraction of their weight, which the
        // smooth WRR arithmetic reproduces exactly.
        let mut best: Option<usize> = None;
        let mut total: i64 = 0;
        for &i in available {
            if self.backends[i].weight == 0 {
                continue;
            }
            let score = self.weighted_score(i);
            total += score;
            let w = &mut self.backends[i];
            w.current_weight += score;
            match best {
                None => best = Some(i),
                Some(b) => {
                    if w.current_weight > self.backends[b].current_weight {
                        best = Some(i);
                    }
                }
            }
        }
        if let Some(b) = best {
            self.backends[b].current_weight -= total;
        }
        best
    }

    /// Effective weight of a node for the weighted strategy: configured weight
    /// scaled by the reinstatement ramp percent. With no EWMA layer the ramp is
    /// always 100, so this is exactly the configured weight.
    fn weighted_score(&self, i: usize) -> i64 {
        let b = &self.backends[i];
        i64::from(b.weight) * i64::from(b.ramp_percent)
    }

    fn select_least_conn(&self, available: &[usize]) -> usize {
        let mut best = available[0];
        for &i in available {
            if self.backends[i].in_flight < self.backends[best].in_flight {
                best = i;
            }
        }
        best
    }

    fn select_hash(&mut self, available: &[usize], key: Option<&str>) -> usize {
        let Some(key) = key else {
            return self.select_round_robin(available);
        };
        if self.ring_signature != available || self.ring.is_empty() {
            let members: Vec<(usize, &str)> = available
                .iter()
                .map(|&i| (i, self.backends[i].name.as_str()))
                .collect();
            self.ring.rebuild(&members);
            self.ring_signature = available.to_vec();
        }
        self.ring.lookup(key).unwrap_or(available[0])
    }

    pub fn begin(&mut self, idx: usize) {
        self.backends[idx].in_flight += 1;
    }

    pub fn end(&mut self, idx: usize) {
        let b = &mut self.backends[idx];
        b.in_flight = b.in_flight.saturating_sub(1);
    }

    /// Send a request to a chosen backend. Does not touch `in_flight`, breaker, or
    /// health. The proxy owns that lifecycle via `begin`, `record_*`, and `end`.
    pub fn dispatch(&self, idx: usize, req: &crate::http::Request) -> UpstreamReply {
        self.backends[idx].upstream.send(req)
    }

    /// Feed a successful outcome back into the backend health and circuit state.
    pub fn record_success(&mut self, idx: usize, now: u64) {
        let b = &mut self.backends[idx];
        b.served += 1;
        b.outlier_failures = 0;
        b.breaker.on_success(now);
    }

    /// Like `record_success` and additionally fold the request latency into the
    /// node's EWMA when the EWMA layer is enabled. Callers without a latency
    /// measurement can keep using `record_success`, which never touches the
    /// EWMA so it cannot skew the pool mean.
    pub fn record_success_latency(&mut self, idx: usize, now: u64, latency_ms: u64) {
        self.record_success(idx, now);
        let Some(cfg) = self.ewma else { return };
        let b = &mut self.backends[idx];
        b.latency_ewma_milli = Some(cfg.fold(b.latency_ewma_milli, latency_ms));
        b.ewma_samples += 1;
    }

    /// Attach the EWMA latency outlier layer to this pool.
    pub fn enable_ewma(&mut self, cfg: EwmaConfig) {
        self.ewma = Some(cfg);
    }

    pub fn ewma_config(&self) -> Option<EwmaConfig> {
        self.ewma
    }

    /// Distribution report: what each node actually received versus its share
    /// of the currently available effective weight, plus EWMA and ramp state.
    // Percent display math on bounded counts, far below the f64 precision limit.
    #[allow(clippy::cast_precision_loss)]
    pub fn distribution(&self, now: u64) -> Vec<DistributionRow> {
        let total_served: u64 = self.backends.iter().map(|b| b.served).sum();
        let available = self.available_indices(now, &[]);
        let total_score: i64 = available.iter().map(|&i| self.weighted_score(i)).sum();
        (0..self.backends.len())
            .map(|i| {
                let b = &self.backends[i];
                let expected_share_pct = if total_score > 0 && available.contains(&i) {
                    100.0 * self.weighted_score(i) as f64 / total_score as f64
                } else {
                    0.0
                };
                DistributionRow {
                    name: b.name.clone(),
                    served: b.served,
                    served_share_pct: if total_served > 0 {
                        100.0 * b.served as f64 / total_served as f64
                    } else {
                        0.0
                    },
                    expected_share_pct,
                    weight: b.weight,
                    ewma_milli: b.latency_ewma_milli,
                    ejected: !b.is_available(now),
                    ramp_percent: b.ramp_percent,
                    ewma_ejections: b.ewma_ejections,
                }
            })
            .collect()
    }

    /// Feed a failed outcome back in. May trip the breaker or eject the backend
    /// as an outlier once the consecutive failure threshold is reached. A single
    /// continuous ejection counts exactly once: failures that land while the
    /// backend is already ejected neither re count nor extend the cooldown.
    pub fn record_failure(&mut self, idx: usize, now: u64) {
        let outlier_threshold = self.health.outlier_consecutive_failures;
        let ejection_base = self.health.ejection_base_ms;
        let b = &mut self.backends[idx];
        b.served += 1;
        b.breaker.on_failure(now);
        b.outlier_failures += 1;
        let already_ejected = matches!(b.ejected_until, Some(until) if now < until);
        if b.outlier_failures >= outlier_threshold && !already_ejected {
            b.ejected_until = Some(now.saturating_add(ejection_base));
            b.ejections += 1;
        }
    }

    /// Run active health probes for any backend whose probe interval has elapsed.
    pub fn run_health_checks(&mut self, now: u64) {
        let interval = self.health.probe_interval_ms;
        let up_thresh = self.health.healthy_probe_threshold;
        let down_thresh = self.health.unhealthy_probe_threshold;
        for b in &mut self.backends {
            let due = match b.last_probe {
                None => true,
                Some(t) => now.saturating_sub(t) >= interval,
            };
            if !due {
                continue;
            }
            b.last_probe = Some(now);
            let live = b.upstream.probe();
            if live {
                b.probe_successes += 1;
                b.probe_failures = 0;
                if !b.healthy && b.probe_successes >= up_thresh {
                    b.healthy = true;
                    b.probe_successes = 0;
                }
            } else {
                b.probe_failures += 1;
                b.probe_successes = 0;
                if b.healthy && b.probe_failures >= down_thresh {
                    b.healthy = false;
                    b.probe_failures = 0;
                }
            }
        }
    }

    /// Convenience for building a pool over an injected clock.
    pub fn poll_with(&mut self, clock: &dyn Clock) {
        self.poll(clock.now_millis());
    }
}

#[cfg(test)]
// Percent math on small bounded counts in these tests.
#[allow(clippy::cast_precision_loss)]
mod tests {
    use super::*;
    use crate::upstream::{MockUpstream, Step, UpstreamError};

    fn pool_with(strategy: Strategy, names_weights: &[(&str, u32)]) -> Pool {
        let health = HealthConfig::default();
        let mut p = Pool::new("svc", strategy, health);
        for (n, w) in names_weights {
            let up = Box::new(MockUpstream::healthy(*n));
            p.add_backend(Backend::new(*n, *w, up, &health, BreakerConfig::default()));
        }
        p
    }

    #[test]
    fn round_robin_exact_cycle() {
        let mut p = pool_with(Strategy::RoundRobin, &[("a", 1), ("b", 1), ("c", 1)]);
        p.poll(0);
        let picks: Vec<usize> = (0..6).map(|_| p.select(0, None, &[]).unwrap()).collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn least_conn_prefers_idle() {
        let mut p = pool_with(Strategy::LeastConnections, &[("a", 1), ("b", 1), ("c", 1)]);
        p.poll(0);
        p.begin(0);
        p.begin(0);
        p.begin(1);
        let pick = p.select(0, None, &[]).unwrap();
        assert_eq!(pick, 2);
    }

    #[test]
    fn open_breaker_is_never_selected() {
        let mut p = pool_with(Strategy::RoundRobin, &[("a", 1), ("b", 1)]);
        // Trip backend 0.
        for _ in 0..BreakerConfig::default().failure_threshold {
            p.record_failure(0, 0);
        }
        p.poll(0);
        assert_eq!(p.backend(0).breaker_state(), BreakerState::Open);
        for _ in 0..20 {
            let pick = p.select(0, None, &[]).unwrap();
            assert_eq!(pick, 1, "must never pick the open breaker");
        }
    }

    #[test]
    fn outlier_ejection_and_reinstatement() {
        let mut p = pool_with(Strategy::RoundRobin, &[("a", 1), ("b", 1)]);
        let n = HealthConfig::default().outlier_consecutive_failures;
        for _ in 0..n {
            p.record_failure(0, 100);
        }
        p.poll(100);
        assert!(p.backend(0).is_ejected(100));
        assert_eq!(p.backend(0).ejections(), 1);
        // Before cooldown, only backend 1 is selectable.
        assert_eq!(p.select(100, None, &[]).unwrap(), 1);
        // After cooldown it comes back (breaker also recovered by then).
        let t = 100 + HealthConfig::default().ejection_base_ms + 10;
        p.poll(t);
        assert!(!p.backend(0).is_ejected(t));
    }

    #[test]
    fn active_health_flips_unhealthy_then_recovers() {
        let health = HealthConfig::default();
        let mut p = Pool::new("svc", Strategy::RoundRobin, health);
        let up = MockUpstream::new("a");
        up.push_probes([false, false]);
        p.add_backend(Backend::new("a", 1, Box::new(up), &health, BreakerConfig::default()));

        p.run_health_checks(0);
        assert!(p.backend(0).is_healthy());
        p.run_health_checks(1000);
        p.run_health_checks(2000);
        assert!(!p.backend(0).is_healthy(), "two failed probes eject it");
        // Default probe result is true, so it recovers after the up threshold.
        p.run_health_checks(3000);
        p.run_health_checks(4000);
        assert!(p.backend(0).is_healthy());
    }

    #[test]
    fn weighted_distribution_matches_weights() {
        let mut p = pool_with(Strategy::Weighted, &[("a", 1), ("b", 1), ("c", 3)]);
        p.poll(0);
        let mut counts = [0usize; 3];
        for _ in 0..5000 {
            counts[p.select(0, None, &[]).unwrap()] += 1;
        }
        // c has weight 3 of total 5, so about 60 percent.
        let frac_c = counts[2] as f64 / 5000.0;
        assert!((frac_c - 0.6).abs() < 0.02, "c fraction {frac_c}");
        assert!((counts[0] as f64 / 5000.0 - 0.2).abs() < 0.02);
    }

    #[test]
    fn dispatch_reaches_upstream() {
        let health = HealthConfig::default();
        let mut p = Pool::new("svc", Strategy::RoundRobin, health);
        let up = MockUpstream::new("a");
        up.push(Step::fail(UpstreamError::Timeout));
        p.add_backend(Backend::new("a", 1, Box::new(up), &health, BreakerConfig::default()));
        p.poll(0);
        let idx = p.select(0, None, &[]).unwrap();
        let reply = p.dispatch(idx, &crate::http::Request::get("/"));
        assert!(reply.outcome.is_err());
    }
}

