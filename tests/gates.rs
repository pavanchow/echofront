//! The correctness gates. These are the load bearing proofs of Echofront's
//! claims. They are bounded for CI and reproducible: set ECHOFRONT_FUZZ_OPS to
//! change the workload size and ECHOFRONT_FUZZ_SEED to change the seed. Same seed
//! gives the same timeline every run.
//!
//! Gate 1: load balancing correctness (round robin cycle, weighted proportion,
//!         consistent hash minimal remap).
//! Gate 2: circuit breaker state machine vs an independent reference model.
//! Gate 3: health checks over the injected clock eject and reinstate on schedule.
//! Gate 4: the proxy never selects an unhealthy, ejected, or open circuit upstream.

use echofront::breaker::{BreakerConfig, BreakerState, CircuitBreaker};
use echofront::hashring::ConsistentHashRing;
use echofront::pool::{Backend, HealthConfig, Pool, Strategy};
use echofront::rng::Rng;
use echofront::upstream::MockUpstream;

fn ops() -> usize {
    std::env::var("ECHOFRONT_FUZZ_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000)
}

fn seed() -> u64 {
    std::env::var("ECHOFRONT_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x00C0_FFEE_1234_5678)
}

fn health() -> HealthConfig {
    HealthConfig::default()
}

fn make_pool(strategy: Strategy, names_weights: &[(&str, u32)]) -> Pool {
    let h = health();
    let mut p = Pool::new("svc", strategy, h);
    for (n, w) in names_weights {
        let up = Box::new(MockUpstream::healthy(*n));
        p.add_backend(Backend::new(*n, *w, up, &h, BreakerConfig::default()));
    }
    p
}

// ------------------------------------------------------------------ Gate 1

#[test]
fn gate1_round_robin_exact_cycle() {
    let mut p = make_pool(Strategy::RoundRobin, &[("a", 1), ("b", 1), ("c", 1), ("d", 1)]);
    p.poll(0);
    let cycles = 250;
    let picks: Vec<usize> = (0..cycles * p.len())
        .map(|_| p.select(0, None, &[]).unwrap())
        .collect();
    // Every window of len consecutive picks is a permutation of all backends.
    for chunk in picks.chunks(p.len()) {
        let mut sorted = chunk.to_vec();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..p.len()).collect::<Vec<_>>(), "not an exact cycle");
    }
}

#[test]
fn gate1_weighted_matches_weights() {
    let weights = [("a", 1u32), ("b", 2), ("c", 3), ("d", 4)];
    let mut p = make_pool(Strategy::Weighted, &weights);
    p.poll(0);
    let n = ops().max(4000);
    let mut counts = vec![0usize; weights.len()];
    for _ in 0..n {
        counts[p.select(0, None, &[]).unwrap()] += 1;
    }
    let total_weight: u32 = weights.iter().map(|(_, w)| *w).sum();
    for (i, (_, w)) in weights.iter().enumerate() {
        let expected = *w as f64 / total_weight as f64;
        let actual = counts[i] as f64 / n as f64;
        assert!(
            (actual - expected).abs() < 0.02,
            "backend {i}: expected {expected:.3} got {actual:.3}"
        );
    }
}

#[test]
fn gate1_consistent_hash_minimal_remap() {
    let members4 = [(0usize, "a"), (1, "b"), (2, "c"), (3, "d")];
    let mut before = ConsistentHashRing::new(200);
    before.rebuild(&members4);

    let k = ops().max(4000);
    let keys: Vec<String> = (0..k).map(|i| format!("session-{i}")).collect();
    let base: Vec<usize> = keys.iter().map(|key| before.lookup(key).unwrap()).collect();

    // Remove one node.
    let mut after = ConsistentHashRing::new(200);
    after.rebuild(&[(0, "a"), (1, "b"), (2, "c")]);
    let mut moved = 0usize;
    for (key, &old) in keys.iter().zip(&base) {
        let new = after.lookup(key).unwrap();
        if new != old {
            moved += 1;
            assert_eq!(old, 3, "only keys on the removed node may move");
        }
    }
    let frac = moved as f64 / k as f64;
    assert!(frac > 0.15 && frac < 0.35, "remap fraction {frac} not near 1/4");

    // Add a node back (a fifth). Only about 1/5 of keys should move, all onto the
    // new node.
    let mut grown = ConsistentHashRing::new(200);
    grown.rebuild(&[(0, "a"), (1, "b"), (2, "c"), (3, "d"), (4, "e")]);
    let mut moved_add = 0usize;
    for (key, &old) in keys.iter().zip(&base) {
        let new = grown.lookup(key).unwrap();
        if new != old {
            moved_add += 1;
            assert_eq!(new, 4, "keys that move must move onto the new node");
        }
    }
    let frac_add = moved_add as f64 / k as f64;
    assert!(frac_add > 0.12 && frac_add < 0.28, "add remap {frac_add} not near 1/5");
}

// ------------------------------------------------------------------ Gate 2

/// Independent reference model of the breaker, written to the same spec.
#[derive(Clone, Copy, PartialEq, Debug)]
enum RefState {
    Closed(u32),
    Open(u64),
    Half(u32),
}

struct RefBreaker {
    fail_thresh: u32,
    succ_thresh: u32,
    cooldown: u64,
    state: RefState,
}

impl RefBreaker {
    fn new(cfg: BreakerConfig) -> Self {
        Self {
            fail_thresh: cfg.failure_threshold,
            succ_thresh: cfg.success_threshold,
            cooldown: cfg.cooldown_ms,
            state: RefState::Closed(0),
        }
    }
    fn poll(&mut self, now: u64) {
        if let RefState::Open(since) = self.state {
            if now.saturating_sub(since) >= self.cooldown {
                self.state = RefState::Half(0);
            }
        }
    }
    fn on_success(&mut self, now: u64) {
        self.poll(now);
        self.state = match self.state {
            RefState::Closed(_) => RefState::Closed(0),
            RefState::Half(s) => {
                if s + 1 >= self.succ_thresh {
                    RefState::Closed(0)
                } else {
                    RefState::Half(s + 1)
                }
            }
            RefState::Open(x) => RefState::Open(x),
        };
    }
    fn on_failure(&mut self, now: u64) {
        self.poll(now);
        self.state = match self.state {
            RefState::Closed(f) => {
                if f + 1 >= self.fail_thresh {
                    RefState::Open(now)
                } else {
                    RefState::Closed(f + 1)
                }
            }
            RefState::Half(_) => RefState::Open(now),
            RefState::Open(x) => RefState::Open(x),
        };
    }
    fn callable(&mut self, now: u64) -> bool {
        self.poll(now);
        !matches!(self.state, RefState::Open(_))
    }
    fn matches(&self, real: BreakerState) -> bool {
        matches!(
            (self.state, real),
            (RefState::Closed(_), BreakerState::Closed)
                | (RefState::Open(_), BreakerState::Open)
                | (RefState::Half(_), BreakerState::HalfOpen)
        )
    }
}

#[test]
fn gate2_breaker_matches_reference_model() {
    let mut rng = Rng::new(seed());
    let n = ops();
    let cfg = BreakerConfig {
        failure_threshold: 3,
        success_threshold: 2,
        cooldown_ms: 1000,
    };
    let mut real = CircuitBreaker::new(cfg);
    let mut refm = RefBreaker::new(cfg);
    let mut now: u64 = 0;

    for _ in 0..n {
        match rng.below(3) {
            0 => {
                real.on_success(now);
                refm.on_success(now);
            }
            1 => {
                real.on_failure(now);
                refm.on_failure(now);
            }
            _ => {
                now += rng.below(600);
                real.poll(now);
                refm.poll(now);
            }
        }
        real.poll(now);
        refm.poll(now);
        assert!(
            refm.matches(real.state()),
            "state diverged: ref={:?} real={:?} at t={now}",
            refm.state,
            real.state()
        );
        assert_eq!(
            real.is_callable(now),
            refm.callable(now),
            "callability diverged at t={now}"
        );
        // An open breaker is not callable.
        if real.state() == BreakerState::Open {
            assert!(!real.is_callable(now), "open breaker must not be callable");
        }
    }
}

// ------------------------------------------------------------------ Gate 3

#[test]
fn gate3_health_ejection_and_reinstatement_deterministic() {
    let h = HealthConfig {
        probe_interval_ms: 1000,
        unhealthy_probe_threshold: 3,
        healthy_probe_threshold: 2,
        outlier_consecutive_failures: 5,
        ejection_base_ms: 5000,
        start_healthy: true,
    };

    let run = || {
        let mut p = Pool::new("svc", Strategy::RoundRobin, h);
        let up = MockUpstream::new("a");
        // Fails probes for a stretch, then recovers.
        up.push_probes([false, false, false, false, false, false]);
        // After the pushed probes, default probe is true (recovery).
        p.add_backend(Backend::new("a", 1, Box::new(up), &h, BreakerConfig::default()));
        p.add_backend(Backend::new(
            "b",
            1,
            Box::new(MockUpstream::healthy("b")),
            &h,
            BreakerConfig::default(),
        ));

        let mut eject_time = None;
        let mut reinstate_time = None;
        let mut t = 0u64;
        for _ in 0..30 {
            p.run_health_checks(t);
            if eject_time.is_none() && !p.backend(0).is_healthy() {
                eject_time = Some(t);
            }
            if eject_time.is_some() && reinstate_time.is_none() && p.backend(0).is_healthy() {
                reinstate_time = Some(t);
            }
            t += 1000;
        }
        (eject_time, reinstate_time)
    };

    let a = run();
    let b = run();
    assert_eq!(a, b, "health timeline must be deterministic for the same script");
    let (eject, reinstate) = a;
    let eject = eject.expect("backend should be ejected");
    let reinstate = reinstate.expect("backend should be reinstated");
    // Ejected within the unhealthy window (3 failed probes at 1s cadence).
    assert!(eject <= 3000, "ejected too late at {eject}");
    // Reinstated after 2 good probes once the script recovers.
    assert!(reinstate > eject, "reinstate {reinstate} must follow eject {eject}");
}

// ------------------------------------------------------------------ Gate 4

#[test]
fn gate4_never_selects_unavailable_upstream() {
    let mut rng = Rng::new(seed());
    let n = ops();

    let strategies = [
        Strategy::RoundRobin,
        Strategy::Weighted,
        Strategy::LeastConnections,
        Strategy::ConsistentHash,
    ];
    let strategy = strategies[rng.index(strategies.len())];
    let mut p = make_pool(strategy, &[("a", 1), ("b", 2), ("c", 1), ("d", 3), ("e", 1)]);

    let mut now: u64 = 0;
    let mut selections = 0u64;

    for _ in 0..n {
        now += rng.below(400);
        p.poll(now);

        // Three quarters of the time route a request and verify the chosen
        // backend was available, otherwise just let the clock advance.
        if rng.below(4) < 3 {
            let key = format!("k-{}", rng.below(64));
            if let Some(idx) = p.select(now, Some(&key), &[]) {
                selections += 1;
                let b = p.backend(idx);
                assert!(
                    b.is_available(now),
                    "selected unavailable backend {} at t={now}: healthy={} ejected={} breaker={:?}",
                    b.name,
                    b.is_healthy(),
                    b.is_ejected(now),
                    b.breaker_state()
                );
                assert_ne!(
                    b.breaker_state(),
                    BreakerState::Open,
                    "selected an open breaker"
                );
                // Simulate an outcome so state evolves.
                p.begin(idx);
                if rng.chance(0.35) {
                    p.record_failure(idx, now);
                } else {
                    p.record_success(idx, now);
                }
                p.end(idx);
            }
        }
    }
    assert!(selections > 0, "workload made no selections");
}
