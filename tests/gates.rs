//! The correctness gates. These are the load bearing proofs of Echofront's
//! claims. They are bounded for CI and reproducible: set `ECHOFRONT_FUZZ_OPS` to
//! change the workload size and `ECHOFRONT_FUZZ_SEED` to change the seed. Same seed
//! gives the same timeline every run.
//!
//! Gate 1: load balancing correctness (round robin cycle, weighted proportion,
//!         consistent hash minimal remap).
//! Gate 2: circuit breaker state machine vs an independent reference model.
//! Gate 3: health checks over the injected clock eject and reinstate on schedule.
//! Gate 4: the proxy never selects an unhealthy, ejected, or open circuit upstream.

// Gate share math uses small bounded counters, far below the f64 precision
// limit, so the pedantic cast lints are noise here.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use echofront::breaker::{BreakerConfig, BreakerState, CircuitBreaker};
use echofront::clock::ManualClock;
use echofront::ewma::EwmaConfig;
use echofront::hashring::ConsistentHashRing;
use echofront::http::Request;
use echofront::pool::{Backend, HealthConfig, Pool, Strategy};
use echofront::proxy::Proxy;
use echofront::retry::RetryConfig;
use echofront::rng::Rng;
use echofront::router::{Route, Router};
use echofront::upstream::{MockUpstream, Step, UpstreamError};

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
        let expected = f64::from(*w) / f64::from(total_weight);
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
        half_open_max_calls: 1,
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

// ------------------------------------------------------------------ Gate 5
// Adversarial edge cases added during hardening.

#[test]
fn gate5_zero_weight_never_selected() {
    let mut p = make_pool(Strategy::Weighted, &[("a", 0), ("b", 3)]);
    p.poll(0);
    for i in 0..300 {
        assert_eq!(
            p.select(0, None, &[]).unwrap(),
            1,
            "zero weight node got traffic on pick {i}"
        );
    }
    // A weighted pool whose available nodes all have weight zero has no
    // capacity, so selection fails honestly instead of ignoring the weight.
    let mut p0 = make_pool(Strategy::Weighted, &[("a", 0), ("b", 0)]);
    p0.poll(0);
    assert!(p0.select(0, None, &[]).is_none(), "all zero weight pool selected someone");
    // Zero weight only affects the weighted strategy: other strategies still
    // consider the node routable, which keeps the strategies orthogonal.
    let mut p1 = make_pool(Strategy::RoundRobin, &[("a", 0), ("b", 1)]);
    p1.poll(0);
    assert!(p1.select(0, None, &[]).is_some());
}

#[test]
fn gate5_continuous_ejection_counts_once() {
    let h = HealthConfig {
        outlier_consecutive_failures: 3,
        ejection_base_ms: 500,
        ..HealthConfig::default()
    };
    let mut p = Pool::new("svc", Strategy::RoundRobin, h);
    for n in ["a", "b"] {
        p.add_backend(Backend::new(
            n,
            1,
            Box::new(MockUpstream::healthy(n)),
            &h,
            BreakerConfig::default(),
        ));
    }
    // First failure run arms the ejection.
    for t in 0..3u64 {
        p.record_failure(0, 100 + t);
    }
    p.poll(102);
    assert!(p.backend(0).is_ejected(102));
    assert_eq!(p.backend(0).ejections(), 1);
    // In flight failures landing while already ejected must not re count or
    // extend the same ejection.
    p.record_failure(0, 103);
    p.record_failure(0, 104);
    p.record_failure(0, 105);
    assert_eq!(p.backend(0).ejections(), 1, "one continuous ejection counted twice");
    assert!(
        p.backend(0).is_ejected(599),
        "cooldown was extended past the armed expiry"
    );
    assert!(!p.backend(0).is_ejected(602), "cooldown must expire on schedule");
    p.poll(700);
    assert_eq!(p.backend(0).ejections(), 1, "counter moved without a fresh run");
}

#[test]
fn gate5_ejection_time_never_overflows() {
    let h = HealthConfig {
        outlier_consecutive_failures: 1,
        ejection_base_ms: 5_000,
        ..HealthConfig::default()
    };
    let mut p = Pool::new("svc", Strategy::RoundRobin, h);
    p.add_backend(Backend::new(
        "a",
        1,
        Box::new(MockUpstream::healthy("a")),
        &h,
        BreakerConfig::default(),
    ));
    let now = u64::MAX - 10;
    p.record_failure(0, now);
    assert!(p.backend(0).is_ejected(now));
    // Saturated expiry: the backend stays ejected essentially forever.
    assert!(p.backend(0).is_ejected(u64::MAX - 1));
}

#[test]
fn gate5_probe_cadence_matches_interval() {
    let h = HealthConfig {
        probe_interval_ms: 1_000,
        unhealthy_probe_threshold: 2,
        healthy_probe_threshold: 2,
        ..HealthConfig::default()
    };
    let mut p = Pool::new("svc", Strategy::RoundRobin, h);
    let up = MockUpstream::new("a");
    up.push_probes([false, false]);
    p.add_backend(Backend::new("a", 1, Box::new(up), &h, BreakerConfig::default()));
    // Probes land exactly one interval apart: two failures flip the backend at
    // the second probe, not one interval later.
    p.run_health_checks(0);
    assert!(p.backend(0).is_healthy());
    p.run_health_checks(1000);
    assert!(
        !p.backend(0).is_healthy(),
        "two failed probes one interval apart must eject by t=1000"
    );
    // Default probe is true after the script runs out: two good probes
    // reinstate, again one interval apart.
    p.run_health_checks(2000);
    assert!(!p.backend(0).is_healthy());
    p.run_health_checks(3000);
    p.run_health_checks(4000);
    assert!(p.backend(0).is_healthy(), "two good probes must reinstate");
}

#[test]
fn gate5_half_open_admits_limited_concurrent_calls() {
    let breaker_cfg = BreakerConfig {
        failure_threshold: 1,
        success_threshold: 1,
        cooldown_ms: 100,
        half_open_max_calls: 1,
    };
    let h = HealthConfig::default();
    let mut p = Pool::new("svc", Strategy::RoundRobin, h);
    p.add_backend(Backend::new(
        "a",
        1,
        Box::new(MockUpstream::healthy("a")),
        &h,
        breaker_cfg,
    ));
    // Trip it, then let the cooldown elapse into half open.
    p.record_failure(0, 0);
    p.poll(100);
    assert_eq!(p.backend(0).breaker_state(), BreakerState::HalfOpen);
    // The trial slot is free: one call may enter.
    let idx = p.select(100, None, &[]).expect("half open admits a trial");
    assert_eq!(idx, 0);
    p.begin(0);
    // With the trial slot held, nothing else may be selected.
    assert!(
        p.select(100, None, &[]).is_none(),
        "half open admitted a second concurrent call"
    );
    p.end(0);
    // Slot released: callable again.
    assert!(p.select(100, None, &[]).is_some());
    // A success closes the breaker and the cap stops applying.
    p.record_success(0, 100);
    assert_eq!(p.backend(0).breaker_state(), BreakerState::Closed);
    p.begin(0);
    assert!(p.select(100, None, &[]).is_some());
    p.end(0);
}

#[test]
fn gate5_single_node_breaker_open_is_total_outage() {
    let h = HealthConfig::default();
    let mut p = Pool::new("svc", Strategy::RoundRobin, h);
    p.add_backend(Backend::new(
        "a",
        1,
        Box::new(MockUpstream::healthy("a")),
        &h,
        BreakerConfig::default(),
    ));
    // Trip the only node: selection must fail honestly, not fall through.
    let threshold = u64::from(BreakerConfig::default().failure_threshold);
    for t in 0..threshold {
        p.record_failure(0, t);
    }
    let opened_at = threshold - 1;
    p.poll(opened_at);
    assert_eq!(p.backend(0).breaker_state(), BreakerState::Open);
    for i in 0..10 {
        assert!(
            p.select(opened_at, None, &[]).is_none(),
            "pool of one must return None while its breaker is open (probe {i})"
        );
    }
    // After the cooldown the node is a half open trial and selectable again.
    let half_open_at = opened_at + BreakerConfig::default().cooldown_ms;
    p.poll(half_open_at);
    assert!(p.select(half_open_at, None, &[]).is_some());
    p.record_success(0, half_open_at + 1);
    assert_eq!(p.backend(0).breaker_state(), BreakerState::HalfOpen);
}

#[test]
fn gate5_all_nodes_fail_then_recover_in_different_orders() {
    let h = HealthConfig {
        probe_interval_ms: 1_000,
        unhealthy_probe_threshold: 2,
        healthy_probe_threshold: 2,
        outlier_consecutive_failures: 5,
        ejection_base_ms: 1_500,
        start_healthy: true,
    };
    let breaker_cfg = BreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        cooldown_ms: 900,
        half_open_max_calls: 1,
    };
    let mut p = Pool::new("svc", Strategy::RoundRobin, h);
    // Each node fails a different number of times, so they recover in a
    // different order than they failed.
    for (i, n) in ["a", "b", "c", "d"].iter().enumerate() {
        let up = MockUpstream::new(*n);
        for _ in 0..((i + 1) * 3) {
            up.push(Step::fail(UpstreamError::Connection));
        }
        p.add_backend(Backend::new(*n, 1, Box::new(up), &h, breaker_cfg));
    }

    let mut now: u64 = 0;
    let mut ever_unavailable_pick = false;
    // Failure phase with failover within each request.
    for r in 0..40u64 {
        now += 120;
        p.poll(now);
        let mut tried: Vec<usize> = Vec::new();
        while let Some(idx) = p.select(now, None, &tried) {
            assert!(
                p.backend(idx).is_available(now),
                "selected unavailable backend at t={now}"
            );
            p.begin(idx);
            let reply = p.dispatch(idx, &Request::get("/"));
            let ok = reply.outcome.is_ok();
            if ok {
                p.record_success(idx, now);
            } else {
                p.record_failure(idx, now);
            }
            p.end(idx);
            if ok || tried.len() >= p.len() {
                break;
            }
            tried.push(idx);
            ever_unavailable_pick = true;
        }
        let _ = r;
    }
    assert!(ever_unavailable_pick, "script never exercised failover");
    // Recovery phase: leftover scripted failures drain first, then everything
    // succeeds and every node ends available.
    for r in 0..30u64 {
        now += 1_100;
        p.poll(now);
        let idx = p.select(now, None, &[]).expect("pool must have capacity");
        p.begin(idx);
        let reply = p.dispatch(idx, &Request::get("/"));
        if r >= 15 {
            assert!(
                reply.outcome.is_ok(),
                "request {r} failed during late recovery at t={now}"
            );
        }
        match reply.outcome {
            Ok(_) => p.record_success(idx, now),
            Err(_) => p.record_failure(idx, now),
        }
        p.end(idx);
    }
    p.poll(now + 10_000);
    for b in p.backends() {
        assert!(
            b.is_available(now + 10_000),
            "backend {} did not recover: healthy={} breaker={:?}",
            b.name,
            b.is_healthy(),
            b.breaker_state()
        );
    }
}

#[test]
fn gate5_sticky_key_never_lands_on_ejected_node() {
    let h = HealthConfig {
        outlier_consecutive_failures: 2,
        ejection_base_ms: 30_000,
        ..HealthConfig::default()
    };
    let mut p = Pool::new("svc", Strategy::ConsistentHash, h);
    for n in ["a", "b", "c", "d"] {
        p.add_backend(Backend::new(
            n,
            1,
            Box::new(MockUpstream::healthy(n)),
            &h,
            BreakerConfig::default(),
        ));
    }
    p.poll(0);
    // Eject node a. Sticky keys that hashed to it must move to a survivor.
    p.record_failure(0, 10);
    p.record_failure(0, 11);
    p.poll(11);
    assert!(p.backend(0).is_ejected(11));
    for i in 0..500 {
        let key = format!("session-{i}");
        let idx = p.select(11, Some(&key), &[]).expect("survivors available");
        let b = p.backend(idx);
        assert!(
            b.is_available(11),
            "sticky key {key} landed on unavailable node {} (probe {i})",
            b.name
        );
    }
    // Reinstatement: keys may return home, and always to an available node.
    p.poll(30_011);
    for i in 0..500 {
        let key = format!("session-{i}");
        let idx = p.select(30_011, Some(&key), &[]).expect("all nodes back");
        assert!(p.backend(idx).is_available(30_011));
    }
}

#[test]
fn gate5_retry_budget_bounds_amplification() {
    let clock = std::rc::Rc::new(ManualClock::new(0));
    let retry_cfg = RetryConfig::default();
    let h = HealthConfig::default();
    let mut pool = Pool::new("svc", Strategy::RoundRobin, h);
    pool.add_backend(Backend::new(
        "bad-a",
        1,
        Box::new(MockUpstream::always_failing("bad-a", UpstreamError::Connection)),
        &h,
        BreakerConfig::default(),
    ));
    pool.add_backend(Backend::new(
        "bad-b",
        1,
        Box::new(MockUpstream::always_failing("bad-b", UpstreamError::Connection)),
        &h,
        BreakerConfig::default(),
    ));
    let mut router = Router::new();
    router.add(Route::new(None, "/", "svc"));
    let mut proxy = Proxy::new(clock.clone(), router, retry_cfg);
    proxy.add_pool(pool);

    let n = 500u64;
    let mut retries_seen = 0u64;
    for r in 0..n {
        let res = proxy.handle(&Request::get("/").with_header("Host", "h"));
        assert!(res.response.is_err(), "request {r} unexpectedly succeeded");
        let attempts = res.attempts.len() as u32;
        assert!(
            attempts <= retry_cfg.max_retries_per_request + 1,
            "request {r} made {attempts} attempts, above the configured maximum"
        );
        retries_seen += res.attempts.iter().filter(|a| a.is_retry).count() as u64;
    }
    // Token arithmetic: the bucket starts at max_milli and each request
    // deposits deposit_milli, so total granted retries can never exceed
    // (max_milli + n * deposit) / cost.
    let bound = (retry_cfg.max_milli + n as i64 * retry_cfg.deposit_milli)
        / retry_cfg.retry_cost_milli;
    assert!(
        (retries_seen as i64) <= bound,
        "retries {retries_seen} exceeded token arithmetic bound {bound}"
    );
    assert!(retries_seen > 0, "budget never allowed a single retry");
}

// ------------------------------------------------------------------ Gate 6
// EWMA latency scoring: outlier ejection, gradual reinstatement, and the
// distribution report.

fn ewma_cfg() -> EwmaConfig {
    EwmaConfig {
        alpha_milli: 500,
        factor_milli: 3_000,
        window_ms: 1_000,
        min_samples: 4,
        ejection_ms: 2_000,
        reinstate_percent: 25,
        ramp_window_ms: 1_000,
    }
}

/// Drive one weighted request at `now` and return (idx, latency). Asserts the
/// never select unavailable invariant on every call.
fn ewma_drive(p: &mut Pool, now: u64) -> (usize, u64) {
    p.poll(now);
    let idx = p.select(now, None, &[]).expect("pool must have capacity");
    let b = p.backend(idx);
    assert!(b.is_available(now), "selected unavailable backend at t={now}");
    p.begin(idx);
    let reply = p.dispatch(idx, &Request::get("/"));
    let latency = reply.latency_ms;
    assert!(reply.outcome.is_ok(), "scripted latency node must answer ok");
    p.record_success_latency(idx, now, latency);
    p.end(idx);
    (idx, latency)
}

#[test]
fn gate6_ewma_ejects_worst_node_and_reinstates_gradually() {
    let cfg = ewma_cfg();
    let h = HealthConfig::default();
    let mut p = Pool::new("svc", Strategy::Weighted, h);
    p.enable_ewma(cfg);
    for n in ["f0", "f1", "f2"] {
        p.add_backend(Backend::new(
            n,
            1,
            Box::new(MockUpstream::healthy(n)),
            &h,
            BreakerConfig::default(),
        ));
    }
    // The slow node stays slow for 30 samples then becomes fast.
    let slow = MockUpstream::new("slow");
    for _ in 0..10 {
        slow.push(Step::ok_slow(200, 100));
    }
    p.add_backend(Backend::new(
        "slow",
        1,
        Box::new(slow),
        &h,
        BreakerConfig::default(),
    ));
    let slow_idx = 3;

    let mut saw_ejection = false;
    for step in 0..120u64 {
        let now = step * 100;
        p.poll(now);
        if p.backend(slow_idx).is_ejected(now) {
            saw_ejection = true;
            if let Some(idx) = p.select(now, None, &[]) {
                assert_ne!(
                    idx, slow_idx,
                    "EWMA ejected node was selected at t={now}"
                );
            }
            continue;
        }
        ewma_drive(&mut p, now);
    }
    assert!(saw_ejection, "the slow node was never ejected");
    // It recovered: exactly one ejection, ramp back to full, serving again.
    assert_eq!(p.backend(slow_idx).ewma_ejections(), 1);
    assert_eq!(p.backend(slow_idx).ramp_percent(), 100);
    let final_now = 120 * 100;
    let idx = p.select(final_now, None, &[]).expect("capacity");
    assert!(p.backend(idx).is_available(final_now));
}

#[test]
fn gate6_ewma_no_ejection_within_tolerance() {
    let cfg = ewma_cfg();
    let h = HealthConfig::default();

    // Identical latencies: nobody is an outlier.
    let mut p = Pool::new("svc", Strategy::Weighted, h);
    p.enable_ewma(cfg);
    for n in ["a", "b", "c", "d"] {
        p.add_backend(Backend::new(
            n,
            1,
            Box::new(MockUpstream::healthy(n)),
            &h,
            BreakerConfig::default(),
        ));
    }
    for step in 0..120u64 {
        let now: u64 = step * 100;
        ewma_drive(&mut p, now);
    }
    for b in p.backends() {
        assert_eq!(b.ewma_ejections(), 0, "node {} ejected without cause", b.name);
    }

    // Moderate spread stays under the 3x line: 10 vs a 5.25 mean is 1.9x.
    let mut p2 = Pool::new("svc2", Strategy::Weighted, h);
    p2.enable_ewma(cfg);
    for (i, latency) in [5u64, 5, 6, 10].into_iter().enumerate() {
        let up = MockUpstream::new(format!("n{i}")).with_default(Step::ok_slow(200, latency));
        p2.add_backend(Backend::new(
            format!("n{i}"),
            1,
            Box::new(up),
            &h,
            BreakerConfig::default(),
        ));
    }
    for step in 0..120u64 {
        let now2: u64 = step * 100;
        ewma_drive(&mut p2, now2);
    }
    for b in p2.backends() {
        assert_eq!(b.ewma_ejections(), 0, "node {} ejected within tolerance", b.name);
    }
}

#[test]
fn gate6_ewma_reinstates_with_partial_weight() {
    let cfg = ewma_cfg();
    let h = HealthConfig::default();
    let mut p = Pool::new("svc", Strategy::Weighted, h);
    p.enable_ewma(cfg);
    for n in ["f0", "f1", "f2"] {
        p.add_backend(Backend::new(
            n,
            1,
            Box::new(MockUpstream::healthy(n)),
            &h,
            BreakerConfig::default(),
        ));
    }
    // Slow node: slow for 30 samples then permanently fast.
    let slow = MockUpstream::new("slow");
    for _ in 0..30 {
        slow.push(Step::ok_slow(200, 100));
    }
    p.add_backend(Backend::new(
        "slow",
        1,
        Box::new(slow),
        &h,
        BreakerConfig::default(),
    ));
    let slow_idx = 3;

    let mut reinstated_at = None;
    for step in 0..60u64 {
        let now = step * 100;
        p.poll(now);
        if p.backend(slow_idx).ewma_ejections() >= 1 && !p.backend(slow_idx).is_ejected(now) {
            reinstated_at = Some(now);
            break;
        }
        ewma_drive(&mut p, now);
    }
    let t0 = reinstated_at.expect("node must be ejected and reinstated");
    assert_eq!(p.backend(slow_idx).ramp_percent(), 25, "reinstatement starts at 25 percent");

    // While ramped down the node earns a fraction of its weight: scores are
    // 25, 100, 100, 100, so its share is about 7.7 percent, not 25.
    let mut counts = [0u64; 4];
    let picks = 9_750u64; // 30 full cycles of the smooth WRR period
    for _ in 0..picks {
        p.poll(t0);
        let idx = p.select(t0, None, &[]).expect("capacity");
        assert!(p.backend(idx).is_available(t0));
        counts[idx] += 1;
    }
    let slow_share = counts[slow_idx] as f64 / picks as f64;
    let expected = 25.0 / 325.0;
    assert!(
        (slow_share - expected).abs() < 0.01,
        "ramped share {slow_share:.4} not near {expected:.4}"
    );
    assert!(slow_share < 0.20, "ramped node received near full traffic");
    assert_eq!(p.backend(slow_idx).ramp_percent(), 25, "frozen clock must hold the ramp");

    // Two clean ramp windows later the node is back at full weight and its
    // share matches the others.
    p.poll(t0 + 1_000);
    assert_eq!(p.backend(slow_idx).ramp_percent(), 50);
    let t1 = t0 + 2_000;
    p.poll(t1);
    assert_eq!(p.backend(slow_idx).ramp_percent(), 100);
    let mut counts2 = [0u64; 4];
    for _ in 0..4_000u64 {
        p.poll(t1);
        let idx = p.select(t1, None, &[]).expect("capacity");
        assert!(p.backend(idx).is_available(t1));
        counts2[idx] += 1;
    }
    for (i, c) in counts2.iter().enumerate() {
        let share = *c as f64 / 4_000.0;
        assert!(
            (share - 0.25).abs() < 0.01,
            "node {i} share {share:.4} not near 0.25 after full ramp"
        );
    }
    // The distribution report agrees with the picks.
    let rows = p.distribution(t1);
    assert_eq!(rows.len(), 4);
    for row in &rows {
        assert!(!row.ejected);
        assert!((row.expected_share_pct - 25.0).abs() < 0.01);
    }
}

#[test]
fn gate6_ewma_scaled_fuzz_invariants_hold() {
    let mut rng = Rng::new(seed() ^ 0x0E77_A000);
    let n = ops().max(3_000);
    let cfg = EwmaConfig {
        alpha_milli: 500,
        factor_milli: 3_000,
        window_ms: 500,
        min_samples: 4,
        ejection_ms: 700,
        reinstate_percent: 25,
        ramp_window_ms: 500,
    };
    let h = HealthConfig::default();
    let mut p = Pool::new("svc", Strategy::Weighted, h);
    p.enable_ewma(cfg);
    let mut slow_class = [false; 40];
    slow_class.iter_mut().enumerate().for_each(|(i, is_slow)| {
        // Every fourth node is slow (50 ms), the rest fast (5 ms).
        *is_slow = i % 4 == 3;
        let latency = if *is_slow { 50 } else { 5 };
        let up = MockUpstream::new(format!("n{i}")).with_default(Step::ok_slow(200, latency));
        p.add_backend(Backend::new(
            format!("n{i}"),
            1,
            Box::new(up),
            &h,
            BreakerConfig::default(),
        ));
    });

    let mut now: u64 = 0;
    let mut ejections_observed = 0u64;
    for _ in 0..n {
        now += rng.below(120);
        p.poll(now);
        let ejected_before: Vec<bool> =
            p.backends().iter().map(|b| b.is_ejected(now)).collect();
        if let Some(idx) = p.select(now, None, &[]) {
            assert!(
                p.backend(idx).is_available(now),
                "selected unavailable node {idx} at t={now}"
            );
            p.begin(idx);
            let reply = p.dispatch(idx, &Request::get("/"));
            p.record_success_latency(idx, now, reply.latency_ms);
            p.end(idx);
            assert!(!ejected_before[idx], "ejected node was selected at t={now}");
        }
        ejections_observed += p
            .backends()
            .iter()
            .map(echofront::Backend::ewma_ejections)
            .sum::<u64>();
        let _ = &ejected_before;
    }
    // Slow nodes got ejected, fast nodes never did.
    let mut slow_ejections = 0u64;
    for (i, b) in p.backends().iter().enumerate() {
        if slow_class[i] {
            slow_ejections += b.ewma_ejections();
        } else {
            assert_eq!(
                b.ewma_ejections(),
                0,
                "fast node {} was ejected as a latency outlier",
                b.name
            );
        }
    }
    assert!(slow_ejections > 0, "no slow node was ever ejected");
    println!(
        "[gate6] ops={n} slow_ejections_total={slow_ejections} ewma_events_sum={ejections_observed}"
    );
}
