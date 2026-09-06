//! Max scale stress harness. Every scenario is env scaled: ECHOFRONT_STRESS_OPS
//! sets the workload size (small default so CI stays fast) and ECHOFRONT_STRESS_NODES
//! sets the pool size. Same seed gives the same timeline every run. The scenarios
//! drive the engine through hundreds of thousands to millions of requests with
//! flapping nodes, breaker churn, nested retries, sticky churn, and clock jumps,
//! checking the core invariants after every step.
//!
//! Invariants checked continuously:
//! 1. A selection is never an unhealthy, ejected, or open circuit backend.
//! 2. Least connections accounting: in_flight returns to zero after paired
//!    begin/end and never goes negative.
//! 3. The breaker never diverges from an independent reference model.
//! 4. Retries granted never exceed the token bucket arithmetic bound.
//! 5. A continuous ejection counts as exactly one ejection.
//! 6. Weighted spread matches weights exactly over full smooth WRR cycles,
//!    including odd weights like 1 vs 100, and zero weight is never selected.

use std::collections::HashMap;
use std::rc::Rc;

use echofront::breaker::{BreakerConfig, BreakerState};
use echofront::clock::{Clock, ManualClock};
use echofront::http::Request;
use echofront::pool::{Backend, HealthConfig, Pool, Strategy};
use echofront::proxy::Proxy;
use echofront::retry::RetryConfig;
use echofront::rng::Rng;
use echofront::router::{Route, Router};
use echofront::upstream::{MockUpstream, Step, UpstreamError};

fn stress_ops() -> u64 {
    std::env::var("ECHOFRONT_STRESS_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000)
}

fn stress_nodes() -> usize {
    std::env::var("ECHOFRONT_STRESS_NODES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12)
}

fn seed() -> u64 {
    std::env::var("ECHOFRONT_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5757_E55E_D011_7A05)
}

/// Independent reference model of the breaker, written to the same spec as the
/// one in gates.rs but kept local so the stress binary proves it on its own.
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
    fn matches(&self, real: BreakerState) -> bool {
        matches!(
            (self.state, real),
            (RefState::Closed(_), BreakerState::Closed)
                | (RefState::Open(_), BreakerState::Open)
                | (RefState::Half(_), BreakerState::HalfOpen)
        )
    }
}

fn chaos_breaker_cfg() -> BreakerConfig {
    BreakerConfig {
        failure_threshold: 4,
        success_threshold: 2,
        cooldown_ms: 700,
        half_open_max_calls: 2,
    }
}

fn chaos_health_cfg() -> HealthConfig {
    HealthConfig {
        probe_interval_ms: 250,
        unhealthy_probe_threshold: 2,
        healthy_probe_threshold: 2,
        outlier_consecutive_failures: 3,
        ejection_base_ms: 900,
        start_healthy: true,
    }
}

fn progress(label: &str, done: u64, total: u64) {
    if total == 0 || done.is_multiple_of((total / 10).max(1)) {
        println!("[{label}] {done}/{total}");
    }
}

fn scripted_upstream(name: &str, len: usize, node_seed: u64) -> MockUpstream {
    let up = MockUpstream::new(name);
    let mut nr = Rng::new(node_seed);
    for _ in 0..len {
        let step = match nr.below(10) {
            0..=2 => Step::fail(UpstreamError::Timeout),
            3 => Step::ok_slow(200, 90),
            4 => Step::ok(503),
            _ => Step::ok(200),
        };
        up.push(step);
    }
    up
}

/// Scenario 1: mixed chaos through the proxy on a large pool. Nodes flap,
/// breakers open and close repeatedly, retries nest, sticky keys churn, the
/// clock occasionally jumps. Availability, breaker reference, least conn
/// accounting, ejection accounting, and the retry bound are checked continuously.
#[test]
fn stress_mixed_chaos_large_pool() {
    let n_reqs = stress_ops();
    let n_nodes = stress_nodes().max(1);
    let mut rng = Rng::new(seed());
    let clock = Rc::new(ManualClock::new(0));
    let health = chaos_health_cfg();

    let strategies = [
        Strategy::RoundRobin,
        Strategy::Weighted,
        Strategy::LeastConnections,
        Strategy::ConsistentHash,
    ];

    for strategy in strategies {
        let mut pool = Pool::new("svc", strategy, health);
        // Enough scripted steps per node for the whole run at any scale, kept
        // bounded so memory stays small even with hundreds of nodes. When a
        // script runs dry the node simply turns healthy.
        let script_len = ((n_reqs as usize * 8) / n_nodes).clamp(1_024, 32_768);
        for i in 0..n_nodes {
            let node_seed = seed() ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let up = scripted_upstream(&format!("n{i}"), script_len, node_seed);
            let weight = match i % 5 {
                0 => 1,
                1 => 100,
                2 => 7,
                3 => 3,
                _ => 1,
            };
            pool.add_backend(Backend::new(
                format!("n{i}"),
                weight,
                Box::new(up),
                &health,
                chaos_breaker_cfg(),
            ));
        }

        let mut router = Router::new();
        router.add(Route::new(None, "/", "svc"));
        let retry_cfg = RetryConfig::default();
        let mut proxy = Proxy::new(clock.clone(), router, retry_cfg);
        proxy.add_pool(pool);

        let mut shadows: Vec<RefBreaker> = (0..n_nodes)
            .map(|_| RefBreaker::new(chaos_breaker_cfg()))
            .collect();
        let mut cont_ejected = vec![false; n_nodes];
        let mut sticky_pinned: HashMap<String, (Vec<bool>, Option<usize>)> = HashMap::new();
        let mut total_retries_seen = 0u64;
        let mut max_attempts_seen = 0u32;

        for r in 0..n_reqs {
            progress("chaos", r, n_reqs);
            // Occasional big clock jump, mostly small steps.
            if rng.below(500) == 0 {
                clock.advance(5_000_000_000);
            } else {
                clock.advance(rng.below(120));
            }
            let now = clock.now_millis();
            proxy.pool_mut("svc").unwrap().run_health_checks(now);

            let avail_before: Vec<bool> = {
                let pool = proxy.pool("svc").unwrap();
                pool.backends().iter().map(|b| b.is_available(now)).collect()
            };
            let ejections_before: Vec<u64> = {
                let pool = proxy.pool("svc").unwrap();
                pool.backends().iter().map(|b| b.ejections()).collect()
            };

            let session = if strategy == Strategy::ConsistentHash {
                Some(format!("user-{}", rng.below(4096)))
            } else {
                None
            };
            let mut req = Request::get(format!("/r{r}")).with_header("Host", "h");
            if let Some(s) = &session {
                req = req.with_header("X-Session-Id", s);
            }
            let res = proxy.handle(&req);
            let now2 = clock.now_millis();

            // Invariant 1: every attempted backend was available before the call.
            for a in &res.attempts {
                assert!(
                    avail_before[a.backend],
                    "attempted unavailable backend {} at t={now}",
                    a.backend
                );
            }
            max_attempts_seen = max_attempts_seen.max(res.attempts.len() as u32);

            // Feed the shadow breakers exactly what the proxy fed the real ones
            // before comparing, so both sides reflect this request.
            for a in &res.attempts {
                if a.ok {
                    shadows[a.backend].on_success(now2);
                } else {
                    shadows[a.backend].on_failure(now2);
                }
            }

            {
                let pool = proxy.pool("svc").unwrap();
                // Invariant 2: least conn accounting returns to zero.
                for b in pool.backends() {
                    assert_eq!(b.in_flight, 0, "in_flight stuck on {} at t={now}", b.name);
                }
                // Invariant 3: breaker matches the reference model.
                for (i, b) in pool.backends().iter().enumerate() {
                    shadows[i].poll(now2);
                    assert!(
                        shadows[i].matches(b.breaker_state()),
                        "breaker diverged on node {i}: ref={:?} real={:?} at t={now2}",
                        shadows[i].state,
                        b.breaker_state()
                    );
                    if b.breaker_state() == BreakerState::Open {
                        assert!(
                            !b.is_available(now2),
                            "open breaker backend selectable at t={now2}"
                        );
                    }
                }
                // Invariant 5: a continuously ejected node counts at most one
                // ejection, and a counter bump requires a fresh failure on it.
                for (i, b) in pool.backends().iter().enumerate() {
                    let ej = b.ejections();
                    let ejected_now = b.is_ejected(now2);
                    let delta = ej.saturating_sub(ejections_before[i]);
                    if delta > 0 {
                        assert!(
                            ejected_now,
                            "ejection counter moved while not ejected, node {i}"
                        );
                        if cont_ejected[i] {
                            assert!(
                                res.attempts.iter().any(|a| a.backend == i),
                                "ejection double count on node {i} with no fresh failure"
                            );
                        }
                    }
                    cont_ejected[i] = ejected_now;
                }
            }

            // Sticky determinism: with the same key and an unchanged available
            // set at selection time, the first pick must be identical. When the
            // set changes, the ring is allowed to remap.
            if let Some(s) = &session {
                if let Some(prev) = sticky_pinned.get(s) {
                    if prev.0 == avail_before {
                        assert_eq!(
                            prev.1, res.selected,
                            "sticky pick drifted with unchanged available set at t={now}"
                        );
                    }
                }
                sticky_pinned.insert(s.clone(), (avail_before.clone(), res.selected));
            }

            // Invariant 4: retry token arithmetic bound.
            let retried = res.attempts.iter().filter(|a| a.is_retry).count() as u64;
            total_retries_seen += retried;
            let reqs = r + 1;
            let bound = (retry_cfg.max_milli + reqs as i64 * retry_cfg.deposit_milli)
                / retry_cfg.retry_cost_milli;
            assert!(
                (total_retries_seen as i64) <= bound,
                "retry amplification exceeded budget bound: {total_retries_seen} > {bound}"
            );
            assert!(
                max_attempts_seen <= retry_cfg.max_retries_per_request + 1,
                "attempts per request exceeded configured maximum"
            );
        }

        let pool = proxy.pool("svc").unwrap();
        let total: u64 = pool.backends().iter().map(|b| b.served()).sum();
        println!(
            "[chaos] strategy={} requests={n_reqs} nodes={n_nodes} attempts_served={total} \
             retries={total_retries_seen} max_attempts={max_attempts_seen}",
            strategy.as_str()
        );
    }
}

/// Scenario 2: a pool of one node. Breaker open means total outage, select
/// returns None, and the node comes back after cooldown. Stretched over a long
/// flapping timeline with big clock jumps.
#[test]
fn stress_single_node_total_outage() {
    let n = stress_ops();
    let mut rng = Rng::new(seed() ^ 0xBEEF);
    let health = chaos_health_cfg();
    let mut p = Pool::new("solo", Strategy::RoundRobin, health);
    let up = MockUpstream::new("only");
    // Long failure runs alternating with long recovery runs.
    let run_len = (n as usize).min(50_000);
    for _ in 0..4 {
        for _ in 0..run_len {
            up.push(Step::fail(UpstreamError::Connection));
        }
        for _ in 0..run_len {
            up.push(Step::ok(200));
        }
    }
    p.add_backend(Backend::new(
        "only",
        1,
        Box::new(up),
        &health,
        chaos_breaker_cfg(),
    ));

    let mut now: u64 = 0;
    let mut outage_windows = 0u64;
    let mut live_windows = 0u64;
    for r in 0..n {
        progress("solo", r, n);
        now += if rng.below(300) == 0 { 9_999_999 } else { rng.below(50) };
        p.poll(now);
        match p.select(now, None, &[]) {
            Some(idx) => {
                assert!(p.backend(idx).is_available(now));
                p.begin(idx);
                let reply = p.dispatch(idx, &Request::get("/"));
                match reply.outcome {
                    Ok(_) => p.record_success(idx, now),
                    Err(_) => p.record_failure(idx, now),
                }
                p.end(idx);
                live_windows += 1;
            }
            None => {
                outage_windows += 1;
                for b in p.backends() {
                    assert!(
                        !b.is_available(now),
                        "select returned None but a node was available"
                    );
                }
            }
        }
        for b in p.backends() {
            assert_eq!(b.in_flight, 0);
        }
    }
    println!("[solo] ops={n} outage_windows={outage_windows} live_windows={live_windows}");
    assert!(outage_windows > 0, "expected at least one total outage window");
    assert!(live_windows > 0, "expected recovery windows");
}

/// Scenario 3: every node fails, then they recover in a scrambled order. While
/// recovering, only nodes that have actually recovered may be selected.
#[test]
fn stress_all_fail_then_recover_scrambled() {
    let n = stress_ops();
    let n_nodes = stress_nodes().max(2);
    let health = chaos_health_cfg();
    let mut p = Pool::new("svc", Strategy::LeastConnections, health);
    // Failure quota differs per node so recovery order is scrambled. Quotas are
    // small so scripts stay tiny even at 300 nodes.
    for i in 0..n_nodes {
        let quota = 3 * (1 + (i * 7) % n_nodes.max(1));
        let up = MockUpstream::new(format!("n{i}"));
        for _ in 0..quota {
            up.push(Step::fail(UpstreamError::ServerError(500)));
        }
        p.add_backend(Backend::new(
            format!("n{i}"),
            1,
            Box::new(up),
            &health,
            chaos_breaker_cfg(),
        ));
    }

    let mut rng = Rng::new(seed() ^ 0xABCD_1234);
    let mut now: u64 = 0;
    let mut recovered_picks = 0u64;
    let mut failing_picks = 0u64;
    for r in 0..n {
        progress("scramble", r, n);
        now += rng.below(30);
        p.poll(now);
        if let Some(idx) = p.select(now, None, &[]) {
            assert!(
                p.backend(idx).is_available(now),
                "selected unavailable backend at t={now}"
            );
            p.begin(idx);
            let reply = p.dispatch(idx, &Request::get("/"));
            match reply.outcome {
                Ok(_) => {
                    p.record_success(idx, now);
                    recovered_picks += 1;
                }
                Err(_) => {
                    p.record_failure(idx, now);
                    failing_picks += 1;
                }
            }
            p.end(idx);
        }
    }
    let healthy_at_end = p.backends().iter().filter(|b| b.is_healthy()).count();
    println!(
        "[scramble] ops={n} nodes={n_nodes} recovered_picks={recovered_picks} \
         failing_picks={failing_picks} healthy_at_end={healthy_at_end}/{n_nodes}"
    );
    assert_eq!(healthy_at_end, n_nodes, "all nodes must recover by the end");
}

/// Scenario 4: sticky session churn with backends going down and coming back in
/// waves. A sticky key must never land on an unavailable node.
#[test]
fn stress_sticky_churn_join_leave() {
    let n = stress_ops();
    let n_nodes = stress_nodes().max(3);
    let health = chaos_health_cfg();
    let mut p = Pool::new("svc", Strategy::ConsistentHash, health);
    for i in 0..n_nodes {
        let per_node = ((n as usize * 4) / n_nodes).clamp(1_024, 32_768);
        let up = MockUpstream::new(format!("n{i}"));
        for k in 0..per_node {
            // Wave pattern: node i is down during its wave slot.
            let wave = (k / 8) % n_nodes;
            if wave == i % n_nodes {
                up.push(Step::fail(UpstreamError::Connection));
            } else {
                up.push(Step::ok(200));
            }
        }
        p.add_backend(Backend::new(
            format!("n{i}"),
            1,
            Box::new(up),
            &health,
            chaos_breaker_cfg(),
        ));
    }

    let mut rng = Rng::new(seed() ^ 0x571C_0001);
    let mut now: u64 = 0;
    let mut picks = 0u64;
    let mut total_outage_windows = 0u64;
    for r in 0..n {
        progress("sticky", r, n);
        now += rng.below(25);
        p.poll(now);
        let key = format!("session-{}", r % ((n_nodes * 3).max(4) as u64));
        if let Some(idx) = p.select(now, Some(&key), &[]) {
            let b = p.backend(idx);
            assert!(
                b.is_available(now),
                "sticky key {key} landed on unavailable node {} at t={now}",
                b.name
            );
            p.begin(idx);
            let reply = p.dispatch(idx, &Request::get("/"));
            match reply.outcome {
                Ok(_) => p.record_success(idx, now),
                Err(_) => p.record_failure(idx, now),
            }
            p.end(idx);
            picks += 1;
        } else {
            total_outage_windows += 1;
        }
    }
    println!("[sticky] ops={n} picks={picks} total_outage_windows={total_outage_windows}");
    assert!(picks > 0);
}

/// Scenario 5: weighted spread must match weights exactly over full smooth WRR
/// cycles, including the odd 1 vs 100 pair and weights with common divisors.
/// Zero weight is never selected. Stretched over millions of picks.
#[test]
fn stress_weighted_exact_spread_odd_weights() {
    let cycles = (stress_ops() / 226).max(10);
    let weights: [(usize, u32); 5] = [(0, 1), (1, 100), (2, 3), (3, 9), (4, 0)];
    let per_cycle: u64 = weights.iter().map(|(_, w)| u64::from(*w)).sum();
    let health = HealthConfig::default();
    let mut p = Pool::new("svc", Strategy::Weighted, health);
    for (i, w) in weights {
        p.add_backend(Backend::new(
            format!("w{i}"),
            w,
            Box::new(MockUpstream::healthy(format!("w{i}"))),
            &health,
            BreakerConfig::default(),
        ));
    }
    p.poll(0);

    let mut counts = vec![0u64; weights.len()];
    for c in 0..cycles {
        progress("weighted", c, cycles);
        for _ in 0..per_cycle {
            let idx = p
                .select(0, None, &[])
                .expect("pool has positive weight nodes");
            assert_ne!(idx, 4, "zero weight node selected");
            counts[idx] += 1;
        }
        // After each full cycle the counts must match the weights exactly.
        for (i, (_, w)) in weights.iter().enumerate() {
            assert_eq!(
                counts[i],
                (c + 1) * u64::from(*w),
                "weighted drift on node {i} after cycle {c}"
            );
        }
    }
    println!("[weighted] cycles={cycles} per_cycle={per_cycle} counts={counts:?} zero_weight_picks=0");
}
