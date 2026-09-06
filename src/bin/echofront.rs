//! Echofront CLI. Runs a scripted, fully deterministic scenario over a manual
//! clock and prints routing decisions, health, and circuit breaker state. No real
//! sockets are involved. Use `demo` for the guided walkthrough or `spread` for a
//! load balancing distribution report.

use std::rc::Rc;

use echofront::breaker::BreakerConfig;
use echofront::clock::{Clock, ManualClock};
use echofront::http::Request;
use echofront::pool::{Backend, HealthConfig, Pool, Strategy};
use echofront::proxy::Proxy;
use echofront::retry::RetryConfig;
use echofront::router::{Route, Router};
use echofront::upstream::{MockUpstream, Step, UpstreamError};

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "demo".to_string());
    match mode.as_str() {
        "demo" => demo(),
        "spread" => {
            let strat = std::env::args()
                .nth(2)
                .and_then(|s| Strategy::parse(&s))
                .unwrap_or(Strategy::Weighted);
            spread(strat);
        }
        "help" | "-h" | "--help" => usage(),
        other => {
            eprintln!("unknown mode: {other}\n");
            usage();
        }
    }
}

fn usage() {
    println!("echofront <demo|spread [strategy]|help>");
    println!("  demo               resilience walkthrough: LB spread, ejection, breaker, failover, recovery");
    println!("  spread <strategy>  distribution report for round-robin|weighted|least-conn|sticky");
}

fn health_cfg() -> HealthConfig {
    HealthConfig {
        probe_interval_ms: 1_000,
        unhealthy_probe_threshold: 2,
        healthy_probe_threshold: 2,
        // Eject an outlier after 3 consecutive failures, before the breaker trips.
        outlier_consecutive_failures: 3,
        ejection_base_ms: 2_500,
        start_healthy: true,
    }
}

fn breaker_cfg() -> BreakerConfig {
    BreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        cooldown_ms: 3_000,
        half_open_max_calls: 2,
    }
}

fn print_pool_state(proxy: &Proxy, service: &str) {
    let pool = proxy.pool(service).unwrap();
    println!(
        "  {:<6} {:>8} {:>7} {:>9} {:>10} {:>8}",
        "node", "healthy", "in_fl", "breaker", "ejected", "served"
    );
    for b in pool.backends() {
        let now = proxy.now();
        let breaker = format!("{:?}", b.breaker_state());
        let ejected = if b.is_ejected(now) { "yes" } else { "no" };
        println!(
            "  {:<6} {:>8} {:>7} {:>9} {:>10} {:>8}",
            b.name,
            b.is_healthy(),
            b.in_flight,
            breaker,
            ejected,
            b.served()
        );
    }
}

fn send(proxy: &mut Proxy, path: &str, session: Option<&str>) {
    let mut req = Request::get(path).with_header("Host", "shop.example.com");
    if let Some(s) = session {
        req = req.with_header("X-Session-Id", s);
    }
    let res = proxy.handle(&req);
    let trail: Vec<String> = res
        .attempts
        .iter()
        .map(|a| {
            let tag = if a.is_retry { "retry->" } else { "" };
            let mark = if a.ok { "ok" } else { "FAIL" };
            format!("{tag}{}({} {})", a.backend_name, a.detail, mark)
        })
        .collect();
    println!(
        "  {} {} -> {}  [{}]",
        res.status(),
        path,
        res.service.as_deref().unwrap_or("no-route"),
        trail.join(" ")
    );
}

fn build_proxy(strategy: Strategy, backends: &[(&str, u32)]) -> (Proxy, Rc<ManualClock>) {
    let clock = Rc::new(ManualClock::new(0));
    let health = health_cfg();
    let mut pool = Pool::new("shop", strategy, health);
    for (name, weight) in backends {
        let up = MockUpstream::healthy(*name);
        pool.add_backend(Backend::new(*name, *weight, Box::new(up), &health, breaker_cfg()));
    }
    let mut router = Router::new();
    router.add(Route::new(Some("shop.example.com"), "/", "shop"));
    let dyn_clock: Rc<dyn Clock> = clock.clone();
    let mut proxy = Proxy::new(dyn_clock, router, RetryConfig::default());
    proxy.add_pool(pool);
    (proxy, clock)
}

fn demo() {
    println!("Echofront demo: a proxy in front of pool 'shop' with nodes A, B, C\n");

    // Build with a scripted C that will start failing on demand.
    let clock = Rc::new(ManualClock::new(0));
    let health = health_cfg();
    let mut pool = Pool::new("shop", Strategy::RoundRobin, health);
    pool.add_backend(Backend::new(
        "A",
        1,
        Box::new(MockUpstream::healthy("A")),
        &health,
        breaker_cfg(),
    ));
    pool.add_backend(Backend::new(
        "B",
        1,
        Box::new(MockUpstream::healthy("B")),
        &health,
        breaker_cfg(),
    ));
    let c = MockUpstream::new("C");
    // C answers fine 3 times, then fails 5 times for the incident, then its
    // default step (ok 200) makes it healthy again.
    c.push_many([Step::ok(200), Step::ok(200), Step::ok(200)]);
    c.push_many(std::iter::repeat_with(|| Step::fail(UpstreamError::ServerError(503))).take(5));
    pool.add_backend(Backend::new("C", 1, Box::new(c), &health, breaker_cfg()));
    let mut router = Router::new();
    router.add(Route::new(Some("shop.example.com"), "/", "shop"));
    let dyn_clock: Rc<dyn Clock> = clock.clone();
    let mut proxy = Proxy::new(dyn_clock, router, RetryConfig::default());
    proxy.add_pool(pool);

    println!("1) Round robin spreads requests evenly across A, B, C:");
    for i in 0..6 {
        clock.advance(50);
        send(&mut proxy, &format!("/catalog?p={i}"), None);
    }
    print_pool_state(&proxy, "shop");

    println!("\n2) C starts failing. After 3 consecutive failures it is ejected as an outlier and traffic fails over to A and B (note C ejected=yes, breaker still Closed):");
    for i in 0..15 {
        clock.advance(50);
        send(&mut proxy, &format!("/checkout?o={i}"), None);
    }
    print_pool_state(&proxy, "shop");

    println!("\n3) The ejection cooldown passes, so C is reinstated and gets traffic again. It is still failing, so after more failures its circuit breaker trips to Open (note breaker=Open):");
    clock.advance(2_600);
    for i in 0..15 {
        clock.advance(50);
        send(&mut proxy, &format!("/cart?i={i}"), None);
    }
    print_pool_state(&proxy, "shop");

    println!("\n4) After the breaker cooldown it goes Half-Open, lets a trial through, C has recovered, and two successes close the breaker (note breaker back to Closed):");
    clock.advance(3_100);
    for i in 0..15 {
        clock.advance(50);
        send(&mut proxy, &format!("/catalog?r={i}"), None);
    }
    print_pool_state(&proxy, "shop");

    println!(
        "\nTotals: requests={} retries={} failovers={} retry_tokens_left={:.1}",
        proxy.total_requests(),
        proxy.total_retries(),
        proxy.total_failovers(),
        proxy.budget().tokens()
    );

    println!("\n5) Sticky sessions: consistent-hash pins a session to one node and keeps it there:");
    let (mut sticky, _c2) = build_proxy(Strategy::ConsistentHash, &[("A", 1), ("B", 1), ("C", 1), ("D", 1)]);
    for user in ["alice", "bob", "carol", "dave", "erin", "frank"] {
        let mut seen = String::new();
        for _ in 0..3 {
            let req = Request::get("/account")
                .with_header("Host", "shop.example.com")
                .with_header("X-Session-Id", user);
            let res = sticky.handle(&req);
            seen = res
                .selected
                .map(|i| sticky.pool("shop").unwrap().backend(i).name.clone())
                .unwrap_or_default();
        }
        println!("  session {user:<6} -> node {seen}");
    }
}

fn spread(strategy: Strategy) {
    let n = 6000usize;
    let (mut proxy, _clock) = build_proxy(strategy, &[("A", 1), ("B", 1), ("C", 3), ("D", 1)]);
    println!("strategy={} requests={n}", strategy.as_str());
    for i in 0..n {
        let session = if strategy == Strategy::ConsistentHash {
            Some(format!("user-{}", i % 50))
        } else {
            None
        };
        let mut req = Request::get("/x").with_header("Host", "shop.example.com");
        if let Some(s) = &session {
            req = req.with_header("X-Session-Id", s);
        }
        proxy.handle(&req);
    }
    let pool = proxy.pool("shop").unwrap();
    let total: u64 = pool.backends().iter().map(|b| b.served()).sum();
    for b in pool.backends() {
        let pct = 100.0 * b.served() as f64 / total as f64;
        let bar = "#".repeat((pct / 2.0) as usize);
        println!(
            "  {:<3} weight={} served={:>5} {:>5.1}%  {}",
            b.name, b.weight, b.served(), pct, bar
        );
    }
}
