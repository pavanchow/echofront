# Echofront

Echofront is a resilience focused layer 7 reverse proxy and load balancer built from the Rust standard library only. Zero external dependencies, edition 2021.

Its whole reason to exist is the hard part of fronting a pool of backends: spreading load well, noticing when a backend goes bad, taking it out of rotation, retrying somewhere healthy without amplifying an incident, and letting it back in when it recovers. Every one of those behaviors runs over an injected clock and a backend trait, so the entire system is deterministic and testable without a single real socket.

Live playground: https://pavanchow.github.io/echofront/

## The gap it fills

A hand rolled proxy usually starts as a loop that picks a backend and forwards a request. That is fine until the day one backend starts returning 503s or hanging. Then you need, in a hurry, all of the following working together and correctly:

- Load balancing that actually honors weights and keeps sticky sessions sticky.
- Health checking that ejects a bad backend fast and reinstates it only once it is really back.
- A circuit breaker so a failing backend stops eating requests and gets a controlled trial before returning.
- Retries that fail over to a healthy backend but do not turn a small incident into a retry storm.

The trouble is that this logic is timing dependent and failure dependent, which is exactly the code that is painful to test against real servers and clocks. Echofront's angle is that all timing goes through a `Clock` trait and all backends go through an `Upstream` trait. That makes the resilience logic itself the product, and it makes the correctness of that logic something you can prove with seeded, deterministic tests rather than hope for in production.

Why a person would use it: it is a small, readable, dependency free reference for how these mechanisms fit together, and a base you can extend. Why an AI agent would use it: the behavior is fully deterministic under a seed, the state is introspectable after every decision, and the correctness gates give a clear pass or fail signal, so an agent can modify it and immediately know whether it broke an invariant.

This is deliberately not another routing DSL or auth gateway. The focus is resilience and load balancing.

## Quickstart

```
cargo run --release -- demo
```

The demo runs a scripted incident on a manual clock: round robin spread across A, B and C, then C starts failing and is ejected as an outlier, then it is reinstated and its circuit breaker trips Open, then it recovers through Half-Open back to Closed. It also shows retries failing over to healthy backends and a consistent hash keeping sessions pinned.

Distribution report for any strategy:

```
cargo run --release -- spread weighted
cargo run --release -- spread round-robin
cargo run --release -- spread least-conn
cargo run --release -- spread sticky
```

## Library API

```rust
use std::rc::Rc;
use echofront::{
    Backend, BreakerConfig, Clock, HealthConfig, ManualClock, MockUpstream,
    Pool, Proxy, Request, RetryConfig, Route, Router, Strategy,
};

// A clock the test drives by hand. Swap in SystemClock for a real run.
let manual = Rc::new(ManualClock::new(0));
let clock: Rc<dyn Clock> = manual.clone();

// A pool with two backends behind a round robin strategy.
let health = HealthConfig::default();
let mut pool = Pool::new("web", Strategy::RoundRobin, health);
pool.add_backend(Backend::new("a", 1, Box::new(MockUpstream::healthy("a")), &health, BreakerConfig::default()));
pool.add_backend(Backend::new("b", 1, Box::new(MockUpstream::healthy("b")), &health, BreakerConfig::default()));

// Route everything to it.
let mut router = Router::new();
router.add(Route::new(None, "/", "web"));

let mut proxy = Proxy::new(clock, router, RetryConfig::default());
proxy.add_pool(pool);

let result = proxy.handle(&Request::get("/").with_header("Host", "example.com"));
assert!(result.is_success());

// Advance time by hand to drive health checks and breaker cooldowns.
manual.advance(1_000);
proxy.run_health_checks();
```

Key types:

- `Clock` / `ManualClock` / `SystemClock`: the injected time source.
- `Upstream` / `MockUpstream` / `Step`: the backend trait and its scripted mock.
- `Strategy`: `RoundRobin`, `Weighted`, `LeastConnections`, `ConsistentHash`.
- `Pool` / `Backend` / `HealthConfig`: the upstream pool, its members and health policy.
- `CircuitBreaker` / `BreakerConfig` / `BreakerState`: the per upstream breaker.
- `RetryBudget` / `RetryConfig`: the token bucket that caps retry amplification.
- `Router` / `Route`, `Proxy` / `HandleResult`: routing and the orchestrator.

## The correctness gate

The gates are in `tests/gates.rs`. They are the load bearing proofs of the claims above and are bounded for CI. Set `ECHOFRONT_FUZZ_OPS` to change the workload size and `ECHOFRONT_FUZZ_SEED` to change the seed. The same seed produces the same timeline every run.

1. Load balancing correctness. Round robin visits every healthy upstream in an exact cycle, weighted distribution matches the configured weights within tolerance over many requests, and the consistent hash keeps keys on the same upstream while remapping only about `1/N` of keys when a member is added or removed.
2. Circuit breaker state machine. A property test drives random success, failure and time sequences through the breaker and an independent reference model written to the same spec, asserting the states always agree and that an Open breaker is never callable.
3. Health over the injected clock. A backend that starts failing its probes is ejected within the configured window and reinstated after it recovers, and the timeline is identical for the same script.
4. Core invariant. Across a randomized workload the proxy never selects an unhealthy, ejected, or open circuit upstream, checked after every routing decision.

Run everything:

```
cargo test
cargo clippy --all-targets -- -D warnings
```

## License

MIT.
