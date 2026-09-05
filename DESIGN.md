# Echofront design

This document explains how Echofront is built and why each correctness gate proves what it claims. The guiding idea is that resilience logic is timing dependent and failure dependent, and that both of those inputs are made explicit and injectable so the logic can be tested deterministically.

## Architecture

A request flows through four layers.

1. The router matches the request to a service by optional host and by longest matching path prefix. It answers one question, which pool handles this request.
2. The pool for that service holds the backends, their live health and circuit state, and the load balancing strategy. It answers one question, which healthy backend should take this request.
3. The proxy is the orchestrator. It routes, selects, sends, and on failure retries against a different backend within a retry budget. It feeds every outcome back into the pool so health and circuit state evolve.
4. The upstream is the backend itself, reached only through a trait. In production this would be a socket client. In tests it is a scripted mock, so no real network is involved anywhere in the suite.

Two abstractions make the whole thing deterministic. The `Clock` trait is the only source of time, and the `Upstream` trait is the only way to reach a backend. Nothing in the engine calls the wall clock or opens a socket directly.

## Load balancing strategies

All four strategies select only from backends that are available at that instant, meaning healthy, not ejected, and not on an open circuit.

- Round robin keeps a cursor over the available backends in stable order and advances it by one each pick. Over a stable healthy set this visits every backend exactly once per cycle.
- Weighted round robin uses the smooth weighted algorithm. Each backend carries a current weight that increases by its configured weight on every pick. The backend with the highest current weight is chosen and then has the sum of all weights subtracted from its current weight. Over one full period of `total_weight` picks this distributes requests in exact proportion to the weights, and it spreads them smoothly rather than in bursts.
- Least connections tracks an in flight counter per backend that the proxy increments when a request begins and decrements when it ends. Selection picks the backend with the fewest in flight requests, breaking ties by index. This favors idle backends and adapts to uneven request durations.
- Consistent hash places each backend on a hash ring at many virtual node positions. A request key is hashed onto the ring and served by the next backend clockwise. The same key always lands on the same backend, and when a backend is added or removed only the keys that fall in its arcs move, which is about one part in N of all keys.

The ring uses FNV-1a to seed positions and a SplitMix64 finalizer to place both the virtual nodes and the lookup keys. The finalizer matters. Raw FNV-1a clusters keys that share a long common prefix, such as many session identifiers of the form user-1, user-2 and so on, which would starve some backends of traffic. Mixing the key hash the same way the ring points are mixed restores an even spread.

## Health and outlier ejection

Echofront runs three independent resilience mechanisms, and a backend is routable only when all three agree.

- Active health checks probe each backend on the injected clock at a configured interval. A run of failed probes past the unhealthy threshold marks the backend unhealthy, and a later run of good probes past the healthy threshold marks it healthy again. This is the proactive signal that catches a backend that is down even when no live traffic has hit it yet.
- Passive outlier ejection watches real traffic results. After a run of consecutive failures past the outlier threshold the backend is ejected for a cooldown, during which it receives no traffic. When the cooldown elapses it is reinstated and its failure counter resets. This is the reactive signal that pulls a bad backend out fast based on what real requests are seeing.
- The circuit breaker, described next, is the third gate and protects a backend from being hammered while it is clearly failing.

Keeping these separate is deliberate. Active health, passive ejection and circuit breaking answer different questions and fire on different evidence, and a real incident often trips them in sequence. The demo walks through exactly that sequence.

## The circuit breaker state machine

Each backend has its own breaker with three states.

- Closed is the normal state. Consecutive failures are counted, and a success resets the count. When the count reaches the failure threshold the breaker trips to Open.
- Open means no traffic. The breaker records when it opened, and once the cooldown has elapsed it moves to Half-Open. While it is Open the backend is not callable, so the load balancer never selects it.
- Half-Open is a trial. It allows requests through, and a run of successes past the success threshold closes the breaker. A single failure in Half-Open sends it straight back to Open with a fresh cooldown.

All timing goes through the clock. A `poll` step promotes Open to Half-Open when the cooldown has elapsed, and it is called before any state or callability is read so the answer is always current for the given time.

## Retries, budget and failover

When a request fails the proxy retries against a different backend that is still available, which is the failover. Retries are gated by a retry budget so that an incident does not turn into a retry storm. The budget is a token bucket measured in integer milli tokens for full determinism. Each new top level request deposits a small fraction of a token, and each retry costs a whole token. The bucket starts full, which lets an isolated failing request fail over immediately and allows a short burst of retries, then it throttles the sustained retry rate toward the deposit ratio. When the bucket is empty, retries are denied and the caller gets the last error.

## The injected clock and determinism

The `ManualClock` is advanced by hand, so a test lays out an exact timeline of events and time steps. Because every breaker cooldown, every ejection window and every health probe interval reads from that clock, the same script produces the same result on every run and every platform. The mock upstream is scripted the same way, with a queue of outcomes and a default that repeats once the queue is drained, plus its own queue of probe results. Randomized gates use a small SplitMix64 generator seeded from an environment variable, so a failing run is reproducible by rerunning with the same seed.

## Why each gate proves its claim

- Gate 1 proves load balancing correctness by construction. For round robin it asserts that every window the size of the pool is a permutation of all backends, which is the exact cycle property. For weighted it runs thousands of selections and checks each backend's share against its weight fraction within a tight tolerance. For consistent hash it maps thousands of keys, removes a backend, and asserts that only keys that were on the removed backend moved and that the moved fraction is near one over N, then adds a backend and asserts that keys only move onto the new one. Those are precisely the properties that make the hash useful for sticky sessions.
- Gate 2 proves the breaker by differential testing. An independent reference model is written to the same transition spec, and a long random sequence of success, failure and time events is applied to both. The gate asserts the two always agree on state and callability and that an Open breaker is never callable. If the real breaker ever diverged from the spec, the models would disagree and the gate would fail with the exact event and time.
- Gate 3 proves health over the clock by running the same probe script twice and asserting the ejection and reinstatement times are identical, then asserting the backend is ejected within the unhealthy window and reinstated only after it recovers. Determinism is the claim, so the gate checks determinism directly.
- Gate 4 proves the core invariant directly. Across a randomized workload of routing decisions and outcomes and time steps, it asserts after every single selection that the chosen backend was available and that its breaker was not Open. The invariant is that the proxy never routes to a bad backend, and the gate checks it at the exact moment a decision is made, for every decision.
