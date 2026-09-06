//! Echofront is a resilience focused layer 7 reverse proxy and load balancer.
//!
//! It is built from the standard library only. The whole engine runs over an
//! injected [`Clock`](clock::Clock) and talks to backends only through the
//! [`Upstream`](upstream::Upstream) trait, so every timing and failure behavior
//! is deterministic and testable without real sockets.
//!
//! The moving parts:
//!
//! - [`http`]: a small HTTP/1.1 request and response model plus a parser.
//! - [`router`]: match a request to a service by host and path prefix.
//! - [`pool`]: an upstream pool with round robin, weighted, least connections and
//!   consistent hash strategies, active health checks, passive outlier ejection,
//!   and a per upstream circuit breaker.
//! - [`ewma`]: EWMA latency scoring with sustained window outlier ejection and
//!   gradual reinstatement through partial traffic weight.
//! - [`breaker`]: the circuit breaker state machine.
//! - [`retry`]: a retry budget that caps retry amplification.
//! - [`proxy`]: the orchestrator that routes, selects, sends, retries and fails
//!   over.
//!
//! The core invariant is that the proxy never selects an unhealthy upstream, an
//! ejected outlier, or one whose circuit is open.

pub mod breaker;
pub mod clock;
pub mod ewma;
pub mod hashring;
pub mod http;
pub mod pool;
pub mod proxy;
pub mod retry;
pub mod rng;
pub mod router;
pub mod upstream;

pub use breaker::{BreakerConfig, BreakerState, CircuitBreaker};
pub use clock::{Clock, ManualClock, SystemClock};
pub use ewma::{DistributionRow, EwmaConfig};
pub use hashring::ConsistentHashRing;
pub use http::{Method, Request, Response};
pub use pool::{Backend, HealthConfig, Pool, Strategy};
pub use proxy::{HandleResult, Proxy, ProxyError};
pub use retry::{RetryBudget, RetryConfig};
pub use rng::Rng;
pub use router::{Route, Router};
pub use upstream::{MockUpstream, Step, Upstream, UpstreamError, UpstreamReply};
