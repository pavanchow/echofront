//! The proxy. It routes a request to a service pool, selects a healthy upstream
//! by the pool strategy, sends it, feeds the outcome back into health and circuit
//! state, and on failure retries against a different upstream within a retry
//! budget. Every timing decision flows through the injected clock.

use std::collections::HashMap;
use std::rc::Rc;

use crate::clock::Clock;
use crate::http::{Request, Response};
use crate::pool::Pool;
use crate::retry::{RetryBudget, RetryConfig};
use crate::router::Router;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyError {
    /// No route matched the request.
    NoRoute,
    /// A route matched but no upstream was healthy and callable.
    NoHealthyUpstream,
    /// Every attempt failed, carrying the count of attempts made.
    AllAttemptsFailed(u32),
}

impl std::fmt::Display for ProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyError::NoRoute => write!(f, "no route matched"),
            ProxyError::NoHealthyUpstream => write!(f, "no healthy upstream"),
            ProxyError::AllAttemptsFailed(n) => write!(f, "all {n} attempts failed"),
        }
    }
}

impl ProxyError {
    pub fn status(&self) -> u16 {
        match self {
            ProxyError::NoRoute => 404,
            ProxyError::NoHealthyUpstream => 503,
            ProxyError::AllAttemptsFailed(_) => 502,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Attempt {
    pub backend: usize,
    pub backend_name: String,
    pub ok: bool,
    pub detail: String,
    pub latency_ms: u64,
    pub is_retry: bool,
}

#[derive(Debug)]
pub struct HandleResult {
    pub service: Option<String>,
    pub selected: Option<usize>,
    pub response: Result<Response, ProxyError>,
    pub attempts: Vec<Attempt>,
}

impl HandleResult {
    pub fn status(&self) -> u16 {
        match &self.response {
            Ok(r) => r.status,
            Err(e) => e.status(),
        }
    }

    pub fn is_success(&self) -> bool {
        self.response.is_ok()
    }
}

pub struct Proxy {
    clock: Rc<dyn Clock>,
    router: Router,
    pools: HashMap<String, Pool>,
    budget: RetryBudget,
    sticky_header: String,
    total_requests: u64,
    total_retries: u64,
    total_failovers: u64,
}

impl Proxy {
    pub fn new(clock: Rc<dyn Clock>, router: Router, retry: RetryConfig) -> Self {
        Self {
            clock,
            router,
            pools: HashMap::new(),
            budget: RetryBudget::new(retry),
            sticky_header: "X-Session-Id".to_string(),
            total_requests: 0,
            total_retries: 0,
            total_failovers: 0,
        }
    }

    pub fn set_sticky_header(&mut self, header: impl Into<String>) {
        self.sticky_header = header.into();
    }

    pub fn add_pool(&mut self, pool: Pool) {
        self.pools.insert(pool.name().to_string(), pool);
    }

    pub fn pool(&self, name: &str) -> Option<&Pool> {
        self.pools.get(name)
    }

    pub fn pool_mut(&mut self, name: &str) -> Option<&mut Pool> {
        self.pools.get_mut(name)
    }

    pub fn service_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.pools.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn now(&self) -> u64 {
        self.clock.now_millis()
    }

    pub fn total_requests(&self) -> u64 {
        self.total_requests
    }

    pub fn total_retries(&self) -> u64 {
        self.total_retries
    }

    pub fn total_failovers(&self) -> u64 {
        self.total_failovers
    }

    pub fn budget(&self) -> &RetryBudget {
        &self.budget
    }

    /// Run active health probes across every pool at the current time.
    pub fn run_health_checks(&mut self) {
        let now = self.clock.now_millis();
        for pool in self.pools.values_mut() {
            pool.run_health_checks(now);
        }
    }

    fn sticky_key(&self, req: &Request) -> Option<String> {
        req.header(&self.sticky_header)
            .map(|s| s.to_string())
            .or_else(|| req.host().map(|h| h.to_string()))
    }

    /// Handle one request end to end: route, select, send, retry and failover.
    pub fn handle(&mut self, req: &Request) -> HandleResult {
        let now = self.clock.now_millis();
        self.total_requests += 1;
        self.budget.on_request();

        let service = match self.router.route(req) {
            Some(s) => s.to_string(),
            None => {
                return HandleResult {
                    service: None,
                    selected: None,
                    response: Err(ProxyError::NoRoute),
                    attempts: Vec::new(),
                }
            }
        };

        let key = self.sticky_key(req);
        let max_retries = self.budget.max_retries_per_request();

        let pool = match self.pools.get_mut(&service) {
            Some(p) => p,
            None => {
                return HandleResult {
                    service: Some(service),
                    selected: None,
                    response: Err(ProxyError::NoRoute),
                    attempts: Vec::new(),
                }
            }
        };
        pool.poll(now);

        let mut attempts: Vec<Attempt> = Vec::new();
        let mut tried: Vec<usize> = Vec::new();
        let mut first_selected: Option<usize> = None;
        let mut attempt_no: u32 = 0;

        loop {
            let idx = match pool.select(now, key.as_deref(), &tried) {
                Some(i) => i,
                None => {
                    let err = if attempts.is_empty() {
                        ProxyError::NoHealthyUpstream
                    } else {
                        ProxyError::AllAttemptsFailed(attempt_no)
                    };
                    return HandleResult {
                        service: Some(service),
                        selected: first_selected,
                        response: Err(err),
                        attempts,
                    };
                }
            };
            if first_selected.is_none() {
                first_selected = Some(idx);
            }
            let is_retry = attempt_no > 0;
            if is_retry {
                self.total_failovers += 1;
            }

            pool.begin(idx);
            let reply = pool.dispatch(idx, req);
            let name = pool.backend(idx).name.clone();
            pool.end(idx);

            match reply.outcome {
                Ok(resp) => {
                    pool.record_success(idx, now);
                    attempts.push(Attempt {
                        backend: idx,
                        backend_name: name,
                        ok: true,
                        detail: format!("{}", resp.status),
                        latency_ms: reply.latency_ms,
                        is_retry,
                    });
                    return HandleResult {
                        service: Some(service),
                        selected: first_selected,
                        response: Ok(resp),
                        attempts,
                    };
                }
                Err(e) => {
                    pool.record_failure(idx, now);
                    attempts.push(Attempt {
                        backend: idx,
                        backend_name: name,
                        ok: false,
                        detail: format!("{e}"),
                        latency_ms: reply.latency_ms,
                        is_retry,
                    });
                    tried.push(idx);
                    attempt_no += 1;
                    if attempt_no > max_retries || !self.budget.try_retry() {
                        return HandleResult {
                            service: Some(service),
                            selected: first_selected,
                            response: Err(ProxyError::AllAttemptsFailed(attempt_no)),
                            attempts,
                        };
                    }
                    self.total_retries += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::breaker::BreakerConfig;
    use crate::clock::ManualClock;
    use crate::pool::{Backend, HealthConfig, Pool, Strategy};
    use crate::router::Route;
    use crate::upstream::{MockUpstream, Step, UpstreamError};

    fn backend(name: &str, up: MockUpstream, health: &HealthConfig) -> Backend {
        Backend::new(name, 1, Box::new(up), health, BreakerConfig::default())
    }

    fn proxy_with(pool: Pool, clock: Rc<dyn Clock>) -> Proxy {
        let mut router = Router::new();
        router.add(Route::new(None, "/", pool.name()));
        let mut p = Proxy::new(clock, router, RetryConfig::default());
        p.add_pool(pool);
        p
    }

    #[test]
    fn retry_fails_over_to_healthy_backend() {
        let clock: Rc<dyn Clock> = Rc::new(ManualClock::new(0));
        let health = HealthConfig::default();
        let mut pool = Pool::new("svc", Strategy::RoundRobin, health);
        let bad = MockUpstream::new("bad");
        bad.push(Step::fail(UpstreamError::Timeout));
        let good = MockUpstream::new("good");
        pool.add_backend(backend("bad", bad, &health));
        pool.add_backend(backend("good", good, &health));
        let mut proxy = proxy_with(pool, clock);

        let res = proxy.handle(&Request::get("/").with_header("Host", "h"));
        assert!(res.is_success(), "should fail over and succeed");
        assert_eq!(res.status(), 200);
        assert_eq!(res.attempts.len(), 2);
        assert!(!res.attempts[0].ok);
        assert!(res.attempts[1].ok);
        assert!(res.attempts[1].is_retry);
        assert_eq!(proxy.total_failovers(), 1);
    }

    #[test]
    fn no_route_returns_404() {
        let clock: Rc<dyn Clock> = Rc::new(ManualClock::new(0));
        let mut router = Router::new();
        router.add(Route::new(None, "/only", "svc"));
        let mut proxy = Proxy::new(clock, router, RetryConfig::default());
        let health = HealthConfig::default();
        proxy.add_pool(Pool::new("svc", Strategy::RoundRobin, health));
        let res = proxy.handle(&Request::get("/other").with_header("Host", "h"));
        assert_eq!(res.status(), 404);
        assert_eq!(res.response, Err(ProxyError::NoRoute));
    }

    #[test]
    fn all_unhealthy_returns_503() {
        let clock: Rc<dyn Clock> = Rc::new(ManualClock::new(0));
        let health = HealthConfig::default();
        let mut pool = Pool::new("svc", Strategy::RoundRobin, health);
        let up = MockUpstream::always_failing("bad", UpstreamError::Connection);
        pool.add_backend(backend("bad", up, &health));
        let mut proxy = proxy_with(pool, clock);
        // Trip the single backend's breaker.
        let mut last = 0;
        for _ in 0..10 {
            last = proxy.handle(&Request::get("/").with_header("Host", "h")).status();
        }
        assert!(last == 502 || last == 503);
        let breaker_open = proxy
            .pool("svc")
            .unwrap()
            .backend(0)
            .breaker_state();
        assert_eq!(breaker_open, crate::breaker::BreakerState::Open);
    }

    #[test]
    fn sticky_key_routes_consistently() {
        let clock: Rc<dyn Clock> = Rc::new(ManualClock::new(0));
        let health = HealthConfig::default();
        let mut pool = Pool::new("svc", Strategy::ConsistentHash, health);
        for n in ["a", "b", "c"] {
            pool.add_backend(backend(n, MockUpstream::new(n), &health));
        }
        let mut proxy = proxy_with(pool, clock);
        let req = Request::get("/")
            .with_header("Host", "h")
            .with_header("X-Session-Id", "user-777");
        let first = proxy.handle(&req).selected;
        for _ in 0..20 {
            assert_eq!(proxy.handle(&req).selected, first);
        }
    }
}
