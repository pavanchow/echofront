//! The upstream abstraction and a deterministic mock implementation. The engine
//! only ever talks to an `Upstream`, so tests inject a scripted mock and no real
//! sockets are involved anywhere in the test suite.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;

use crate::http::{Request, Response};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpstreamError {
    /// The upstream did not answer in time.
    Timeout,
    /// The connection could not be established.
    Connection,
    /// The upstream answered with a server error status.
    ServerError(u16),
}

impl fmt::Display for UpstreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpstreamError::Timeout => write!(f, "timeout"),
            UpstreamError::Connection => write!(f, "connection refused"),
            UpstreamError::ServerError(s) => write!(f, "server error {s}"),
        }
    }
}

impl std::error::Error for UpstreamError {}

/// Result of one call to an upstream, carrying a simulated latency so callers
/// can report and reason about it.
#[derive(Clone, Debug)]
pub struct UpstreamReply {
    pub outcome: Result<Response, UpstreamError>,
    pub latency_ms: u64,
}

impl UpstreamReply {
    pub fn is_ok(&self) -> bool {
        self.outcome.is_ok()
    }
}

/// A backend the proxy can send a request to. Implementors decide how a request
/// is answered. The health probe defaults to a HEAD style call.
pub trait Upstream {
    fn send(&self, req: &Request) -> UpstreamReply;

    /// Active health probe. Returns true when the upstream is considered live.
    fn probe(&self) -> bool {
        self.send(&Request::probe())
            .outcome
            .map(|r| !r.is_server_error())
            .unwrap_or(false)
    }
}

/// One scripted step for the mock. `Ok(status)` returns that status,
/// `Err(kind)` returns that error.
#[derive(Clone, Debug)]
pub struct Step {
    pub outcome: Result<u16, UpstreamError>,
    pub latency_ms: u64,
}

impl Step {
    pub fn ok(status: u16) -> Self {
        Step {
            outcome: Ok(status),
            latency_ms: 5,
        }
    }

    pub fn ok_slow(status: u16, latency_ms: u64) -> Self {
        Step {
            outcome: Ok(status),
            latency_ms,
        }
    }

    pub fn fail(err: UpstreamError) -> Self {
        Step {
            outcome: Err(err),
            latency_ms: 5,
        }
    }
}

/// A deterministic mock upstream. Responses come from a scripted queue, and once
/// the queue is empty the `default` step repeats forever. Health probe results
/// come from their own queue with their own default.
pub struct MockUpstream {
    name: String,
    script: RefCell<VecDeque<Step>>,
    default: Step,
    probes: RefCell<VecDeque<bool>>,
    probe_default: bool,
    calls: Cell<u64>,
    probe_calls: Cell<u64>,
}

impl MockUpstream {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            script: RefCell::new(VecDeque::new()),
            default: Step::ok(200),
            probes: RefCell::new(VecDeque::new()),
            probe_default: true,
            calls: Cell::new(0),
            probe_calls: Cell::new(0),
        }
    }

    /// Always healthy, always 200.
    pub fn healthy(name: impl Into<String>) -> Self {
        MockUpstream::new(name)
    }

    /// Always fails request and probe with the given error.
    pub fn always_failing(name: impl Into<String>, err: UpstreamError) -> Self {
        let mut m = MockUpstream::new(name);
        m.default = Step::fail(err);
        m.probe_default = false;
        m
    }

    pub fn with_default(mut self, step: Step) -> Self {
        self.default = step;
        self
    }

    pub fn with_probe_default(mut self, live: bool) -> Self {
        self.probe_default = live;
        self
    }

    pub fn push(&self, step: Step) {
        self.script.borrow_mut().push_back(step);
    }

    pub fn push_many(&self, steps: impl IntoIterator<Item = Step>) {
        self.script.borrow_mut().extend(steps);
    }

    pub fn push_probe(&self, live: bool) {
        self.probes.borrow_mut().push_back(live);
    }

    pub fn push_probes(&self, results: impl IntoIterator<Item = bool>) {
        self.probes.borrow_mut().extend(results);
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn calls(&self) -> u64 {
        self.calls.get()
    }

    pub fn probe_calls(&self) -> u64 {
        self.probe_calls.get()
    }

    fn next_step(&self) -> Step {
        self.script
            .borrow_mut()
            .pop_front()
            .unwrap_or_else(|| self.default.clone())
    }
}

impl Upstream for MockUpstream {
    fn send(&self, _req: &Request) -> UpstreamReply {
        self.calls.set(self.calls.get() + 1);
        let step = self.next_step();
        let outcome = match step.outcome {
            Ok(status) if status >= 500 => Err(UpstreamError::ServerError(status)),
            Ok(status) => Ok(Response::new(status).with_body(self.name.clone().into_bytes())),
            Err(e) => Err(e),
        };
        UpstreamReply {
            outcome,
            latency_ms: step.latency_ms,
        }
    }

    fn probe(&self) -> bool {
        self.probe_calls.set(self.probe_calls.get() + 1);
        self.probes
            .borrow_mut()
            .pop_front()
            .unwrap_or(self.probe_default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripted_then_default() {
        let m = MockUpstream::new("a");
        m.push(Step::ok(201));
        m.push(Step::fail(UpstreamError::Timeout));
        let r1 = m.send(&Request::get("/"));
        assert_eq!(r1.outcome.unwrap().status, 201);
        let r2 = m.send(&Request::get("/"));
        assert_eq!(r2.outcome.unwrap_err(), UpstreamError::Timeout);
        let r3 = m.send(&Request::get("/"));
        assert_eq!(r3.outcome.unwrap().status, 200);
        assert_eq!(m.calls(), 3);
    }

    #[test]
    fn five_hundred_maps_to_error() {
        let m = MockUpstream::new("a");
        m.push(Step::ok(503));
        let r = m.send(&Request::get("/"));
        assert_eq!(r.outcome.unwrap_err(), UpstreamError::ServerError(503));
    }

    #[test]
    fn probe_script_and_default() {
        let m = MockUpstream::new("a").with_probe_default(true);
        m.push_probes([false, false]);
        assert!(!m.probe());
        assert!(!m.probe());
        assert!(m.probe());
        assert_eq!(m.probe_calls(), 3);
    }

    #[test]
    fn always_failing_helper() {
        let m = MockUpstream::always_failing("bad", UpstreamError::Connection);
        assert!(!m.probe());
        assert_eq!(
            m.send(&Request::get("/")).outcome.unwrap_err(),
            UpstreamError::Connection
        );
    }
}
