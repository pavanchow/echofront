//! Request routing. Matches a request to a service name by optional host and by
//! longest matching path prefix. This is intentionally simple, it decides which
//! upstream pool handles the request, nothing more.

use crate::http::Request;

#[derive(Clone, Debug)]
pub struct Route {
    /// When set, the request Host header must equal this (case insensitive).
    pub host: Option<String>,
    /// The path prefix that must match. "/" matches everything.
    pub path_prefix: String,
    /// The service (pool) name to route to.
    pub service: String,
}

impl Route {
    pub fn new(
        host: Option<&str>,
        path_prefix: impl Into<String>,
        service: impl Into<String>,
    ) -> Self {
        Self {
            host: host.map(|h| h.to_ascii_lowercase()),
            path_prefix: path_prefix.into(),
            service: service.into(),
        }
    }

    fn matches(&self, req: &Request) -> bool {
        if let Some(h) = &self.host {
            match req.host() {
                Some(rh) if rh.eq_ignore_ascii_case(h) => {}
                _ => return false,
            }
        }
        path_prefix_matches(&self.path_prefix, &req.path)
    }
}

/// A prefix matches when the path equals it or continues with a `/` boundary,
/// so `/api` matches `/api` and `/api/x` but not `/apixyz`.
fn path_prefix_matches(prefix: &str, path: &str) -> bool {
    if prefix == "/" {
        return true;
    }
    let path_head = path.split('?').next().unwrap_or(path);
    if !path_head.starts_with(prefix) {
        return false;
    }
    match path_head.as_bytes().get(prefix.len()) {
        None => true,
        Some(b'/') => true,
        _ => prefix.ends_with('/'),
    }
}

#[derive(Default)]
pub struct Router {
    routes: Vec<Route>,
}

impl Router {
    pub fn new() -> Self {
        Self { routes: Vec::new() }
    }

    pub fn add(&mut self, route: Route) {
        self.routes.push(route);
    }

    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    /// The service for this request. Among matching routes, the one with the
    /// longest path prefix wins, with a host specific route breaking ties.
    pub fn route(&self, req: &Request) -> Option<&str> {
        self.routes
            .iter()
            .filter(|r| r.matches(req))
            .max_by_key(|r| (r.path_prefix.len(), r.host.is_some()))
            .map(|r| r.service.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(host: &str, path: &str) -> Request {
        Request::get(path).with_header("Host", host)
    }

    #[test]
    fn longest_prefix_wins() {
        let mut r = Router::new();
        r.add(Route::new(None, "/", "root"));
        r.add(Route::new(None, "/api", "api"));
        r.add(Route::new(None, "/api/v2", "apiv2"));
        assert_eq!(r.route(&req("h", "/")), Some("root"));
        assert_eq!(r.route(&req("h", "/api/users")), Some("api"));
        assert_eq!(r.route(&req("h", "/api/v2/things")), Some("apiv2"));
    }

    #[test]
    fn host_scoping() {
        let mut r = Router::new();
        r.add(Route::new(Some("a.example.com"), "/", "svc_a"));
        r.add(Route::new(Some("b.example.com"), "/", "svc_b"));
        assert_eq!(r.route(&req("a.example.com", "/x")), Some("svc_a"));
        assert_eq!(r.route(&req("b.example.com", "/x")), Some("svc_b"));
        assert_eq!(r.route(&req("c.example.com", "/x")), None);
    }

    #[test]
    fn prefix_boundary() {
        let mut r = Router::new();
        r.add(Route::new(None, "/api", "api"));
        assert_eq!(r.route(&req("h", "/apixyz")), None);
        assert_eq!(r.route(&req("h", "/api")), Some("api"));
        assert_eq!(r.route(&req("h", "/api?q=1")), Some("api"));
    }

    #[test]
    fn no_match() {
        let mut r = Router::new();
        r.add(Route::new(None, "/only", "svc"));
        assert_eq!(r.route(&req("h", "/other")), None);
    }
}
