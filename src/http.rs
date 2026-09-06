//! A minimal HTTP/1.1 request and response model plus a small parser. This is
//! deliberately just enough to route on: methods, path, headers and an optional
//! body. It is not a hardened server parser.

use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
    Other(String),
}

impl Method {
    pub fn parse(s: &str) -> Method {
        match s {
            "GET" => Method::Get,
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "DELETE" => Method::Delete,
            "PATCH" => Method::Patch,
            "HEAD" => Method::Head,
            "OPTIONS" => Method::Options,
            other => Method::Other(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
            Method::Patch => "PATCH",
            Method::Head => "HEAD",
            Method::Options => "OPTIONS",
            Method::Other(s) => s,
        }
    }

    /// Whether it is safe to retry this method on a different upstream without
    /// changing observable state. Only idempotent methods qualify.
    pub fn is_idempotent(&self) -> bool {
        matches!(
            self,
            Method::Get | Method::Head | Method::Put | Method::Delete | Method::Options
        )
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    pub method: Method,
    pub path: String,
    pub version: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn new(method: Method, path: impl Into<String>) -> Self {
        Self {
            method,
            path: path.into(),
            version: "HTTP/1.1".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn get(path: impl Into<String>) -> Self {
        Request::new(Method::Get, path)
    }

    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Case insensitive header lookup, first match wins.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn host(&self) -> Option<&str> {
        self.header("Host")
    }

    /// The health probe request the active health checker sends.
    pub fn probe() -> Self {
        Request::new(Method::Head, "/healthz")
    }

    /// Parse a raw HTTP/1.1 request. Headers are split on the first colon and
    /// trimmed. The body is everything after the blank line.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::Empty` for an empty or blank request and
    /// `ParseError::MalformedRequestLine` or `ParseError::MalformedHeader` when
    /// the request line or a header line cannot be split.
    pub fn parse(raw: &str) -> Result<Request, ParseError> {
        let (head, body) = match raw.split_once("\r\n\r\n") {
            Some((h, b)) => (h, b),
            None => match raw.split_once("\n\n") {
                Some((h, b)) => (h, b),
                None => (raw, ""),
            },
        };

        let mut lines = head.split('\n').map(|l| l.trim_end_matches('\r'));
        let request_line = lines.next().ok_or(ParseError::Empty)?;
        if request_line.trim().is_empty() {
            return Err(ParseError::Empty);
        }
        let mut parts = request_line.split_whitespace();
        let method = parts.next().ok_or(ParseError::MalformedRequestLine)?;
        let path = parts.next().ok_or(ParseError::MalformedRequestLine)?;
        let version = parts.next().unwrap_or("HTTP/1.1");

        let mut headers = Vec::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| ParseError::MalformedHeader(line.to_string()))?;
            headers.push((name.trim().to_string(), value.trim().to_string()));
        }

        Ok(Request {
            method: Method::parse(method),
            path: path.to_string(),
            version: version.to_string(),
            headers,
            body: body.as_bytes().to_vec(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub reason: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            reason: reason_phrase(status).to_string(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    pub fn is_success(&self) -> bool {
        (200..400).contains(&self.status)
    }

    pub fn is_server_error(&self) -> bool {
        self.status >= 500
    }
}

pub fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    Empty,
    MalformedRequestLine,
    MalformedHeader(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Empty => write!(f, "empty request"),
            ParseError::MalformedRequestLine => write!(f, "malformed request line"),
            ParseError::MalformedHeader(l) => write!(f, "malformed header: {l}"),
        }
    }
}

impl std::error::Error for ParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_request_line_and_headers() {
        let raw = "GET /api/users?id=3 HTTP/1.1\r\nHost: api.example.com\r\nAccept: */*\r\n\r\n";
        let req = Request::parse(raw).unwrap();
        assert_eq!(req.method, Method::Get);
        assert_eq!(req.path, "/api/users?id=3");
        assert_eq!(req.version, "HTTP/1.1");
        assert_eq!(req.host(), Some("api.example.com"));
        assert_eq!(req.header("accept"), Some("*/*"));
    }

    #[test]
    fn parses_body() {
        let raw = "POST /submit HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello";
        let req = Request::parse(raw).unwrap();
        assert_eq!(req.method, Method::Post);
        assert_eq!(req.body, b"hello");
    }

    #[test]
    fn tolerates_lf_only() {
        let raw = "GET / HTTP/1.1\nHost: h\n\n";
        let req = Request::parse(raw).unwrap();
        assert_eq!(req.path, "/");
        assert_eq!(req.host(), Some("h"));
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(Request::parse("").unwrap_err(), ParseError::Empty);
    }

    #[test]
    fn idempotency() {
        assert!(Method::Get.is_idempotent());
        assert!(!Method::Post.is_idempotent());
    }

    #[test]
    fn response_classification() {
        assert!(Response::new(200).is_success());
        assert!(!Response::new(500).is_success());
        assert!(Response::new(503).is_server_error());
    }
}
