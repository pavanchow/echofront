//! A retry budget. Retries improve success rates but a naive per request retry
//! count amplifies load during an incident. The budget is a token bucket: each
//! new request deposits a small fraction of a token, each retry costs a whole
//! token. Retries are only permitted while tokens remain, capping the retry rate
//! near the deposit ratio. Integer milli tokens keep it fully deterministic.

#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    /// Maximum retries attempted for a single request, budget permitting.
    pub max_retries_per_request: u32,
    /// Tokens deposited per new request, in milli tokens. 200 means 0.2.
    pub deposit_milli: i64,
    /// Cost of one retry, in milli tokens.
    pub retry_cost_milli: i64,
    /// Cap on the bucket, in milli tokens.
    pub max_milli: i64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries_per_request: 2,
            deposit_milli: 200,
            retry_cost_milli: 1_000,
            max_milli: 10_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RetryBudget {
    cfg: RetryConfig,
    tokens_milli: i64,
    granted: u64,
    denied: u64,
}

impl RetryBudget {
    pub fn new(cfg: RetryConfig) -> Self {
        // Start full so an isolated failing request can still fail over. The
        // bucket then throttles the sustained retry rate toward the deposit
        // ratio, allowing a burst but not unbounded amplification.
        Self {
            cfg,
            tokens_milli: cfg.max_milli,
            granted: 0,
            denied: 0,
        }
    }

    pub fn max_retries_per_request(&self) -> u32 {
        self.cfg.max_retries_per_request
    }

    /// Record that a new top level request arrived, depositing tokens.
    pub fn on_request(&mut self) {
        self.tokens_milli = (self.tokens_milli + self.cfg.deposit_milli).min(self.cfg.max_milli);
    }

    /// Try to spend a token for one retry. Returns true when the retry is allowed.
    pub fn try_retry(&mut self) -> bool {
        if self.tokens_milli >= self.cfg.retry_cost_milli {
            self.tokens_milli -= self.cfg.retry_cost_milli;
            self.granted += 1;
            true
        } else {
            self.denied += 1;
            false
        }
    }

    pub fn granted(&self) -> u64 {
        self.granted
    }

    pub fn denied(&self) -> u64 {
        self.denied
    }

    // Milli tokens are bounded by max_milli, far below the f64 precision limit.
    #[allow(clippy::cast_precision_loss)]
    pub fn tokens(&self) -> f64 {
        self.tokens_milli as f64 / 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(b: &mut RetryBudget) {
        // Spend the initial burst allowance without any deposits.
        while b.try_retry() {}
    }

    #[test]
    fn budget_caps_retry_rate() {
        let mut b = RetryBudget::new(RetryConfig::default());
        // 100 all failing requests, each trying one retry. The bucket allows an
        // initial burst then throttles toward the 20 percent deposit ratio, so
        // the granted count is far below 100 yet clearly nonzero.
        let mut allowed = 0;
        for _ in 0..100 {
            b.on_request();
            if b.try_retry() {
                allowed += 1;
            }
        }
        assert!(allowed < 60, "retries {allowed} not throttled below request count");
        assert!(allowed >= 20, "retries {allowed} unexpectedly low");
    }

    #[test]
    fn no_retry_once_drained() {
        let mut b = RetryBudget::new(RetryConfig::default());
        drain(&mut b);
        assert!(!b.try_retry());
        assert!(b.denied() >= 1);
    }

    #[test]
    fn deposits_accumulate_then_spend() {
        let mut b = RetryBudget::new(RetryConfig::default());
        drain(&mut b);
        for _ in 0..5 {
            b.on_request();
        }
        // 5 * 0.2 = 1.0 token, exactly one retry after being drained.
        assert!(b.try_retry());
        assert!(!b.try_retry());
    }
}
