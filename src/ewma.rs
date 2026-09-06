//! EWMA latency scoring. Each backend keeps an exponentially weighted moving
//! average of its request latency. A node whose EWMA rises above the pool mean
//! by a configured factor for a sustained window is ejected as a latency
//! outlier, then reinstated gradually: it comes back at a fraction of its
//! weight and ramps up only while it stays clean. Everything runs on the
//! injected clock, integer milli units, no floats in the decision path.

/// Configuration for the EWMA outlier layer. Disabled unless a config is
/// attached to the pool with `Pool::enable_ewma`.
#[derive(Clone, Copy, Debug)]
pub struct EwmaConfig {
    /// Smoothing factor in milli units. Each new sample moves the EWMA by
    /// alpha_milli / 1000 of the gap. 250 means one quarter per sample.
    pub alpha_milli: u64,
    /// Eject when a node EWMA exceeds the pool mean by this factor in milli
    /// units. 3000 means three times the pool mean.
    pub factor_milli: u64,
    /// The node EWMA must stay above the line for this long before ejection,
    /// so a single slow burst cannot eject a healthy node.
    pub window_ms: u64,
    /// Nodes below this sample count neither contribute to the pool mean nor
    /// get ejected.
    pub min_samples: u32,
    /// How long an EWMA ejection lasts before reinstatement begins.
    pub ejection_ms: u64,
    /// A reinstated node returns at this percent of its weight.
    pub reinstate_percent: u32,
    /// Each clean window of this length doubles the ramp percent until it is
    /// back to full weight.
    pub ramp_window_ms: u64,
}

impl Default for EwmaConfig {
    fn default() -> Self {
        Self {
            alpha_milli: 250,
            factor_milli: 3_000,
            window_ms: 2_000,
            min_samples: 8,
            ejection_ms: 1_500,
            reinstate_percent: 25,
            ramp_window_ms: 1_000,
        }
    }
}

impl EwmaConfig {
    /// The ejection line for a pool mean given in milli milliseconds.
    pub fn threshold_milli(&self, pool_mean_milli: u64) -> u64 {
        pool_mean_milli
            .saturating_mul(self.factor_milli)
            .saturating_div(1_000)
    }

    /// Fold one latency sample (in milliseconds) into an EWMA held in milli
    /// units. `None` means no sample yet, so the first sample seeds it whole.
    pub fn fold(&self, current: Option<u64>, latency_ms: u64) -> u64 {
        let sample_milli = latency_ms.saturating_mul(1_000);
        match current {
            None => sample_milli,
            Some(prev) => {
                let alpha = self.alpha_milli.min(1_000);
                (alpha * sample_milli + (1_000 - alpha) * prev) / 1_000
            }
        }
    }
}

/// One row of the pool distribution report: what a node actually received
/// versus what its share of the healthy traffic says it should receive.
#[derive(Clone, Debug)]
pub struct DistributionRow {
    pub name: String,
    pub served: u64,
    /// Share of all recorded outcomes this node received, in percent.
    pub served_share_pct: f64,
    /// Share this node should receive given the effective weights of the
    /// currently available nodes, in percent.
    pub expected_share_pct: f64,
    /// Configured weight of the node.
    pub weight: u32,
    /// Current EWMA of request latency in milli milliseconds.
    pub ewma_milli: Option<u64>,
    pub ejected: bool,
    /// Current reinstatement ramp percent, 100 means full weight.
    pub ramp_percent: u32,
    pub ewma_ejections: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_seeds_then_smooths() {
        let cfg = EwmaConfig::default();
        let a = cfg.fold(None, 100);
        assert_eq!(a, 100_000);
        // alpha 250: 0.25 * 0 + 0.75 * 100000 = 75000
        let b = cfg.fold(Some(a), 0);
        assert_eq!(b, 75_000);
    }

    #[test]
    fn threshold_scales_mean() {
        let cfg = EwmaConfig::default();
        assert_eq!(cfg.threshold_milli(10_000), 30_000);
        assert_eq!(cfg.threshold_milli(0), 0);
    }
}
