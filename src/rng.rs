//! A tiny deterministic PRNG (`SplitMix64`) so the property and fuzz gates are
//! seedable and reproducible with no external crates.

// A PRNG by nature casts wide integers around: the unit float discards low bits
// by design and the index bound is runtime small.
#![allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]

/// `SplitMix64`. Same seed produces the same stream on every platform.
#[derive(Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform integer in `[0, bound)`. `bound` must be non zero.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }

    /// A `usize` index in `[0, len)`.
    pub fn index(&mut self, len: usize) -> usize {
        (self.below(len as u64)) as usize
    }

    /// A float in `[0.0, 1.0)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_stream() {
        let mut a = Rng::new(12345);
        let mut b = Rng::new(12345);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn below_is_bounded() {
        let mut r = Rng::new(1);
        for _ in 0..10_000 {
            assert!(r.below(7) < 7);
        }
    }

    #[test]
    fn unit_in_range() {
        let mut r = Rng::new(9);
        for _ in 0..10_000 {
            let u = r.unit();
            assert!((0.0..1.0).contains(&u));
        }
    }
}
