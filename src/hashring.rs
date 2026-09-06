//! A consistent hash ring for sticky session routing. Each member is placed on
//! the ring at many virtual node positions so that adding or removing a member
//! remaps only a small fraction of keys.

use std::collections::BTreeMap;

/// FNV-1a 64 bit. Stable across platforms, no external crate.
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// `SplitMix64` finalizer. Used to spread virtual node positions uniformly around
/// the ring so no member is starved of the key space.
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[derive(Clone)]
pub struct ConsistentHashRing {
    vnodes: u32,
    ring: BTreeMap<u64, usize>,
    members: Vec<usize>,
}

impl ConsistentHashRing {
    pub fn new(vnodes: u32) -> Self {
        Self {
            vnodes: vnodes.max(1),
            ring: BTreeMap::new(),
            members: Vec::new(),
        }
    }

    /// Rebuild the ring from the given members. `members` is a list of
    /// (identity, name) where identity is what `lookup` returns and name seeds
    /// the virtual node positions.
    pub fn rebuild(&mut self, members: &[(usize, &str)]) {
        self.ring.clear();
        self.members = members.iter().map(|(id, _)| *id).collect();
        for (id, name) in members {
            let base = fnv1a_64(name.as_bytes());
            for v in 0..self.vnodes {
                let point = mix64(base.wrapping_add(mix64(u64::from(v))));
                self.ring.insert(point, *id);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    pub fn members(&self) -> &[usize] {
        &self.members
    }

    /// The member responsible for `key`, walking clockwise from the key hash and
    /// wrapping to the first node past the end of the ring.
    pub fn lookup(&self, key: &str) -> Option<usize> {
        if self.ring.is_empty() {
            return None;
        }
        // Mix the key hash the same way as the ring points. Raw FNV-1a clusters
        // keys that share a long prefix, which would starve some members.
        let h = mix64(fnv1a_64(key.as_bytes()));
        if let Some((_, id)) = self.ring.range(h..).next() {
            Some(*id)
        } else {
            self.ring.values().next().copied()
        }
    }
}

#[cfg(test)]
// Percent math on small bounded counts in these tests.
#[allow(clippy::cast_precision_loss)]
mod tests {
    use super::*;

    fn build(names: &[(usize, &str)], vnodes: u32) -> ConsistentHashRing {
        let mut r = ConsistentHashRing::new(vnodes);
        r.rebuild(names);
        r
    }

    #[test]
    fn same_key_same_member() {
        let r = build(&[(0, "a"), (1, "b"), (2, "c")], 100);
        let first = r.lookup("session-42").unwrap();
        for _ in 0..100 {
            assert_eq!(r.lookup("session-42"), Some(first));
        }
    }

    #[test]
    fn removal_remaps_minimal_fraction() {
        let members = [(0, "a"), (1, "b"), (2, "c"), (3, "d")];
        let before = build(&members, 200);
        let keys: Vec<String> = (0..5000).map(|i| format!("key-{i}")).collect();
        let mapping: Vec<usize> = keys.iter().map(|k| before.lookup(k).unwrap()).collect();

        // Remove member 3.
        let after = build(&[(0, "a"), (1, "b"), (2, "c")], 200);
        let mut moved = 0usize;
        let mut moved_from_other = 0usize;
        for (k, &old) in keys.iter().zip(&mapping) {
            let new = after.lookup(k).unwrap();
            if new != old {
                moved += 1;
                if old != 3 {
                    moved_from_other += 1;
                }
            }
        }
        // Keys that were NOT on the removed node must not move.
        assert_eq!(moved_from_other, 0, "keys on surviving nodes must not move");
        // Fraction moved should be close to 1/4.
        let frac = moved as f64 / keys.len() as f64;
        assert!(frac > 0.15 && frac < 0.35, "moved fraction {frac} not near 1/4");
    }

    #[test]
    fn prefix_similar_keys_spread_across_members() {
        let r = build(&[(0, "A"), (1, "B"), (2, "C"), (3, "D")], 160);
        let mut counts = [0usize; 4];
        for i in 0..400 {
            counts[r.lookup(&format!("user-{i}")).unwrap()] += 1;
        }
        for (i, c) in counts.iter().enumerate() {
            assert!(*c > 0, "member {i} starved of prefix similar keys: {counts:?}");
        }
    }

    #[test]
    fn distributes_across_members() {
        let r = build(&[(0, "a"), (1, "b"), (2, "c"), (3, "d")], 200);
        let mut counts = [0usize; 4];
        for i in 0..8000 {
            let id = r.lookup(&format!("k-{i}")).unwrap();
            counts[id] += 1;
        }
        for c in counts {
            assert!(c > 0, "every member should receive some keys");
        }
    }
}
