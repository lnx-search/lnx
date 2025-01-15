use std::hash::{BuildHasher, Hasher};

#[derive(Default, Copy, Clone, Debug)]
/// A [BuildHasher] that returns a no-op hasher.
///
/// This performs no actual hashing operation.
pub struct NoOpRandomState;

impl BuildHasher for NoOpRandomState {
    type Hasher = NoOpHasher;

    fn build_hasher(&self) -> Self::Hasher {
        NoOpHasher(0)
    }
}

/// A hasher that performs no true-hashing operation and simply
/// takes the last u64 value it was given via `hash_u64`.
pub struct NoOpHasher(u64);

impl Hasher for NoOpHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!("No-op hasher cannot hash arbitrary bytes")
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}
