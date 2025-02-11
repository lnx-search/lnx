use std::hash::{BuildHasher, Hasher};

#[derive(Debug, Copy, Clone)]
/// A [BuildHasher] that produces a [NoOpHasher].
pub struct NoOpRandomState;

impl BuildHasher for NoOpRandomState {
    type Hasher = NoOpHasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        NoOpHasher(0)
    }
}

/// A [NoOpHasher] accepts the last provided `u64` value
/// as the finished hash, it does not additional work.
pub struct NoOpHasher(u64);

impl Hasher for NoOpHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!("NoOpHasher cannot hash arbitrary bytes");
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}
