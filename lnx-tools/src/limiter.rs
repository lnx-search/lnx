use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
/// A limiter that applies backpressure if a given
/// capacity is exceeded.
///
/// This effectively just wraps a semaphore.
pub struct BufferLimiter {
    capacity: u32,
    semaphore: Arc<Semaphore>,
}

impl Debug for BufferLimiter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BufferLimiter(capacity={})", self.capacity)
    }
}

impl BufferLimiter {
    /// Creates a new limiter with a given capacity in bytes.
    pub fn new(capacity: u32) -> Self {
        Self {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity as usize)),
        }
    }

    /// Attempts to allocate `size` bytes, this will apply backpressure
    /// if that amount cannot be allocated.
    pub async fn allocate(&self, size: u32) -> BufferAllocationPermit {
        assert!(
            size <= self.capacity,
            "The overall size of the allocation exceeds the maximum capacity"
        );
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(size)
            .await
            .expect("Semaphore has been closed.");
        BufferAllocationPermit(permit)
    }
}

/// A wrapper around a [OwnedSemaphorePermit] that should be held
/// until the buffer can be released.
pub struct BufferAllocationPermit(OwnedSemaphorePermit);
