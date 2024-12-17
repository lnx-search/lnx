use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

use crate::metastore::TabletId;

/// The waker factory that creates [FlushWaker]s for the configured tablet writer.
pub struct FlushWakerController {
    tablet_id: TabletId,
    flush_offset: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl FlushWakerController {
    pub(super) fn new(tablet_id: TabletId) -> Self {
        Self {
            tablet_id,
            flush_offset: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub(super) fn wake(&self, offset: u64) {
        self.flush_offset.store(offset, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    pub(super) fn create_waker(&self, target_offset: u64) -> FlushWaker {
        FlushWaker {
            tablet_id: self.tablet_id,
            target_offset,
            flush_offset: self.flush_offset.clone(),
            notify: self.notify.clone(),
        }
    }
}

#[derive(Clone, Debug)]
/// A waker that allows users to wait for when the writer
/// has completed a flush operation.
///
/// By default, the writer flushes `N` milliseconds after writing
/// and can coalesce multiple flushes into one within a short time frame.
pub struct FlushWaker {
    /// The tablet ID attached to this waker.
    tablet_id: TabletId,
    /// The offset that the writer must have commited up to.
    target_offset: u64,
    /// The current commited offset of the writer.
    ///
    /// If this is greater than or equal to the `target_offset`
    /// then there is no requirement to wait for the commit event.
    flush_offset: Arc<AtomicU64>,
    /// The internal waker.
    notify: Arc<Notify>,
}

impl FlushWaker {
    #[cfg(test)]
    /// Creates a new waker that will always resolve for tests
    pub(crate) fn create_for_test() -> Self {
        Self {
            tablet_id: TabletId::new(),
            target_offset: 0,
            flush_offset: Arc::new(AtomicU64::new(1)),
            notify: Arc::new(Notify::new()),
        }
    }

    #[inline]
    /// The [TabletId] the waker belongs to.
    pub fn tablet_id(&self) -> TabletId {
        self.tablet_id
    }

    /// Wait for the flush to be completed.
    ///
    /// If the writer has already flushed the necessary bytes then this resolves
    /// immediately.
    pub async fn wait(self) {
        if self.needs_to_wait() {
            self.notify.notified().await;
        }
    }

    fn needs_to_wait(&self) -> bool {
        let commit_offset = self.flush_offset.load(Ordering::Relaxed);
        self.target_offset > commit_offset
    }
}

#[derive(Default)]
/// A collection of [FlushWaker] which can be added
/// and waited on concurrently.
pub struct BulkFlushWaker {
    flushes: Vec<FlushWaker>,
}

impl BulkFlushWaker {
    /// Adds a new [FlushWaker] to the collection.
    pub fn push(&mut self, waker: FlushWaker) {
        self.flushes.push(waker);
    }

    /// Waits for all [FlushWaker] to signal the data is persisted.
    pub async fn wait(self) {
        for waker in self.flushes {
            waker.wait().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_tablet_propagated() {
        let tablet_id = TabletId::new();
        let controller = FlushWakerController::new(tablet_id);
        let waker = controller.create_waker(5);
        assert_eq!(waker.tablet_id(), tablet_id);
    }

    #[tokio::test]
    async fn test_waker_skips_wait_if_offset_caught_up() {
        let controller = FlushWakerController::new(TabletId::new());

        let waker = controller.create_waker(5);
        controller.wake(8);

        let res = tokio::time::timeout(Duration::from_secs(0), waker.wait()).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_waker_skips_wait_for_notify() {
        let controller = FlushWakerController::new(TabletId::new());

        let waker = controller.create_waker(5);
        controller.wake(2);

        let res = tokio::time::timeout(Duration::from_millis(100), waker.wait()).await;
        assert!(res.is_err());

        let waker = controller.create_waker(5);
        controller.wake(2);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            controller.wake(10);
        });
        tokio::time::sleep(Duration::from_millis(1)).await;

        let res = tokio::time::timeout(Duration::from_millis(100), waker.wait()).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_bulk_waker_basic() {
        let controller = FlushWakerController::new(TabletId::new());

        let waker1 = controller.create_waker(5);
        let waker2 = controller.create_waker(10);
        let waker3 = controller.create_waker(15);

        controller.wake(15);

        let mut bulk = BulkFlushWaker::default();
        bulk.push(waker1);
        bulk.push(waker2);
        bulk.push(waker3);

        let res = tokio::time::timeout(Duration::from_secs(0), bulk.wait()).await;
        assert!(res.is_ok());
    }
}
