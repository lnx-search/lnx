use std::collections::VecDeque;
use std::{cmp, io};

use crossbeam_channel::{self, Receiver, Sender};
use parking_lot::Mutex;

use super::mem_block::{
    PageFreePermit,
    PageIndex,
    PageOrRetry,
    PrepareRevertibleEvictionError,
    TryFreeError,
    VirtualMemoryBlock,
};

/// The minimum number of outstanding pages scheduled for a revertible  eviction that are outstanding
/// before the system will consider running the cleanup step during a read task.
const RUN_CLEANUP_REVERTIBLE_EVICTION_THRESHOLD: usize = 1_000;
/// The minimum number of outstanding pages scheduled for dirty eviction that are outstanding
/// before the system will consider running the cleanup step during a read task.
const RUN_CLEANUP_DIRTY_EVICTION_THRESHOLD: usize = 500;

/// Keeps track of pages to mark for eviction and frees
/// pages once safe to do so.
pub struct PendingEvictions {
    incoming_revertible_evictions: Receiver<PageIndex>,
    revertible_eviction_backlog: Mutex<Backlog>,
    dirty_eviction_backlog: Mutex<VecDeque<PageFreePermit>>,
}

impl PendingEvictions {
    pub(super) fn new() -> (Self, Sender<PageIndex>) {
        let (tx, rx) = crossbeam_channel::unbounded();

        let slf = Self {
            incoming_revertible_evictions: rx,
            revertible_eviction_backlog: Mutex::new(Backlog::default()),
            dirty_eviction_backlog: Mutex::new(VecDeque::new()),
        };

        (slf, tx)
    }

    pub(super) fn process_page_dirty_permit(&self, permit: PageFreePermit) {
        let mut backlog = self.dirty_eviction_backlog.lock();
        backlog.push_back(permit);
    }

    /// Attempts to perform some cleanup maintenance on the backlog is applicable.
    pub(super) fn try_cleanup(&self, memory: &VirtualMemoryBlock) {
        const SMALL_FREE_LIMIT: usize = 100;

        let Some(mut backlog) = self.revertible_eviction_backlog.try_lock() else {
            return;
        };

        let total_outstanding_evictions = self.incoming_revertible_evictions.len()
            + backlog.pages_to_mark.len()
            + backlog.pages_to_evict.len();

        if total_outstanding_evictions >= RUN_CLEANUP_REVERTIBLE_EVICTION_THRESHOLD {
            self.cleanup_revertible_evictions(SMALL_FREE_LIMIT, memory, &mut backlog);
        }

        let Some(mut dirty_pages) = self.dirty_eviction_backlog.try_lock() else {
            return;
        };
        if dirty_pages.len() >= RUN_CLEANUP_DIRTY_EVICTION_THRESHOLD {
            free_pages(SMALL_FREE_LIMIT, memory, &mut dirty_pages);
        }
    }

    /// Waits for locks to become available and clears the backlog of evictions.
    pub(super) fn cleanup(&self, memory: &VirtualMemoryBlock) {
        let mut backlog = self.revertible_eviction_backlog.lock();
        self.cleanup_revertible_evictions(usize::MAX, memory, &mut backlog);

        let mut dirty_pages = self.dirty_eviction_backlog.lock();
        free_pages(usize::MAX, memory, &mut dirty_pages);
    }

    fn cleanup_revertible_evictions(
        &self,
        limit: usize,
        memory: &VirtualMemoryBlock,
        backlog: &mut Backlog,
    ) {
        while let Ok(page) = self.incoming_revertible_evictions.try_recv() {
            backlog.pages_to_mark.push_back(PageOrRetry::Page(page));
        }

        // We cap the number of things we process in order to even the load on tasks.
        for _ in 0..backlog.pages_to_mark.len() {
            let Some(page) = backlog.pages_to_mark.pop_front() else {
                break;
            };
            match memory.try_mark_for_revertible_eviction(page) {
                Ok(permit) => {
                    backlog.pages_to_evict.push_back(permit);
                },
                Err(PrepareRevertibleEvictionError::PageLocked(retry)) => {
                    backlog.pages_to_mark.push_back(PageOrRetry::Retry(retry));
                },
                Err(PrepareRevertibleEvictionError::AlreadyFree) => {},
                Err(PrepareRevertibleEvictionError::Dirty) => {},
                Err(PrepareRevertibleEvictionError::OperationStale) => {},
            }
        }

        free_pages(limit, memory, &mut backlog.pages_to_evict);
    }
}

fn free_pages(
    limit: usize,
    memory: &VirtualMemoryBlock,
    permits: &mut VecDeque<PageFreePermit>,
) {
    let pop_n = cmp::min(permits.len(), limit);
    for _ in 0..pop_n {
        let Some(permit) = permits.pop_front() else {
            break;
        };

        match memory.try_free(&permit) {
            Ok(()) => {},
            Err(TryFreeError::InUse | TryFreeError::Locked) => {
                permits.push_back(permit);
            },
            Err(TryFreeError::PermitExpired) => {},
            Err(TryFreeError::Io(error)) => {
                tracing::error!(error = %error, "cache failed to free page");
                permits.push_back(permit);
            },
        }
    }
}

#[derive(Default)]
struct Backlog {
    pages_to_mark: VecDeque<PageOrRetry>,
    pages_to_evict: VecDeque<PageFreePermit>,
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;
    use crate::cache::mem_block::PageSize;

    #[test]
    fn test_skip_cleanup() {
        let memory = VirtualMemoryBlock::allocate(8, PageSize::Std8KB).unwrap();

        let (evictions, tx) = PendingEvictions::new();
        evictions.try_cleanup(&memory);

        tx.send(PageIndex(0)).unwrap();
        tx.send(PageIndex(2)).unwrap();
        tx.send(PageIndex(3)).unwrap();
        evictions.try_cleanup(&memory);

        assert_eq!(evictions.incoming_revertible_evictions.len(), 3);
        assert_eq!(
            evictions
                .revertible_eviction_backlog
                .lock()
                .pages_to_evict
                .len(),
            0
        );
        assert_eq!(
            evictions
                .revertible_eviction_backlog
                .lock()
                .pages_to_mark
                .len(),
            0
        );
        assert_eq!(evictions.dirty_eviction_backlog.lock().len(), 0);
    }

    #[test]
    fn test_run_try_cleanup() {
        let memory = VirtualMemoryBlock::allocate(1024, PageSize::Std8KB).unwrap();
        let data = vec![1; 8 << 10];
        for id in 0..1000 {
            let permit = memory.try_prepare_for_write(PageIndex(id)).unwrap();
            memory.write_page(permit, &data);
        }

        let (evictions, tx) = PendingEvictions::new();
        for id in 0..1000 {
            tx.send(PageIndex(id)).unwrap();
        }

        evictions.try_cleanup(&memory);

        // The system will immediately try to free 100 pages.
        assert_eq!(evictions.incoming_revertible_evictions.len(), 0);
        assert_eq!(
            evictions
                .revertible_eviction_backlog
                .lock()
                .pages_to_evict
                .len(),
            900
        );
        assert_eq!(
            evictions
                .revertible_eviction_backlog
                .lock()
                .pages_to_mark
                .len(),
            0
        );
        assert_eq!(evictions.dirty_eviction_backlog.lock().len(), 0);

        evictions.cleanup(&memory);

        // 92 remain because each generation is 256 ops, and we have written and expired 1000 pages
        // so we expect that the 1 generation will be left which will still be alive.
        assert_eq!(evictions.incoming_revertible_evictions.len(), 0);
        assert_eq!(
            evictions
                .revertible_eviction_backlog
                .lock()
                .pages_to_evict
                .len(),
            92
        );
        assert_eq!(
            evictions
                .revertible_eviction_backlog
                .lock()
                .pages_to_mark
                .len(),
            0
        );
        assert_eq!(evictions.dirty_eviction_backlog.lock().len(), 0);

        for id in 0..164 {
            let permit = memory.try_prepare_for_write(PageIndex(id)).unwrap();
            memory.write_page(permit, &data);
        }

        evictions.cleanup(&memory);
        assert_eq!(evictions.incoming_revertible_evictions.len(), 0);
        assert_eq!(
            evictions
                .revertible_eviction_backlog
                .lock()
                .pages_to_evict
                .len(),
            0
        );
        assert_eq!(
            evictions
                .revertible_eviction_backlog
                .lock()
                .pages_to_mark
                .len(),
            0
        );
        assert_eq!(evictions.dirty_eviction_backlog.lock().len(), 0);

        memory
            .prepare_read(PageIndex(500)..PageIndex(505))
            .try_finish()
            .expect_err("pages should be freed");
    }

    #[test]
    fn test_locked_pages() {
        let memory = VirtualMemoryBlock::allocate(1024, PageSize::Std8KB).unwrap();
        let data = vec![1; 8 << 10];

        for id in 0..1023 {
            let permit = memory.try_prepare_for_write(PageIndex(id)).unwrap();
            memory.write_page(permit, &data);
        }

        let (evictions, tx) = PendingEvictions::new();

        tx.send(PageIndex(0)).unwrap();
        tx.send(PageIndex(2)).unwrap();
        tx.send(PageIndex(3)).unwrap();
        tx.send(PageIndex(1023)).unwrap();

        let permit = memory.try_prepare_for_write(PageIndex(1023)).unwrap();

        evictions.cleanup(&memory);
        assert_eq!(
            evictions
                .revertible_eviction_backlog
                .lock()
                .pages_to_mark
                .len(),
            1
        );

        drop(permit);
    }

    #[test]
    fn test_evict_dirty() {
        let memory = VirtualMemoryBlock::allocate(8, PageSize::Std8KB).unwrap();

        let (evictions, _tx) = PendingEvictions::new();

        let data = vec![1; 8 << 10];
        for id in 0..8 {
            let permit = memory.try_prepare_for_write(PageIndex(id)).unwrap();
            memory.write_page(permit, &data);
        }

        let permit = memory
            .try_dirty_page(PageOrRetry::Page(PageIndex(0)))
            .unwrap();
        evictions.process_page_dirty_permit(permit);

        evictions.cleanup(&memory);
        assert_eq!(evictions.dirty_eviction_backlog.lock().len(), 1);
    }
}
