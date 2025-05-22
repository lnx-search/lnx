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
        let Some(mut backlog) = self.revertible_eviction_backlog.try_lock() else {
            return;
        };

        let total_outstanding_evictions = self.incoming_revertible_evictions.len()
            + backlog.pages_to_mark.len()
            + backlog.pages_to_evict.len();

        if total_outstanding_evictions >= RUN_CLEANUP_REVERTIBLE_EVICTION_THRESHOLD {
            self.cleanup_revertible_evictions(memory, &mut backlog);
        }

        let Some(mut dirty_pages) = self.dirty_eviction_backlog.try_lock() else {
            return;
        };
        if dirty_pages.len() >= RUN_CLEANUP_DIRTY_EVICTION_THRESHOLD {
            free_pages(memory, &mut dirty_pages);
        }
    }

    /// Waits for locks to become available and clears the backlog of evictions.
    pub(super) fn cleanup(&self, memory: &VirtualMemoryBlock) {
        let mut backlog = self.revertible_eviction_backlog.lock();
        self.cleanup_revertible_evictions(memory, &mut backlog);

        let mut dirty_pages = self.dirty_eviction_backlog.lock();
        free_pages(memory, &mut dirty_pages);
    }

    fn cleanup_revertible_evictions(
        &self,
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

        free_pages(memory, &mut backlog.pages_to_evict);
    }
}

fn free_pages(memory: &VirtualMemoryBlock, permits: &mut VecDeque<PageFreePermit>) {
    let pop_n = cmp::min(permits.len(), 100);
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
