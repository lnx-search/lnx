use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use parking_lot::Mutex;

const TICKETS_PER_GENERATION: u64 = 256;

/// The generation handler keeps track of active references
/// to pages over time.
///
/// The page cache uses a generational GC-like system.
///
/// Every operation increments a counter which we call a "ticket", then when we want to free a page,
/// we first increment the counter then wait until all operations that came _before_ that
/// operation have completed and ended.
///
/// Every 256 operations, we create a new generation which is a ref-count of the last 256 tickets.
/// Once a generation ref-count has dropped to zero, we advance our `oldest_alive_ticket` by 256 operations.
///
/// This method is designed to minimise contention and additional cleanup cycles while still
/// having relatively low memory overhead.
pub(super) struct GenerationTicketMachine {
    /// The monotonic ticket ID counter.
    ///
    /// This increments with every operation performed.
    ticket_counter: AtomicU64,
    /// The current generation in use for new tickets.
    ///
    /// Since this uses a [MiniArcSwap] tickets _might_ be put with an
    /// older generation, but this is fine as we still uphold
    /// the guarantee that older tickets are dropped.
    active_generation: ArcSwap<GenerationEntry>,
    /// State shared with [TicketGuard]s so they can automatically
    /// advance the generation state when dropped.
    shared_state: Arc<SharedState>,
}

impl Default for GenerationTicketMachine {
    fn default() -> Self {
        let shared_state = Arc::new(SharedState {
            oldest_alive_ticket: AtomicU64::new(0),
            generation: Mutex::new(GenerationState {
                next_generation_id: 0,
                active_generations: std::ptr::null_mut(),
            }),
        });

        // This entry is immediately replaced, always.
        let base_entry = GenerationEntry {
            generation_id: 0,
            shared_state: shared_state.clone(),
            ptr_chain: Box::into_raw(Box::new(PtrChain::null())),
        };

        Self {
            ticket_counter: AtomicU64::new(0),
            active_generation: ArcSwap::from_pointee(base_entry),
            shared_state,
        }
    }
}

impl GenerationTicketMachine {
    /// Returns the oldest alive ticket still in use.
    ///
    /// It can be assumed that any ticket older than this value
    /// has now been dropped.
    pub(super) fn oldest_alive_ticket(&self) -> u64 {
        self.shared_state.oldest_alive_ticket()
    }

    /// Increment the ticket counter but do not return a [TicketGuard].
    pub(super) fn increment_ticket_id(&self) -> u64 {
        let ticket_id = self.ticket_counter.fetch_add(1, Ordering::Relaxed);

        if ticket_id % TICKETS_PER_GENERATION == 0 {
            let generation = self.shared_state.replace_generation();
            self.active_generation.store(generation);
        }

        ticket_id
    }

    /// Increments the ticket operation counter and returns a [TicketGuard]
    /// which will ensure the oldest ticket counter is not advanced until
    /// this guard is dropped.
    pub(super) fn get_next_ticket(&self) -> TicketGuard {
        self.increment_ticket_id();
        let generation = self.active_generation.load_full();
        TicketGuard { generation }
    }
}

/// A guard that represents a single operation and its lifecycle.
///
/// This is used to ensure mutations to pages are not applied until
/// all readers still accessing the page have been dropped.
pub(super) struct TicketGuard {
    generation: Arc<GenerationEntry>,
}

impl TicketGuard {
    /// Clones `self` for use in reads.
    ///
    /// This is a separate method to prevent accidental misuse.
    pub(super) fn clone_for_read(&self) -> Self {
        Self {
            generation: self.generation.clone(),
        }
    }
}

struct SharedState {
    /// The oldest ticket ID still alive.
    ///
    /// This is an estimated value based on the generations currently alive.
    /// However, this value cannot lead to false-negatives, meaning newer
    /// ticket IDs may not be alive, but it is guaranteed that all tickets
    /// older than this ID are no longer alive.
    oldest_alive_ticket: AtomicU64,
    /// Generation state used for tracking ticket lifecycles.
    generation: Mutex<GenerationState>,
}

impl SharedState {
    fn oldest_alive_ticket(&self) -> u64 {
        self.oldest_alive_ticket.load(Ordering::Relaxed)
    }

    fn set_oldest_alive_ticket(&self, ticket: u64) {
        self.oldest_alive_ticket.store(ticket, Ordering::Relaxed);
    }

    fn replace_generation(self: &Arc<Self>) -> Arc<GenerationEntry> {
        let shared_state = self.clone();
        let mut lock = self.generation.lock();

        let generation_id = lock.next_generation_id();
        let entry = GenerationEntry {
            generation_id,
            shared_state,
            ptr_chain: Box::into_raw(Box::new(PtrChain::null())),
        };

        lock.push_active_generation(entry)
    }
}

struct GenerationState {
    next_generation_id: u64,
    active_generations: *mut GenerationEntry,
}

impl GenerationState {
    fn push_active_generation(
        &mut self,
        entry: GenerationEntry,
    ) -> Arc<GenerationEntry> {
        let mut entry = Arc::new(entry);

        let entry_ptr = Arc::get_mut(&mut entry).unwrap();

        // Connect the new head of the list with the old head.
        if !self.active_generations.is_null() {
            unsafe {
                (*entry_ptr.ptr_chain).next = self.active_generations;
                (*(*self.active_generations).ptr_chain).prev =
                    entry_ptr as *mut GenerationEntry
            };
        }

        self.active_generations = entry_ptr as *mut GenerationEntry;

        entry
    }

    fn next_generation_id(&mut self) -> u64 {
        let generation_id = self.next_generation_id;
        self.next_generation_id += 1;
        generation_id
    }
}

unsafe impl Send for GenerationState {}
unsafe impl Sync for GenerationState {}

/// A doubly-linked list of active generations.
struct GenerationEntry {
    generation_id: u64,
    shared_state: Arc<SharedState>,
    ptr_chain: *mut PtrChain,
}

unsafe impl Send for GenerationEntry {}
unsafe impl Sync for GenerationEntry {}

impl Drop for GenerationEntry {
    fn drop(&mut self) {
        if self.ptr_chain.is_null() {
            return;
        }

        // We have to acquire the lock guard first in order to
        // safely mutate the other generation entries.
        //
        // Once we have this lock, we can be assured that no other
        // concurrent accesses are going on, which also ensures that it
        // is safe to read out `next` and `prev` pointers.
        let guard = self.shared_state.generation.lock();

        // SAFETY: We hold the generation lock which ensures no other accesses are happening.
        let maybe_oldest_ticket = unsafe { PtrChain::detach_from_chain(self.ptr_chain) };
        if let Some(oldest_ticket) = maybe_oldest_ticket {
            self.shared_state.set_oldest_alive_ticket(oldest_ticket);
        }

        drop(guard);

        let ptr = mem::replace(&mut self.ptr_chain, std::ptr::null_mut());
        drop(unsafe { Box::from_raw(ptr) });
    }
}

struct PtrChain {
    next: *mut GenerationEntry,
    prev: *mut GenerationEntry,
}

impl PtrChain {
    fn null() -> Self {
        Self {
            next: std::ptr::null_mut(),
            prev: std::ptr::null_mut(),
        }
    }

    /// Detached `this` from the linked list chain.
    ///
    /// # Safety
    /// Care must be taken to ensure no other callers are reading or accessing
    /// `self` or its siblings.
    unsafe fn detach_from_chain(this: *mut Self) -> Option<u64> {
        if this.is_null() {
            return None;
        }

        let this = unsafe { &mut *this };

        if !this.next.is_null() {
            unsafe { (*(*this.next).ptr_chain).prev = this.prev };
        }

        if !this.prev.is_null() {
            unsafe { (*(*this.prev).ptr_chain).next = this.next };
        }

        let mut oldest_ticket = None;

        // If we are the end of the list, then we can safely
        // work out the oldest alive ticket.
        if this.next.is_null() && !this.prev.is_null() {
            let oldest_generation = unsafe { (*this.prev).generation_id };
            oldest_ticket = Some(oldest_generation * TICKETS_PER_GENERATION);
        }

        this.next = std::ptr::null_mut();
        this.prev = std::ptr::null_mut();

        oldest_ticket
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_ticket_machine_increment() {
        let machine = GenerationTicketMachine::default();

        assert_eq!(machine.increment_ticket_id(), 0);
        assert_eq!(machine.increment_ticket_id(), 1);
        assert_eq!(machine.increment_ticket_id(), 2);

        assert_eq!(machine.oldest_alive_ticket(), 0);

        for _ in 0..256 {
            machine.increment_ticket_id();
        }
        assert_eq!(machine.oldest_alive_ticket(), 256);

        for _ in 0..256 {
            machine.increment_ticket_id();
        }
        assert_eq!(machine.oldest_alive_ticket(), 512);
    }

    #[test]
    fn test_threaded_ticket_machine_increment() {
        let machine = Arc::new(GenerationTicketMachine::default());

        let handle1 = std::thread::spawn({
            let machine = machine.clone();
            move || {
                for _ in 0..1024 {
                    machine.increment_ticket_id();
                }
            }
        });

        let handle2 = std::thread::spawn({
            let machine = machine.clone();
            move || {
                for _ in 0..1024 {
                    machine.increment_ticket_id();
                }
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(machine.oldest_alive_ticket(), 1792);
    }

    #[test]
    fn test_ticket_guard_prevent_oldest_advancing() {
        let machine = GenerationTicketMachine::default();
        assert_eq!(machine.increment_ticket_id(), 0);

        let guard = machine.get_next_ticket();

        for _ in 0..256 {
            machine.increment_ticket_id();
        }
        assert_eq!(machine.oldest_alive_ticket(), 0);

        drop(guard);
        assert_eq!(machine.oldest_alive_ticket(), 256);
    }

    #[test]
    fn test_threaded_ticket_guard_prevent_oldest_advancing() {
        let machine = Arc::new(GenerationTicketMachine::default());
        assert_eq!(machine.increment_ticket_id(), 0);

        let guard = machine.get_next_ticket();

        let handle1 = std::thread::spawn({
            let machine = machine.clone();
            move || {
                for _ in 0..1024 {
                    machine.increment_ticket_id();
                }
            }
        });

        let handle2 = std::thread::spawn({
            let machine = machine.clone();
            move || {
                for _ in 0..1024 {
                    machine.increment_ticket_id();
                }
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
        assert_eq!(machine.oldest_alive_ticket(), 0);

        drop(guard);
        assert_eq!(machine.oldest_alive_ticket(), 2048);
    }
}
