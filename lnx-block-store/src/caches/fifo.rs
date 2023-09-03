use std::collections::VecDeque;

use ahash::RandomState;
use lnx_document::DocBlockReader;
use parking_lot::Mutex;

/// A simple FIFO based cache.
pub struct FifoCache {
    /// The capacity of the cache.
    capacity: u64,
    /// A queue of block IDs and their size.
    state: Mutex<State>,
    /// The map of blocks.
    blocks: dashmap::DashMap<u64, DocBlockReader, RandomState>,
}

impl super::Cache for FifoCache {
    fn with_capacity(capacity: u64) -> Self {
        let state = Mutex::new(State::default());
        let blocks = dashmap::DashMap::with_capacity_and_hasher(5, RandomState::new());

        Self {
            capacity,
            state,
            blocks,
        }
    }

    fn put(&self, block_id: u64, reader: DocBlockReader) {
        let block_size = reader.buffer_usage() as u32;

        // If we can never fit the reader in the capacity then we don't try.
        if block_size as u64 > self.capacity {
            return;
        }

        let mut lock = self.state.lock();

        let potential_usage = lock.usage + block_size as u64;

        // Evict some of the oldest entries from the cache.
        if potential_usage > self.capacity {
            let mut evicted_size = 0;

            while let Some((block_id, size)) = lock.queue.pop_back() {
                evicted_size += size as u64;

                self.blocks.remove(&block_id);

                if evicted_size >= block_size as u64 {
                    break;
                }
            }

            lock.usage -= evicted_size;

            if evicted_size < block_size as u64 {
                return;
            }
        }

        // Insert the entry to the head of queue.
        lock.queue.push_front((block_id, block_size));
        lock.usage += block_size as u64;
        self.blocks.insert(block_id, reader);
    }

    fn get(&self, block_id: u64) -> Option<DocBlockReader> {
        self.blocks.get(&block_id).map(|v| v.value().clone())
    }
}

struct State {
    /// The current usage of the cache.
    usage: u64,
    /// A queue of block IDs and their size.
    queue: VecDeque<(u64, u32)>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            usage: 0,
            queue: VecDeque::with_capacity(5),
        }
    }
}
