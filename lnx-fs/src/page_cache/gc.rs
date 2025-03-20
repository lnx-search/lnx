use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::mem;
use std::ops::Range;
use std::panic::{AssertUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use futures::channel::oneshot;
use parking_lot::{Condvar, Mutex};
use smallvec::SmallVec;

type TriggerCallback = Box<dyn Fn() -> bool + Send>;
type FileIdAndGenerationPair = (u64, u64);

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
struct TriggerKey {
    /// The ID of the file the trigger belongs to.
    file_id: u64,
    /// A marker that requires all generations before this ID
    /// to be dead before the trigger can be called.
    generation_id: u64,
}

/// The minimum number of dead generations to accumulate before
/// the GC considers purging data.
///
/// This is set to avoid constant re-allocations within the GC and
/// needless additional scanning.
const REMOVED_GENERATIONS_THRESHOLD: usize = 1_000;
static GC: GCState = GCState::new();

/// Marks a given generation as dead for the given file.
///
/// This method _may_ panic if the GC thread has yet to be started and attempting
/// to create a new GC thread errors.
pub(crate) fn mark_dead_generation(file_id: u64, generation_id: u64) {
    let event = GCEvent::GenerationDead {
        file_id,
        generation_id,
    };
    GC.send(event);
}

/// Adds a new generation to be tracked by the GC.
///
/// This method _may_ panic if the GC thread has yet to be started and attempting
/// to create a new GC thread errors.
pub(crate) fn track_generation(file_id: u64, generation_id: u64) {
    let event = GCEvent::RegisterGeneration {
        file_id,
        generation_id,
    };
    GC.send(event);
}

/// Register a new trigger when all generations upto the provided point are dead.
///
/// This method _may_ panic if the GC thread has yet to be started and attempting
/// to create a new GC thread errors.
pub(crate) fn register_trigger<CB>(
    file_id: u64,
    trigger_once_checkpoint_at: u64,
    callback: CB,
) where
    CB: Fn() -> bool + Send + 'static,
{
    let callback = Box::new(callback) as TriggerCallback;
    let event = GCEvent::RegisterTrigger {
        file_id,
        trigger_once_checkpoint_at,
        callback,
    };
    GC.send(event);
}

/// Forces the GC to run a collection cycle.
pub(crate) fn force_collection() -> impl Future<Output = ()> + Send + 'static {
    let (tx, rx) = oneshot::channel();
    GC.send(GCEvent::ForceCollection { signal: tx });
    async move { let _ = rx.await; }
}

#[derive(Default)]
/// A background actor that incrementally cleans up free pages, we do this in a dedicated thread
/// to minimise the overhead for lnx's application.
struct CacheGCActor {
    /// The guard marks the GC as dead when dropped.
    guard: ThreadLiveGuard,
    /// Triggers still waiting for generations to be cleaned up before triggering.
    triggers: BTreeMap<TriggerKey, TriggerCallback>,
    /// A global tree of all the currently active generations.
    active_generations: BTreeSet<TriggerKey>,
    /// The number of dead generations accumulated.
    num_dead_generations: usize,
}

impl CacheGCActor {
    fn run(mut self) {
        tracing::info!("GC actor is running");

        loop {
            // Wait for new event triggers
            GC.wake.wait(&mut GC.wake_mutex.lock());

            self.drain_events();

            self.consider_gc_options();
        }
    }

    /// Pulls new events from the GC queue until it is empty.
    fn drain_events(&mut self) {
        while let Some(event) = GC.pending_events.pop() {
            self.handle_event(event);
        }
    }

    /// process a new GC event.
    fn handle_event(&mut self, event: GCEvent) {
        match event {
            GCEvent::GenerationDead {
                file_id,
                generation_id,
            } => {
                let key = TriggerKey {
                    file_id,
                    generation_id,
                };

                let did_remove = self.active_generations.remove(&key);
                self.num_dead_generations += did_remove as usize;
            },
            GCEvent::RegisterGeneration {
                file_id,
                generation_id,
            } => {
                let key = TriggerKey {
                    file_id,
                    generation_id,
                };

                self.active_generations.insert(key);
            },
            GCEvent::RegisterTrigger {
                file_id,
                trigger_once_checkpoint_at,
                callback,
            } => {
                let key = TriggerKey {
                    file_id,
                    generation_id: trigger_once_checkpoint_at,
                };

                self.triggers.insert(key, callback);
            },
            GCEvent::ForceCollection { signal } => {
                self.run_gc_cycle();
                let _ = signal.send(());
            }
        }
    }

    /// Checks currently waiting triggers to see if triggers can be called or not.
    ///
    /// This will not run if the minimum number of dead generations have been met.
    fn consider_gc_options(&mut self) {
        if self.num_dead_generations < REMOVED_GENERATIONS_THRESHOLD {
            return;
        }

        self.run_gc_cycle();
    }
    
    fn run_gc_cycle(&mut self) {
        tracing::trace!("dead generation threshold met, purging pages");

        let num_evicted_triggers = 0;
        let start = Instant::now();

        self.collect_dead_pages();

        self.num_dead_generations = 0;

        if num_evicted_triggers > 0 {
            let elapsed = start.elapsed();
            tracing::debug!(
                elapsed = ?elapsed,
                num_evicted_triggers = num_evicted_triggers,
                "gc completed eviction",
            )
        }
    }

    fn collect_dead_pages(&mut self) {
        let mut current_triggers = mem::take(&mut self.triggers).into_iter().peekable();

        while let Some((key, trigger)) = current_triggers.next() {
            let start = TriggerKey {
                file_id: key.file_id,
                generation_id: 0,
            };
            let end = key;

            let can_trigger = self.active_generations.range(start..end).next().is_none();

            // Advance the iterator until we encounter a new file id.
            if !can_trigger {
                while let Some(entry) = current_triggers
                    .next_if(|(next_key, _)| next_key.file_id == key.file_id)
                {
                    self.triggers.insert(entry.0, entry.1);
                }
                continue;
            }

            eprintln!("freeing {key:?}");
            let wrapped_trigger = AssertUnwindSafe(&trigger);
            let did_complete = std::panic::catch_unwind(wrapped_trigger)
                .map_err(|err| {
                    handle_trigger_panic(key.file_id, key.generation_id, err);
                })
                .unwrap_or_default();

            if !did_complete {
                self.triggers.insert(key, trigger);
            }
        }
    }
}

/// An event for the GC to process.
enum GCEvent {
    /// A generation reference has been dropped because
    /// it no longer has any strong ref counts, we might be able to free
    /// some pending pages.
    GenerationDead {
        /// The ID of the file.
        file_id: u64,
        /// The generation ID.
        generation_id: u64,
    },
    /// Adds a new generation to track.
    ///
    /// This is used to keep tabs on what generations currently have readers and are preventing
    /// pages being freed.
    RegisterGeneration {
        /// The ID of the file.
        file_id: u64,
        /// The generation ID.
        generation_id: u64,
    },
    /// Adds a new trigger to be called once the generation conditions are met.
    ///
    /// This is used to properly free memory pages once the generations where the page
    /// _could_ be accessed are no longer active.
    RegisterTrigger {
        /// The ID of the file.
        file_id: u64,
        /// The of generation IDs that need to not exist in order        
        /// for this trigger to be called.
        trigger_once_checkpoint_at: u64,
        /// The callback to call once the condition is met.
        callback: TriggerCallback,
    },
    /// Forcefully triggers a GC collection cycle
    ForceCollection {
        signal: oneshot::Sender<()>
    },
}

struct GCState {
    wake: Condvar,
    wake_mutex: Mutex<()>,
    is_live: AtomicBool,
    pending_events: crossbeam_queue::SegQueue<GCEvent>,
}

impl GCState {
    const fn new() -> Self {
        Self {
            wake: Condvar::new(),
            wake_mutex: Mutex::new(()),
            is_live: AtomicBool::new(false),
            pending_events: crossbeam_queue::SegQueue::new(),
        }
    }

    fn send(&self, event: GCEvent) {
        if !self.is_live.load(Ordering::Relaxed) {
            self.spawn_gc();
        }

        self.pending_events.push(event);
        self.wake.notify_one();
    }

    fn spawn_gc(&self) {
        let actor = CacheGCActor::default();

        std::thread::Builder::new()
            .name("fscache-gc".to_string())
            .spawn(move || actor.run())
            .expect("Spawn GC thread worker");
    }
}

#[derive(Default)]
struct ThreadLiveGuard;

impl Drop for ThreadLiveGuard {
    fn drop(&mut self) {
        GC.is_live.store(false, Ordering::Relaxed);
    }
}

/// Log when a trigger panics.
fn handle_trigger_panic(file_id: u64, generation: u64, error: Box<dyn Any>) {
    if let Some(msg) = error.downcast_ref::<String>() {
        tracing::error!(
            file_id = file_id,
            generation_target = generation,
            error = %msg,
            "gc callback panicked, memory may leak",
        );
    } else if let Some(msg) = error.downcast_ref::<&'static str>() {
        tracing::error!(
            file_id = file_id,
            generation_target = generation,
            error = %msg,
            "gc callback panicked, memory may leak"
        );
    } else {
        tracing::error!(
            file_id = file_id,
            generation_target = generation,
            "gc callback panicked with unknown error, memory may leak",
        );
    }
}
