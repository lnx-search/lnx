use std::sync::Arc;

use datacake::crdt::Key;
use datacake::eventual_consistency::Document;
use hashbrown::HashMap;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use smallvec::SmallVec;

type BoxedStorageListener = Arc<dyn StorageListener>;
type BoxedFragmentListener = Arc<dyn FragmentListener>;
type Listeners<L> = SmallVec<[L; 10]>;
type ListenersMap<L> = HashMap<String, Listeners<L>>;

static STORAGE_LISTENERS: Lazy<RwLock<ListenersMap<BoxedStorageListener>>> =
    Lazy::new(Default::default);
static FRAGMENT_LISTENERS: Lazy<RwLock<Listeners<BoxedFragmentListener>>> =
    Lazy::new(Default::default);

/// Register a new listener for a given keyspace event(s).
pub fn register_storage_listener(keyspace: &str, listener: impl StorageListener) {
    let mut lock = STORAGE_LISTENERS.write();
    lock.entry(keyspace.to_owned())
        .or_default()
        .push(Arc::new(listener));
}

/// Register a new listener for a given fragment events.
pub fn register_fragment_listener(listener: impl FragmentListener) {
    let mut lock = FRAGMENT_LISTENERS.write();
    lock.push(Arc::new(listener));
}

#[deny(unused)]
/// Trigger the on_put event for any listeners.
///
/// This runs listeners on the blocking runtime.
pub(crate) fn trigger_on_put(keyspace: &str, doc: Document) {
    let keyspace = keyspace.to_owned();
    lnx_executor::spawn_task(async move {
        let listeners_map = STORAGE_LISTENERS.read();
        if let Some(listeners) = listeners_map.get(&keyspace).cloned() {
            for listener in listeners {
                listener.on_put(doc.clone());
            }
        }
    });
}

#[deny(unused)]
/// Trigger the on_del even for any listeners.
///
/// This runs listeners on the blocking runtime.
pub(crate) fn trigger_on_del(keyspace: &str, doc_id: Key) {
    let keyspace = keyspace.to_owned();
    lnx_executor::spawn_task(async move {
        let listeners_map = STORAGE_LISTENERS.read();
        if let Some(listeners) = listeners_map.get(&keyspace).cloned() {
            for listener in listeners {
                listener.on_del(doc_id);
            }
        }
    });
}

macro_rules! derive_fragment_triggers {
    ($cb:ident => $($arg:expr $(,)?)*) => {{
        lnx_executor::spawn_task(async move {
            let listeners = FRAGMENT_LISTENERS.read().clone();
            for listener in listeners.iter() {
                listener.$cb($($arg),*);
            }
        });
    }};
}

#[deny(unused)]
/// Trigger the fragment seal event.
pub(crate) fn trigger_fragment_seal(fragment_id: u64) {
    derive_fragment_triggers!(on_seal => fragment_id);
}

#[deny(unused)]
/// Trigger the fragment ready ready event.
pub(crate) fn trigger_fragment_read_ready(fragment_id: u64) {
    derive_fragment_triggers!(on_read_ready => fragment_id);
}

#[deny(unused)]
/// Trigger the fragment delete event.
pub(crate) fn trigger_fragment_delete(fragment_id: u64) {
    derive_fragment_triggers!(on_delete => fragment_id);
}

/// Listen for specific events on the replicated storage.
pub trait StorageListener: Send + Sync + 'static {
    /// Triggered when ever the keyspace for this listener observes
    /// an update.
    fn on_put(&self, doc: Document);

    /// Triggered when ever the keyspace for this listener observes
    /// a deletion of a document.
    fn on_del(&self, doc_id: Key);
}

/// Listen for specific events on the replicated storage.
pub trait FragmentListener: Send + Sync + 'static {
    /// Triggered when a set of blocks are added and flushed.
    fn on_block_flush(&self, _fragment_id: u64, _blocks: &[Document]) {}

    /// Triggered when a fragment is sealed.
    fn on_seal(&self, _fragment_id: u64) {}

    /// Triggered when a fragment is available to be read.
    fn on_read_ready(&self, _fragment_id: u64) {}

    /// Triggered when the fragment is marked for deletion.
    fn on_delete(&self, _fragment_id: u64) {}
}
