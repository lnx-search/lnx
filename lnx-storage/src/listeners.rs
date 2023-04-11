use std::sync::Arc;

use datacake::crdt::Key;
use datacake::eventual_consistency::Document;
use hashbrown::HashMap;
use parking_lot::RwLock;
use smallvec::SmallVec;

type BoxedStorageListener = Arc<dyn StorageListener>;
type BoxedFragmentListener = Arc<dyn FragmentListener>;
type Listeners<L> = SmallVec<[L; 10]>;
type ListenersMap<L> = HashMap<String, Listeners<L>>;
type StorageListeners = ListenersMap<BoxedStorageListener>;
type FragmentsListeners = Listeners<BoxedFragmentListener>;

macro_rules! derive_fragment_triggers {
    ($slf:expr, $cb:ident => $($arg:expr $(,)?)*) => {{
        let slf = $slf.clone();
        lnx_executor::spawn_task(async move {
            let listeners = slf.fragments_listeners.read().clone();
            for listener in listeners.iter() {
                listener.$cb($($arg),*);
            }
        });
    }};
}

#[derive(Default, Clone)]
/// The listener manager that triggers callbacks when
/// events occur within the storage system.
pub struct ListenerManager {
    storage_listeners: Arc<RwLock<StorageListeners>>,
    fragments_listeners: Arc<RwLock<FragmentsListeners>>,
}

impl ListenerManager {
    /// Register a listener for storage events.
    ///
    /// No events are triggered when working on the fragment keyspace.
    pub fn register_storage_listener(
        &self,
        keyspace: &str,
        listeners: impl StorageListener,
    ) {
        self.storage_listeners
            .write()
            .entry(keyspace.to_owned())
            .or_default()
            .push(Arc::new(listeners))
    }

    /// Register a listener for fragment events.
    pub fn register_fragment_listener(&self, listeners: impl FragmentListener) {
        self.fragments_listeners.write().push(Arc::new(listeners))
    }

    /// Trigger the on_put event for any listeners.
    ///
    /// This runs listeners on the blocking runtime.
    pub(crate) fn trigger_on_put(&self, keyspace: &str, doc: Document) {
        let slf = self.clone();
        let keyspace = keyspace.to_owned();
        lnx_executor::spawn_task(async move {
            let listeners_map = slf.storage_listeners.read();
            if let Some(listeners) = listeners_map.get(&keyspace).cloned() {
                for listener in listeners {
                    listener.on_put(doc.clone());
                }
            }
        });
    }

    /// Trigger the on_del even for any listeners.
    ///
    /// This runs listeners on the blocking runtime.
    pub(crate) fn trigger_on_del(&self, keyspace: &str, doc_id: Key) {
        let slf = self.clone();
        let keyspace = keyspace.to_owned();
        lnx_executor::spawn_task(async move {
            let listeners_map = slf.storage_listeners.read();
            if let Some(listeners) = listeners_map.get(&keyspace).cloned() {
                for listener in listeners {
                    listener.on_del(doc_id);
                }
            }
        });
    }

    /// Trigger the fragment block flush event.
    pub(crate) fn trigger_fragment_block_flush(
        &self,
        fragment_id: u64,
        blocks: Vec<Document>,
    ) {
        derive_fragment_triggers!(self, on_block_flush => fragment_id, &blocks);
    }

    /// Trigger the fragment delete event.
    pub(crate) fn trigger_fragment_file_add(&self, fragment_id: u64, file_path: String) {
        derive_fragment_triggers!(self, on_file_add => fragment_id, &file_path);
    }

    /// Trigger the fragment seal event.
    pub(crate) fn trigger_fragment_seal(&self, fragment_id: u64) {
        derive_fragment_triggers!(self, on_seal => fragment_id);
    }

    /// Trigger the fragment ready ready event.
    pub(crate) fn trigger_fragment_read_ready(&self, fragment_id: u64) {
        derive_fragment_triggers!(self, on_read_ready => fragment_id);
    }

    /// Trigger the fragment delete event.
    pub(crate) fn trigger_fragment_delete(&self, fragment_id: u64) {
        derive_fragment_triggers!(self, on_delete => fragment_id);
    }
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

    /// Triggered when a file is added to a fragment.
    ///
    /// This is only triggered on local-fragments not remove
    /// fragments.
    fn on_file_add(&self, _fragment_id: u64, _file_path: &str) {}

    /// Triggered when a fragment is sealed.
    fn on_seal(&self, _fragment_id: u64) {}

    /// Triggered when a fragment is available to be read.
    fn on_read_ready(&self, _fragment_id: u64) {}

    /// Triggered when the fragment is marked for deletion.
    fn on_delete(&self, _fragment_id: u64) {}
}
