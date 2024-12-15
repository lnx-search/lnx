use tracing::{instrument, trace};

use super::{Metastore, MetastoreEntry, TabletId};
use crate::io::{EventData, FileEvent};
use crate::FileMetadata;

/// A metastore operation that allows applying multiple operations
/// in bulk as part of a single state lock operation.
pub struct BulkMetastoreModifyOperation<'a> {
    pub(super) metastore: &'a Metastore,
    pub(super) mutations: Vec<(TabletId, FileEvent)>,
}

impl<'a> BulkMetastoreModifyOperation<'a> {
    #[instrument(skip(self))]
    /// Add a file to be tracked in the metastore.
    pub(crate) fn add_event(&mut self, tablet_id: TabletId, event: FileEvent) {
        self.mutations.push((tablet_id, event));
    }

    #[instrument(skip_all)]
    /// Commits the currently pending bulk operations.
    pub(crate) fn commit(self) {
        let mut lock = self.metastore.write_state.lock();
        for (tablet_id, event) in self.mutations {
            match event.data {
                EventData::Create {
                    file_path,
                    data_range,
                } => {
                    lock.insert(
                        file_path.clone(),
                        MetastoreEntry {
                            path: file_path,
                            metadata: FileMetadata {
                                tablet_id,
                                position: data_range,
                                created_at: event.created_at,
                            },
                        },
                    );
                },
                EventData::Delete { file_path } => {
                    lock.remove_entry(file_path);
                },
                EventData::Rename { from_path, to_path } => {
                    if let Some(mut entry) = lock.get_one(&from_path).map(|e| e.clone())
                    {
                        entry.path = to_path.clone();
                        lock.insert(to_path, entry);
                    }
                },
            }
        }
        lock.publish();
        trace!("Metastore memory commit OK");
    }

    #[instrument(skip_all)]
    /// Explicitly abort the bulk operation and rollback.
    ///
    /// If this is not called directly it will be aborted on drop.
    pub(crate) fn rollback(self) {
        drop(self); // We just ignore the state we've collected.
    }
}
