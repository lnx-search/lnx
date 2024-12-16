use std::collections::BTreeMap;

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
    /// The number of changes currently in the transaction.
    pub fn num_changes(&self) -> usize {
        self.mutations.len()
    }

    #[instrument(skip(self))]
    /// Add a file to be tracked in the metastore.
    pub(crate) fn add_event(&mut self, tablet_id: TabletId, event: FileEvent) {
        self.mutations.push((tablet_id, event));
    }

    #[instrument(skip_all)]
    /// Commits the currently pending bulk operations.
    pub(crate) fn commit(self) {
        let mut local_inserts = BTreeMap::new();

        let mut lock = self.metastore.write_state.lock();
        for (tablet_id, event) in self.mutations {
            match event.data {
                EventData::Create {
                    file_path,
                    data_range,
                } => {
                    let entry = MetastoreEntry {
                        path: file_path.clone(),
                        metadata: FileMetadata {
                            tablet_id,
                            position: data_range,
                            created_at: event.created_at,
                        },
                    };

                    // A local copy for performing intra-transaction ops.
                    local_inserts.insert(file_path.clone(), entry.clone());
                    lock.update(file_path, entry);
                },
                EventData::Delete { file_path } => {
                    local_inserts.remove(&file_path);
                    lock.remove_entry(file_path);
                },
                EventData::Rename { from_path, to_path } => {
                    let maybe_existing = local_inserts
                        .remove(&from_path)
                        .or_else(|| lock.get_one(&from_path).map(|e| e.clone()));

                    lock.remove_entry(from_path);

                    if let Some(mut entry) = maybe_existing {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bulk_mutate_basic_ops() {
        let metastore = Metastore::connect(":memory:").await.unwrap();
        let mut bulk = BulkMetastoreModifyOperation {
            metastore: &metastore,
            mutations: Vec::new(),
        };

        bulk.add_event(
            TabletId::new(),
            FileEvent::create(None, "example1.txt".into(), 0..123),
        );
        bulk.add_event(
            TabletId::new(),
            FileEvent::delete(None, "example2.txt".into()),
        );
        bulk.add_event(
            TabletId::new(),
            FileEvent::rename(None, "example3.txt".into(), "example4.txt".into()),
        );
        assert_eq!(bulk.mutations.len(), 3);
        bulk.commit();

        assert!(metastore.exists("example1.txt"));
        assert!(!metastore.exists("example2.txt"));
        assert!(!metastore.exists("example3.txt"));
        assert!(!metastore.exists("example4.txt"));
    }

    #[tokio::test]
    async fn test_bulk_mutate_create_delete_ordering() {
        let metastore = Metastore::connect(":memory:").await.unwrap();
        let mut bulk = BulkMetastoreModifyOperation {
            metastore: &metastore,
            mutations: Vec::new(),
        };

        bulk.add_event(
            TabletId::new(),
            FileEvent::create(None, "example1.txt".into(), 0..123),
        );
        bulk.add_event(
            TabletId::new(),
            FileEvent::delete(None, "example1.txt".into()),
        );
        assert_eq!(bulk.mutations.len(), 2);
        bulk.commit();

        assert!(!metastore.exists("example1.txt"));
    }

    #[tokio::test]
    async fn test_bulk_mutate_create_rename_ordering() {
        let metastore = Metastore::connect(":memory:").await.unwrap();
        let mut bulk = BulkMetastoreModifyOperation {
            metastore: &metastore,
            mutations: Vec::new(),
        };

        bulk.add_event(
            TabletId::new(),
            FileEvent::create(None, "example1.txt".into(), 0..123),
        );
        bulk.add_event(
            TabletId::new(),
            FileEvent::rename(None, "example1.txt".into(), "example2.txt".into()),
        );
        assert_eq!(bulk.mutations.len(), 2);
        bulk.commit();

        assert!(!metastore.exists("example1.txt"));
        assert!(metastore.exists("example2.txt"));
    }

    #[tokio::test]
    async fn test_bulk_mutate_after_commit() {
        let metastore = Metastore::connect(":memory:").await.unwrap();

        let mut bulk = BulkMetastoreModifyOperation {
            metastore: &metastore,
            mutations: Vec::new(),
        };
        bulk.add_event(
            TabletId::new(),
            FileEvent::create(None, "example1.txt".into(), 0..123),
        );
        assert_eq!(bulk.mutations.len(), 1);
        bulk.commit();
        let mut bulk = BulkMetastoreModifyOperation {
            metastore: &metastore,
            mutations: Vec::new(),
        };
        bulk.add_event(
            TabletId::new(),
            FileEvent::delete(None, "example1.txt".into()),
        );
        assert_eq!(bulk.mutations.len(), 1);
        bulk.commit();
        assert!(!metastore.exists("example1.txt"));

        let mut bulk = BulkMetastoreModifyOperation {
            metastore: &metastore,
            mutations: Vec::new(),
        };
        bulk.add_event(
            TabletId::new(),
            FileEvent::create(None, "example1.txt".into(), 0..123),
        );
        assert_eq!(bulk.mutations.len(), 1);
        bulk.commit();
        let mut bulk = BulkMetastoreModifyOperation {
            metastore: &metastore,
            mutations: Vec::new(),
        };
        bulk.add_event(
            TabletId::new(),
            FileEvent::rename(None, "example1.txt".into(), "example2.txt".into()),
        );
        assert_eq!(bulk.mutations.len(), 1);
        bulk.commit();
        assert!(!metastore.exists("example1.txt"));
        assert!(metastore.exists("example2.txt"));
    }
}
