use std::sync::Arc;
use parking_lot::Mutex;
use sqlx::Transaction;
use tracing::{error, instrument, trace};

use super::{Cache, FileUrl, Metastore, MetastoreEntry, MetastoreError, TabletId};
use crate::FileMetadata;

/// A metastore operation that allows applying multiple operations
/// in bulk as part of a single state lock operation.
pub struct BulkMetastoreModifyOperation<'a> {
    pub(super) metastore: &'a Metastore,
    pub(super) mutations: Vec<StateMutationOp>,
}

impl<'a> BulkMetastoreModifyOperation<'a> {
    #[instrument(skip(self))]
    /// Add a file to be tracked in the metastore.
    pub(crate) fn add_file(
        &mut self,
        url: FileUrl,
        metadata: FileMetadata,
    ) {
        let entry = MetastoreEntry {
            url,
            metadata,
        };
        self.mutations.push(StateMutationOp::Add { entry });
    }

    #[instrument(skip(self))]
    /// Remove a file from being tracked in the metastore.
    pub(crate) fn remove_file(
        &mut self,
        path: &str,
    ) {
        self.mutations.push(StateMutationOp::Remove {
            path: path.to_string(),
        });
    }

    #[allow(unused)] // TODO: Add GC system
    /// Delete all files for a given tablet.
    pub(crate) fn delete_tablet_files(
        &mut self,
        tablet: TabletId,
    ) -> Vec<String> {
        let entries = self.metastore
            .list_files_in_tablet(tablet)
            .into_iter()
            .map(|e| e.url.path)
            .collect::<Vec<_>>();

        self.mutations
            .extend(
                entries
                    .iter()
                    .cloned()
                    .map(|path| StateMutationOp::Remove { path })
            );

        entries
    }

    #[instrument(skip_all)]
    /// Commits the currently pending bulk operations.
    pub(crate) fn commit(self) {
        let mut lock = self.metastore.write_state.lock();
        for op in self.mutations {
            match op {
                StateMutationOp::Add { entry } => {
                    lock.update(entry.url.path.clone(), entry);
                },
                StateMutationOp::Remove { path } => {
                    lock.remove_entry(path);
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
        drop(self);  // We just ignore the state we've collected.
    }
}

pub(super) enum StateMutationOp {
    Add {
        entry: MetastoreEntry,
    },
    Remove {
        path: String,
    },
}
