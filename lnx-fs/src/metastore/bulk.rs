use sqlx::Transaction;
use tracing::{instrument, trace};

use super::{Cache, FileUrl, MetastoreError, TabletId};
use crate::FileMetadata;

/// A metastore operation that allows applying multiple operations
/// in bulk as part of a single atomic operation.
pub struct BulkMetastoreModifyOperation<'a> {
    pub(super) tx: Transaction<'a, sqlx::Sqlite>,
    pub(super) cache: &'a Cache,
    pub(super) mutations: Vec<CacheMutationOp>,
}

impl<'a> BulkMetastoreModifyOperation<'a> {
    #[instrument(skip(self))]
    /// Add a file to be tracked in the metastore.
    pub(crate) async fn add_file(
        &mut self,
        url: FileUrl,
        metadata: FileMetadata,
    ) -> Result<(), MetastoreError> {
        let extension = url.path.rsplit_once('.').map(|parts| parts.1);

        let query = r#"
            INSERT INTO lnx__active_files (
                path,
                extension,
                tablet_id,
                range_start,
                range_end,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (path) 
            DO UPDATE SET             
                tablet_id = excluded.tablet_id,
                range_start = excluded.range_start,
                range_end = excluded.range_end,
                created_at = excluded.created_at;
        "#;

        sqlx::query(query)
            .bind(&url.path)
            .bind(extension)
            .bind(url.tablet_id.to_string())
            .bind(metadata.position.start as i64)
            .bind(metadata.position.end as i64)
            .bind(metadata.created_at as i64)
            .execute(&mut *self.tx)
            .await?;

        self.mutations.push(CacheMutationOp::Add { url, metadata });

        Ok(())
    }

    #[instrument(skip(self))]
    /// Remove a file from being tracked in the metastore.
    pub(crate) async fn remove_file(
        &mut self,
        path: &str,
    ) -> Result<(), MetastoreError> {
        let query = r#"
            DELETE FROM lnx__active_files WHERE path = ?;
        "#;

        sqlx::query(query).bind(path).execute(&mut *self.tx).await?;

        self.mutations.push(CacheMutationOp::Remove {
            path: path.to_string(),
        });

        Ok(())
    }

    #[instrument(skip(self))]
    /// Renames a file from one name to another name.
    ///
    /// This will implicitly overwrite any file which already exists at the given path.
    pub(crate) async fn rename_file(
        &mut self,
        from_path: &str,
        to_path: &str,
    ) -> Result<(), MetastoreError> {
        let extension = to_path.rsplit_once('.').map(|parts| parts.1);

        self.mutations.push(CacheMutationOp::Remove {
            path: from_path.to_string(),
        });

        // Remove the file at the destination if it exists.
        self.remove_file(to_path).await?;

        // Update the file to have the new path.
        let query = r#"
            UPDATE lnx__active_files 
            SET             
                path = ?,
                extension = ?
            WHERE path = ?;
        "#;

        sqlx::query(query)
            .bind(to_path)
            .bind(extension)
            .bind(from_path)
            .execute(&mut *self.tx)
            .await?;

        Ok(())
    }

    #[allow(unused)] // TODO: Add GC system
    /// Delete all files for a given tablet.
    pub(crate) async fn delete_tablet_files(
        &mut self,
        tablet: TabletId,
    ) -> Result<Vec<String>, MetastoreError> {
        let query = r#"
            DELETE FROM lnx__active_files 
            WHERE tablet_id = ? 
            RETURNING path;
        "#;

        let file_paths: Vec<String> = sqlx::query_scalar(query)
            .bind(tablet.to_string())
            .fetch_all(&mut *self.tx)
            .await?;

        self.mutations.push(CacheMutationOp::RemoveMany {
            paths: file_paths.clone(),
        });

        Ok(file_paths)
    }

    #[instrument(skip_all)]
    /// Commits the currently pending bulk operations.
    pub(crate) async fn commit(self) -> Result<(), MetastoreError> {
        let cache = self.cache;
        let tx = self.tx;
        tx.commit().await?;
        trace!("Metastore transaction commit OK");

        for op in self.mutations {
            match op {
                CacheMutationOp::Add { url, metadata } => {
                    cache.insert(url.path, (url.tablet_id, metadata));
                },
                CacheMutationOp::Remove { path } => {
                    cache.remove(&path);
                },
                CacheMutationOp::RemoveMany { paths } => {
                    for path in paths {
                        cache.remove(&path);
                    }
                },
            }
        }
        trace!("Metastore cache commit OK");

        Ok(())
    }

    #[instrument(skip_all)]
    /// Explicitly abort the bulk operation and rollback.
    ///
    /// If this is not called directly it will be aborted on drop.
    pub(crate) async fn rollback(self) -> Result<(), MetastoreError> {
        self.tx.rollback().await?;
        Ok(())
    }
}

pub(super) enum CacheMutationOp {
    Add {
        url: FileUrl,
        metadata: FileMetadata,
    },
    Remove {
        path: String,
    },
    RemoveMany {
        paths: Vec<String>,
    },
}
