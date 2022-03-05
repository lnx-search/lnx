use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use lnx_common::schema::{FieldName, Schema};
use lnx_utils::{FromBytes, ToBytes};
use crate::polling::PollingMode;
use crate::templates::doc_store::DocStore;
use crate::templates::meta_store::MetaStore;


#[derive(Clone)]
/// A Index's storage and configuration settings.
///
/// Additional settings can be added via the `store` and `load` method.
pub struct IndexStore {
    index_name: FieldName,
    schema: Schema,
    output_path: PathBuf,
    polling_mode: PollingMode,
    doc_store: Arc<dyn DocStore>,
    meta_store: Arc<dyn MetaStore>,
}

impl IndexStore {
    #[inline]
    pub fn polling_mode(&self) -> &PollingMode {
        &self.polling_mode
    }

    #[inline]
    pub fn file_path(&self) -> &Path {
        &self.output_path
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.index_name.as_str()
    }

    #[inline]
    pub fn docs(&self) -> &Arc<dyn DocStore> {
        &self.doc_store
    }

    #[inline]
    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.meta_store
    }

    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Updates the current settings for the given key.
    ///
    /// These changes are reflected in the database.
    #[instrument(name = "index-settings-store", skip(self, settings), fields(index_name = %self.name()))]
    pub async fn store<T: ToBytes>(&self, key: &str, settings: T) -> Result<()> {
        let instant = Instant::now();
        self.meta()
            .update_settings(key, settings.to_bytes()?)
            .await?;
        info!("Settings store took {:?}", instant.elapsed());

        Ok(())
    }

    /// Removes the current settings for the given key.
    ///
    /// These changes are reflected in the database.
    #[instrument(name = "index-settings-remove", skip(self), fields(index_name = %self.name()))]
    pub async fn remove(&self, key: &str) -> Result<()> {
        let instant = Instant::now();
        self.meta()
            .remove_settings(key)
            .await?;
        info!("Settings update took {:?}", instant.elapsed());

        Ok(())
    }

    /// Loads the latest current settings for the given key.
    ///
    /// These changes are reflected in the database.
    #[instrument(name = "index-settings-load", skip(self), fields(index_name = %self.name()))]
    pub async fn load<T: FromBytes>(&self, key: &str) -> Result<Option<T>> {
        let instant = Instant::now();
        let settings = self.meta().load_settings(key).await?;
        info!("Settings load took {:?}", instant.elapsed());

        settings
            .map(|buff| T::from_bytes(&buff).map_err(anyhow::Error::from))
            .transpose()
    }
}