use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use lnx_common::index::context::IndexContext;
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
    ctx: IndexContext,
    polling_mode: PollingMode,
    doc_store: Arc<dyn DocStore>,
    meta_store: Arc<dyn MetaStore>,
}

impl IndexStore {
    pub fn new(
        ctx: IndexContext,
        polling_mode: PollingMode,
        doc_store: Arc<dyn DocStore>,
        meta_store: Arc<dyn MetaStore>
    ) -> Self {
        Self {
            ctx,
            polling_mode,
            doc_store,
            meta_store,
        }
    }

    #[inline]
    pub fn polling_mode(&self) -> PollingMode {
        self.polling_mode
    }

    #[inline]
    pub fn ctx(&self) -> &IndexContext {
        &self.ctx
    }

    #[inline]
    pub fn docs(&self) -> &Arc<dyn DocStore> {
        &self.doc_store
    }

    #[inline]
    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.meta_store
    }

    /// Updates the current settings for the given key.
    ///
    /// These changes are reflected in the database.
    #[instrument(name = "index-settings-store", skip(self, settings))]
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
    #[instrument(name = "index-settings-remove", skip(self))]
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
    #[instrument(name = "index-settings-load", skip(self))]
    pub async fn load<T: FromBytes>(&self, key: &str) -> Result<Option<T>> {
        let instant = Instant::now();
        let settings = self.meta().load_settings(key).await?;
        info!("Settings load took {:?}", instant.elapsed());

        settings
            .map(|buff| T::from_bytes(&buff).map_err(anyhow::Error::from))
            .transpose()
    }
}