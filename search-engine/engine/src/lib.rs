use std::sync::Arc;

use anyhow::{Error, Result};
use arc_swap::ArcSwap;
use hashbrown::HashMap;
use parking_lot::Mutex;
use search_index::structures::IndexDeclaration;
pub use search_index::{
    structures,
    DocumentId,
    Index,
    QueryPayload,
    QueryResults,
    StorageBackend,
};

/// A manager around a set of indexes.
#[derive(Clone)]
pub struct Engine {
    declarations: Arc<Mutex<HashMap<String, IndexDeclaration>>>,
    indexes: Arc<ArcSwap<HashMap<String, Index>>>,
}

impl Engine {
    /// Creates a new unpopulated engine.
    pub fn new() -> Self {
        Self {
            declarations: Arc::new(Mutex::new(HashMap::new())),
            indexes: Arc::new(ArcSwap::from_pointee(HashMap::new())),
        }
    }

    /// Adds an index to the index from a given declaration.
    ///
    /// This duplicates the current indexes and swaps the clone, in general
    /// this is a very heavy operation and shouldn't be ran often / arbitrarily.
    pub async fn add_index(
        &self,
        index: IndexDeclaration,
        override_if_exists: bool,
    ) -> Result<()> {
        let mut indexes;
        {
            let guard = self.indexes.load();
            indexes = guard.as_ref().clone();
        }

        if !override_if_exists & indexes.get(index.name()).is_some() {
            return Err(Error::msg("index already exists."));
        }

        // remove the index if it exists
        self.remove_index(index.name()).await?;

        let ctx = index.create_context()?;
        let name = ctx.name();
        let built_index = Index::create(ctx).await?;

        indexes.insert(name, built_index);
        self.indexes.store(Arc::new(indexes));

        {
            self.declarations
                .lock()
                .insert(index.name().to_string(), index);
        }

        Ok(())
    }

    /// Removes an index to the index from the engine with a given name.
    ///
    /// This internally calls `Index.destroy()` to cleanup writers.
    pub async fn remove_index(&self, name: &str) -> Result<()> {
        let indexes = {
            let indexes = self.indexes.load();

            let mut indexes = indexes.as_ref().clone();
            if let Some(old) = indexes.remove(name) {
                old.destroy().await?;
            };

            indexes
        };

        self.indexes.store(Arc::new(indexes));

        {
            self.declarations.lock().remove(name);
        }

        Ok(())
    }

    /// Gets an index from the engine with the a given name.
    ///
    /// An error will be returned if the index does not exist.
    pub fn get_index(&self, index: &str) -> Option<Index> {
        let guard = self.indexes.load();
        let index = guard.get(index)?;

        Some(index.clone())
    }

    pub fn get_all_indexes(&self) -> Vec<IndexDeclaration> {
        let guard = self.declarations.lock();
        guard.values().map(|v| v.clone()).collect()
    }

    pub async fn shutdown(&self) -> Result<()> {
        let guard = self.indexes.load();
        for (_, index) in guard.iter() {
            index.shutdown().await?;
        }

        Ok(())
    }
}
