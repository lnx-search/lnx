use std::sync::Arc;

use anyhow::{Error, Result};
use arc_swap::ArcSwap;
use hashbrown::HashMap;
use search_index::structures::IndexDeclaration;
pub use search_index::{structures, Index, QueryPayload, QueryResults, StorageBackend, DocumentId};

/// A manager around a set of indexes.
#[derive(Clone)]
pub struct Engine {
    indexes: Arc<ArcSwap<HashMap<String, Index>>>,
}

impl Engine {
    /// Creates a new unpopulated engine.
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(ArcSwap::from_pointee(HashMap::new())),
        }
    }

    /// Adds an index to the index from a given declaration.
    ///
    /// This duplicates the current indexes and swaps the clone, in general
    /// this is a very heavy operation and shouldn't be ran often / arbitrarily.
    pub async fn add_index(&self, index: &IndexDeclaration, override_if_exists: bool) -> Result<()> {
        let mut indexes;
        {
            let guard = self.indexes.load();
            indexes = guard.as_ref().clone();
        }

        if !override_if_exists & indexes.get(index.name()).is_some() {
            return Err(Error::msg("index already exists."))
        }

        let ctx = index.create_context()?;
        let name = ctx.name();
        let index = Index::create(ctx).await?;

        indexes.insert(name, index);
        self.indexes.store(Arc::new(indexes));

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
}
