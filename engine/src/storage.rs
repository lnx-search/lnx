use anyhow::Result;
use tokio::fs;

use crate::structures::{IndexDeclaration, LoadedIndex};


/// Manages a given directory which acts as the storage location
/// for indexes for a given engine.
pub(crate) struct StorageManager {
    dir: String,
}

impl StorageManager {
    /// Creates a new storage manage.
    pub(crate) fn new(dir: String) -> Self {
        Self {
            dir,
        }
    }

    /// Loads all indexes from the given directory.
    ///
    /// This will error if the files inside the directory are not all
    /// valid index files.
    pub(crate) async fn load_all(&self) -> Result<Vec<LoadedIndex>> {
        let mut indexes = vec![];
        let mut read_dir = fs::read_dir(&self.dir).await?;

        while let Some(file) = read_dir.next_entry().await? {
            info!("loading index from directory {} with path {:?}", &self.dir, file.path());
            let data = fs::read(file.path()).await?;

            let loader = serde_json::from_slice::<IndexDeclaration>(&data)?;
            let loaded = loader.into_schema();

            indexes.push(loaded);
        }

        Ok(indexes)
    }

    /// Gets a loaded index from the given path.
    pub(crate) async fn get_index_meta(&self, name: &str) -> Result<LoadedIndex> {
        let path = format!("{}/{}", &self.dir, name);
        let data = fs::read(path).await?;

        info!("loading index metadata for index {}", &name);
        let loader = serde_json::from_slice::<IndexDeclaration>(&data)?;
        Ok(loader.into_schema())
    }

    /// Stores a given index declaration in the set directory.
    ///
    /// Indexes are stored via their name and should be unique, if two indexes
    /// are defined the most recently updated / defined on will be used
    /// due to overriding data.
    pub(crate) async fn store_index_meta(&self, index: &IndexDeclaration<'_>) -> Result<()> {
        let buff = serde_json::to_vec(index)?;
        let path = format!("{}/{}", &self.dir, index.name);

        info!("storing index metadata in {}", &path);
        fs::write(path, buff).await?;

        Ok(())
    }
}


