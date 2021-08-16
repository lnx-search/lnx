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
    pub(crate) async fn with_directory(dir: String) -> Result<Self> {
        fs::create_dir_all(&dir).await?;

        Ok(Self { dir })
    }

    /// Loads all indexes from the given directory.
    ///
    /// This will error if the files inside the directory are not all
    /// valid index files.
    pub(crate) async fn load_all(&self) -> Result<Vec<LoadedIndex>> {
        let mut indexes = vec![];
        let mut read_dir = fs::read_dir(&self.dir).await?;

        while let Some(file) = read_dir.next_entry().await? {
            info!(
                "loading index from directory {} with path {:?}",
                &self.dir,
                file.path()
            );
            let data = fs::read(file.path()).await?;

            let loader = serde_json::from_slice::<IndexDeclaration>(&data)?;
            let loaded = loader.into_schema();

            indexes.push(loaded);
        }

        Ok(indexes)
    }

    /// Stores a given index declaration in the set directory.
    ///
    /// Indexes are stored via their name and should be unique, if two indexes
    /// are defined the most recently updated / defined on will be used
    /// due to overriding data.
    pub(crate) async fn store_index_meta(&self, index: &IndexDeclaration) -> Result<()> {
        let buff = serde_json::to_vec(index)?;
        let path = format!("{}/{}", &self.dir, index.name);

        info!("storing index metadata in {}", &path);
        fs::write(path, buff).await?;

        Ok(())
    }

    /// Removes a given index from the directory storage.
    ///
    /// If the name doesnt exist in this path this will not error.
    pub(crate) async fn remove_index_meta(&self, name: &str) -> Result<()> {
        let path = format!("{}/{}", &self.dir, name);
        if let Err(e) = fs::remove_file(&path).await {
            match e.kind() {
                // if it doesnt exist, just ignore it.
                tokio::io::ErrorKind::NotFound => Ok(()),
                _ => Err(e),
            }?;
        };

        Ok(())
    }

    /// Removes all persistent index definitions.
    pub(crate) async fn clear_all(&self) -> Result<()> {
        fs::remove_dir_all(&self.dir).await?;

        Ok(())
    }
}
