use std::path::Path;
use std::sync::Arc;
use hashbrown::HashMap;
use tantivy::directory::MmapDirectory;
use tantivy::Index;
use lnx_common::schema::Schema;


use super::error::InitEngineError;


pub async fn init_engine(
    storage_cfg: lnx_storage::Config,
    indexer_cfg: lnx_writer::IndexerHandlerConfig,
) -> Result<(), InitEngineError> {
    lnx_storage::init_with_config(storage_cfg)
        .await
        .map_err(InitEngineError::StorageError)?;

    let mut indexes = HashMap::new();
    for (name, data) in lnx_storage::engine().indexes().as_ref() {
        let index = load_index(data.file_path(), data.schema())?;
        indexes.insert(name.to_string(), Arc::new(index));
    }

    lnx_writer::start(indexer_cfg, indexes);

    Ok(())
}


fn load_index(path: &Path, schema: &Schema) -> Result<Index, InitEngineError> {
    std::fs::create_dir_all(path)?;

    let dir = MmapDirectory::open(path)?;
    let does_exist = Index::exists(&dir)?;

    let index = if does_exist {
        Index::open(dir)
    } else {
        Index::open_or_create(dir, schema.as_tantivy_schema())
    }?;

    let ref_schema = index.schema();
    schema.validate_with_tantivy_schema(&ref_schema)?;

    Ok(index)
}