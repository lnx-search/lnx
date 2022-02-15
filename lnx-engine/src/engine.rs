use super::error::EngineStartError;


pub async fn init_engine(
    storage_cfg: lnx_storage::Config,
    indexer_cfg: lnx_writer::IndexerHandlerConfig,
) -> Result<(), EngineStartError> {
    lnx_storage::init_with_config(storage_cfg)
        .await
        .map_err(EngineStartError::StorageError)?;

    lnx_writer::start(indexer_cfg, todo!());

    Ok(())
}



