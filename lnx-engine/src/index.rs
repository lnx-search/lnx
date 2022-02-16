use anyhow::Result;
use lnx_common::schema::Schema;
use lnx_storage::ReplicationInfo;

#[instrument(name = "add-index", skip(additional_settings, schema, index))]
pub async fn add_index(
    name: &str,
    schema: Schema,
    replication: ReplicationInfo,
    polling_mode: lnx_storage::PollingMode,
    additional_settings: std::collections::HashMap<String, Vec<u8>>,
    index: tantivy::Index,
) -> Result<()> {
    lnx_storage::engine()
        .add_new_index(name, schema, replication, polling_mode, additional_settings)
        .await?;

    let handle = lnx_writer::start_polling_for_index(name.to_string(), polling_mode);

    lnx_writer::get().add_index(name, index, handle).await?;

    Ok(())
}

#[instrument(name = "remove-index")]
pub async fn remove_index(name: &str) -> Result<()> {
    lnx_writer::get().remove_index(name).await?;

    lnx_storage::engine().remove_index(name).await?;

    Ok(())
}

