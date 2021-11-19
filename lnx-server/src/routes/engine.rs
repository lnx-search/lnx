use anyhow::{Error, Result};
use engine::structures::IndexDeclaration;
use routerify::ext::RequestExt;
use serde::Deserialize;

use crate::helpers::{LnxRequest, LnxResponse};
use crate::responders::json_response;
use crate::state::State;
use crate::{get_or_400, json, INDEX_KEYSPACE};

#[derive(Deserialize)]
struct IndexCreationPayload {
    #[serde(default)]
    override_if_exists: bool,
    index: IndexDeclaration,
}

pub async fn create_index(mut req: LnxRequest) -> LnxResponse {
    let payload: IndexCreationPayload = json!(req.body_mut());
    let state = req.data::<State>().expect("get state");

    state
        .engine
        .add_index(payload.index, payload.override_if_exists)
        .await?;

    let indexes = state.engine.get_all_indexes();
    let storage = state.storage.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let raw_data = serde_json::to_vec(&indexes)?;
        storage.store_structure(INDEX_KEYSPACE, &raw_data)
    })
    .await
    .map_err(Error::from)??;

    json_response(200, "index created.")
}

pub async fn delete_index(req: LnxRequest) -> LnxResponse {
    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    state.engine.remove_index(index).await?;

    let indexes = state.engine.get_all_indexes();
    let storage = state.storage.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let raw_data = serde_json::to_vec(&indexes)?;
        storage.store_structure(INDEX_KEYSPACE, &raw_data)
    })
    .await
    .map_err(Error::from)??;

    json_response(200, "index deleted")
}
