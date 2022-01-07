use std::os::linux::raw::stat;
use engine::structures::IndexDeclaration;
use routerify::ext::RequestExt;
use serde::Deserialize;
use anyhow::Context;

use crate::helpers::{atomic_store, LnxRequest, LnxResponse};
use crate::responders::json_response;
use crate::state::State;
use crate::{get_or_400, json, INDEX_KEYSPACE};
use crate::error::LnxError;

#[derive(Deserialize)]
struct IndexCreationPayload {
    #[serde(default)]
    override_if_exists: bool,
    index: IndexDeclaration,
}

pub async fn create_index(mut req: LnxRequest) -> LnxResponse {
    let payload: IndexCreationPayload = json!(req.body_mut());
    let state = req.data::<State>().expect("get state");

    // In case we need to remove the index due to failed persistence.
    let name = payload.index.name().to_string();

    state
        .engine
        .add_index(payload.index, payload.override_if_exists)
        .await?;

    let indexes = state.engine.get_all_indexes();
    let storage = state.storage.clone();

    let res = atomic_store(storage, INDEX_KEYSPACE, indexes)
        .await
        .context("attempting to persist index settings");

    if res.is_err() {
        state.engine.remove_index(&name).await?;
        res?;
    }

    json_response(200, "index created.")
}

pub async fn delete_index(req: LnxRequest) -> LnxResponse {
    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));

    let indexes: Vec<IndexDeclaration> = state.engine
        .get_all_indexes()
        .into_iter()
        .filter(|v| v.name() != index)
        .collect();

    let storage = state.storage.clone();

    atomic_store(storage, INDEX_KEYSPACE, indexes)
        .await
        .context("attempting to persist index settings")?;

    state.engine.remove_index(index).await?;

    json_response(200, "index deleted")
}
