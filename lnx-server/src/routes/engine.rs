use engine::structures::IndexDeclaration;
use routerify::ext::RequestExt;
use serde::Deserialize;

use crate::helpers::{atomic_store, LnxRequest, LnxResponse};
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

    // In case we need to remove the index due to failed persistence.
    let name = payload.index.name().to_string();

    state
        .engine
        .add_index(payload.index, payload.override_if_exists)
        .await?;

    let indexes = state.engine.get_all_indexes();
    let storage = state.storage.clone();

    // This kinda sucks that we have to do this due to Bincode not enjoying
    // the IndexDeclaration struct.
    let buffer = serde_json::to_vec(&indexes)?;
    let res = atomic_store(storage, INDEX_KEYSPACE, buffer).await;

    if res.is_err() {
        state.engine.remove_index(&name).await?;
        res?;
    }

    json_response(200, "index created.")
}

pub async fn delete_index(req: LnxRequest) -> LnxResponse {
    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));

    let indexes: Vec<IndexDeclaration> = state
        .engine
        .get_all_indexes()
        .into_iter()
        .filter(|v| v.name() != index)
        .collect();

    let storage = state.storage.clone();

    // This kinda sucks that we have to do this due to Bincode not enjoying
    // the IndexDeclaration struct.
    let buffer = serde_json::to_vec(&indexes)?;
    atomic_store(storage, INDEX_KEYSPACE, buffer).await?;

    state.engine.remove_index(index).await?;

    json_response(200, "index deleted")
}
