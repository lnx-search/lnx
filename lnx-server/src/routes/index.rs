use std::collections::BTreeMap;
use std::time::Instant;

use engine::structures::{DocumentOptions, DocumentValueOptions};
use engine::{DocumentId, Index, QueryPayload, QueryResults};
use routerify::ext::RequestExt;

use crate::helpers::{LnxRequest, LnxResponse};
use crate::responders::json_response;
use crate::state::State;
use crate::{get_or_400, json, unauthorized};
use crate::error::{LnxError, Result};


pub async fn ensure_index_perms(req: LnxRequest) -> Result<LnxRequest> {
    if !req.uri().path().starts_with("/indexes/") {
        return Ok(req)
    };

    let state = req.data::<State>().expect("get state");

    if !state.auth.enabled() {
        return Ok(req)
    }

    let auth = req.headers().get("Authorization");
    let token = match auth {
        Some(auth) => auth
            .to_str()
            .map_err(|_| LnxError::BadRequest("invalid token provided"))?,
        None => return unauthorized!("missing authorization header"),
    };

    let data = match state.auth.get_token_data(&token) {
        None => return unauthorized!("invalid token provided"),
        Some(v) => v,
    };

    let path = req.uri().path();
    let index = {
        let stop: &str = path
            .strip_prefix("/indexes/")
            .unwrap_or_else(|| "");

        let mut split = stop.split("/");
        split.next().unwrap_or_else(|| stop).to_string()
    };

    if !data.has_access_to_index(&index) {
       return unauthorized!("invalid token does not have access to this index")
    }

    Ok(req)
}

pub async fn commit(req: LnxRequest) -> LnxResponse {
    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    let index = get_or_400!(state.engine.get_index(index), "index does not exist");

    index.commit().await?;

    json_response(200, "changed committed")
}

pub async fn rollback(req: LnxRequest) -> LnxResponse {
    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    let index = get_or_400!(state.engine.get_index(index), "index does not exist");

    index.rollback().await?;

    json_response(200, "changed dropped")
}

pub async fn search_index(mut req: LnxRequest) -> LnxResponse {
    let payload: QueryPayload = json!(req.body_mut());

    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    let index = get_or_400!(state.engine.get_index(index), "index does not exist");

    let start = Instant::now();
    let results: QueryResults = index.search(payload).await?;

    if state.log_search {
        info!(
            "search took {:?} returning {} results",
            start.elapsed(),
            results.len()
        );
    }

    json_response(200, &results)
}

pub async fn get_document(req: LnxRequest) -> LnxResponse {
    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    let index = get_or_400!(state.engine.get_index(index), "index does not exist");

    let raw_doc_id = get_or_400!(req.param("document_id"));
    let document_id = get_or_400!(raw_doc_id.parse::<DocumentId>().ok());

    let document = index.get_document(document_id).await?;

    json_response(200, &document)
}

pub async fn add_stop_words(mut req: LnxRequest) -> LnxResponse {
    let payload: Vec<String> = json!(req.body_mut());

    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    let index: Index =
        get_or_400!(state.engine.get_index(index), "index does not exist");

    index.add_stop_words(payload).await?;

    json_response(200, "stop words added")
}

pub async fn remove_stop_words(mut req: LnxRequest) -> LnxResponse {
    let payload: Vec<String> = json!(req.body_mut());

    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    let index: Index =
        get_or_400!(state.engine.get_index(index), "index does not exist");

    index.remove_stop_words(payload).await?;

    json_response(200, "stop words added")
}

pub async fn add_documents(mut req: LnxRequest) -> LnxResponse {
    let payload: DocumentOptions = json!(req.body_mut());

    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    let index: Index =
        get_or_400!(state.engine.get_index(index), "index does not exist");

    index.add_documents(payload).await?;

    json_response(200, "changes registered")
}

pub async fn delete_documents(mut req: LnxRequest) -> LnxResponse {
    let payload: BTreeMap<String, DocumentValueOptions> = json!(req.body_mut());

    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    let index: Index =
        get_or_400!(state.engine.get_index(index), "index does not exist");

    index.delete_documents_where(payload).await?;

    json_response(200, "changes registered")
}

pub async fn clear_documents(req: LnxRequest) -> LnxResponse {
    let state = req.data::<State>().expect("get state");
    let index = get_or_400!(req.param("index"));
    let index: Index =
        get_or_400!(state.engine.get_index(index), "index does not exist");

    index.clear_documents().await?;

    json_response(200, "changes registered")
}
