use std::collections::BTreeMap;
use serde::Deserialize;
use anyhow::{Error, Result};
use thruster::{middleware_fn, MiddlewareNext, MiddlewareResult};

use engine::{QueryPayload, DocumentId};
use engine::structures::{DocumentOptions, DocumentValueOptions, IndexDeclaration};

use crate::{get_index, check_error, INDEX_KEYSPACE};
use crate::responders::json_response;
use crate::state::Ctx;


#[derive(Deserialize)]
struct IndexCreationPayload {
    #[serde(default)]
    override_if_exists: bool,
    index: IndexDeclaration,
}


#[middleware_fn]
pub async fn create_index(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let res = ctx.request()
        .body_json::<IndexCreationPayload>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize index payload");

    let res = ctx.state.engine.add_index(payload.index, payload.override_if_exists).await;
    let (_, ctx): ((), Ctx) = check_error!(res, ctx, "add index");

    let indexes = ctx.state.engine.get_all_indexes();
    let storage = ctx.state.storage.clone();
    let res = tokio::task::spawn_blocking(move || -> Result<()> {
        storage.store_structure(INDEX_KEYSPACE, &indexes)
    }).await.map_err(Error::from);
    let (res, ctx) =  check_error!(res, ctx, "join worker thread");
    let (_, ctx) =  check_error!(res, ctx, "save indexes");

    Ok(json_response(ctx, 200, "index created."))
}

#[middleware_fn]
pub async fn delete_index(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let index = match ctx.request().params() {
        None => return Ok(json_response(
            ctx,
            400,
            "missing required url parameters.",
        )),
        Some(params) => {
            match params.get("index") {
                Some(t) => t.to_string(),
                None => return Ok(json_response(
                    ctx,
                    400,
                    "missing required url parameters 'index'.",
                )),
            }
        },
    };

    let res = ctx.state.engine.remove_index(&index).await;
    let (_, ctx) = check_error!(res, ctx, "remove index");

    let indexes = ctx.state.engine.get_all_indexes();
    let storage = ctx.state.storage.clone();
    let res = tokio::task::spawn_blocking(move || -> Result<()> {
        storage.store_structure(INDEX_KEYSPACE, &indexes)
    }).await.map_err(Error::from);
    let (res, ctx) =  check_error!(res, ctx, "join worker thread");
    let (_, ctx) =  check_error!(res, ctx, "save indexes");

    Ok(json_response(ctx,200,"index deleted"))
}

#[middleware_fn]
pub async fn search_index(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);

    let res = ctx.request()
        .body_json::<QueryPayload>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize query payload");
    let (results, ctx) = check_error!(index.search(payload).await, ctx, "search index");

    Ok(json_response(ctx, 200, &results))
}

#[middleware_fn]
pub async fn add_stop_words(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);

    let res = ctx.request()
        .body_json::<Vec<String>>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize stop words");
    let (_, ctx) = check_error!(index.add_stop_words(payload).await, ctx, "add stop words");

    Ok(json_response(ctx, 200, "stop words added"))
}

#[middleware_fn]
pub async fn remove_stop_words(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);

    let res = ctx.request()
        .body_json::<Vec<String>>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize stop words");
    let (_, ctx) = check_error!(index.remove_stop_words(payload).await, ctx, "remove stop words");

    Ok(json_response(ctx, 200, "stop words removed"))
}

#[middleware_fn]
pub async fn add_documents(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);

    let res = ctx.request()
        .body_json::<DocumentOptions>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize documents");
    let (_, ctx) = check_error!(index.add_documents(payload).await, ctx, "add documents");

    Ok(json_response(ctx, 200, "documents added"))
}


#[middleware_fn]
pub async fn delete_documents(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);

    let res = ctx.request()
        .body_json::<BTreeMap<String, DocumentValueOptions>>()
        .map_err(Error::from);

    let (payload, ctx) = check_error!(res, ctx, "deserialize delete filter");
    let (_, ctx) = check_error!(index.delete_documents_where(payload).await, ctx, "remove documents");

    Ok(json_response(ctx, 200, "documents removed"))
}

#[middleware_fn]
pub async fn clear_documents(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);
    let (_, ctx) = check_error!(index.clear_documents().await, ctx, "clear documents");

    Ok(json_response(ctx, 200, "documents removed"))
}


#[middleware_fn]
pub async fn get_document(ctx: Ctx, _next: MiddlewareNext<Ctx>) -> MiddlewareResult<Ctx> {
    let (index, ctx) = get_index!(ctx);

    let document_id = match ctx.request().params() {
        None => return Ok(json_response(
            ctx,
            400,
            "missing required url parameters.",
        )),
        Some(params) => {
            match params.get("index") {
                Some(t) => t.to_string(),
                None => return Ok(json_response(
                    ctx,
                    400,
                    "missing required url parameters 'index'.",
                )),
            }
        },
    };

    let document_id = match document_id.parse::<DocumentId>() {
        Ok(v) => v,
        Err(_) => return Ok(json_response(
            ctx,
            400,
            "invalid document id given.",
        )),
    };

    let (result, ctx) = check_error!(index.get_document(document_id).await, ctx, "get document");

    Ok(json_response(ctx, 200, &result))
}