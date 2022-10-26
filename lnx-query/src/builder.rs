use std::collections::HashMap;

use tantivy::Score;
use tantivy::query::{Query, AllQuery, Occur};
use tantivy::schema::{Schema, Field};

use crate::query_structure::{QueryLayer, QueryKind, HelperOps};

#[derive(Debug, thiserror::Error)]
pub enum QueryBuildError {
    #[error("Unknown field: {0:?}")]
    UnknownField(String),

    #[error("The Query is invalid.")]
    InvalidQuery,
}



pub fn build_query(
    query: QueryLayer, 
    schema: &Schema,
    default_fields: &[Field],
    default_boost_factors: &HashMap<String, Score>,
) -> Result<Box<dyn Query>, QueryBuildError> {
    if let Some(kind) = query.query {
        let (_, query) = build_kind_layer(kind, schema, default_fields, default_boost_factors)?;
        return Ok(query);
    } 

    if let Some(pipeline) = query.pipeline {
        return build_pipeline_op(*pipeline, schema, default_fields, default_boost_factors)
    }

    Ok(Box::new(AllQuery{}))
}

fn build_query_inner(
    query: QueryLayer, 
    schema: &Schema,
    default_fields: &[Field],
    default_boost_factors: &HashMap<String, Score>,
) -> Result<(Occur, Box<dyn Query>), QueryBuildError> {
    if let Some(kind) = query.query {
        return build_kind_layer(kind, schema, default_fields, default_boost_factors);
    } 

    if let Some(pipeline) = query.pipeline {
        return Ok((Occur::Should, build_pipeline_op(*pipeline, schema, default_fields, default_boost_factors)?));
    }

    Ok((Occur::Should, Box::new(AllQuery{})))
}


fn build_kind_layer(
    layer: QueryKind,
    schema: &Schema,
    default_fields: &[Field],
    default_boost_factors: &HashMap<String, Score>,
) -> Result<(Occur, Box<dyn Query>), QueryBuildError> {
    let res: (Occur, Box<dyn Query>) = match layer {
        QueryKind::Term(selector) => {
            let ctx = selector.into_inner_query();

            todo!()
        },
        QueryKind::All {  } => (Occur::Should, Box::new(AllQuery {})),
        QueryKind::Phrase(selector) => {
            let ctx = selector.into_inner_query();

            todo!()
        },
        QueryKind::Regex(selector) => {
            let ctx = selector.into_inner_query();

            todo!()
        },
        QueryKind::Range(ctx) => {

            todo!()
        },
        QueryKind::Fuzzy(selector) => {
            let ctx = selector.into_inner_query();

            todo!()
        },
        QueryKind::FastFuzzy(selector) => {
            let ctx = selector.into_inner_query();

            

            todo!()
        },
    };

    Ok(res)
}

fn build_pipeline_op(
    pipeline: HelperOps,
    schema: &Schema,
    default_fields: &[Field],
    default_boost_factors: &HashMap<String, Score>,
) -> Result<Box<dyn Query>, QueryBuildError> {
    let queries: Vec<(Occur, Box<dyn Query>)> = match pipeline {
        HelperOps::All(inner) => {

        },
        HelperOps::Any(inner) => {

        },
        HelperOps::None(inner) => {

        },
    };

    Ok(todo!())
}