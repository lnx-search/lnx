use std::collections::HashMap;

use tantivy::query::{
    AllQuery,
    BooleanQuery,
    FuzzyTermQuery,
    Occur,
    PhraseQuery,
    Query,
    RegexQuery,
    TermQuery,
};
use tantivy::schema::{Field, FieldEntry, IndexRecordOption, Schema};
use tantivy::{Index, Score};

use crate::query_structure::{
    AsQuery,
    AsQueryTerm,
    FastFuzzyQueryContext,
    FieldSelector,
    FuzzyQueryContext,
    HelperOps,
    InvalidTermValue,
    MultiValueSelector,
    QueryKind,
    QueryLayer,
    QuerySelector,
};

#[derive(Debug, thiserror::Error)]
pub enum QueryBuildError {
    #[error("Unknown field: {0:?}")]
    UnknownField(String),

    #[error("The Query is invalid.")]
    InvalidQuery,

    #[error("The value of the query is invalid: {0}")]
    BadValue(String),

    #[error("{0}")]
    InvalidTermValue(#[from] InvalidTermValue),

    #[error("{0}")]
    TantivyError(#[from] tantivy::TantivyError),
}

pub struct BuilderSettings<'a> {
    pub index: &'a Index,
    pub schema: &'a Schema,
    pub default_query_occur: Occur,
}

pub fn build_query(
    query: &QueryLayer,
    settings: &BuilderSettings<'_>,
    default_fields: &[Field],
    boost_factors: &HashMap<String, Score>,
) -> Result<Box<dyn Query>, QueryBuildError> {
    if let Some(kind) = query.query.as_ref() {
        let fields = query
            .fields
            .as_ref()
            .map(|selector| get_fields(selector, settings.schema))
            .transpose()?;

        let mut queries = vec![];
        for field in fields.as_deref().unwrap_or(default_fields) {
            let entry = settings.schema.get_field_entry(*field);
            let query = build_kind_layer(kind, settings, *field, entry)?;

            queries.push((Occur::Should, query))
        }

        return Ok(Box::new(BooleanQuery::new(queries)));
    }

    if let Some(pipeline) = query.pipeline.as_ref() {
        return build_pipeline_op(pipeline, settings, default_fields, boost_factors);
    }

    Ok(Box::new(AllQuery {}))
}

fn get_fields(
    selector: &FieldSelector,
    schema: &Schema,
) -> Result<Vec<Field>, QueryBuildError> {
    match selector {
        FieldSelector::Single(field) => {
            let field = schema
                .get_field(field)
                .ok_or_else(|| QueryBuildError::UnknownField(field.clone()))?;

            Ok(vec![field])
        },
        FieldSelector::Multi(fields) => {
            let mut schema_fields = vec![];
            for field in fields {
                let field = schema
                    .get_field(field)
                    .ok_or_else(|| QueryBuildError::UnknownField(field.clone()))?;

                schema_fields.push(field);
            }

            Ok(schema_fields)
        },
    }
}

fn build_kind_layer(
    layer: &QueryKind,
    settings: &BuilderSettings<'_>,
    field: Field,
    field_entry: &FieldEntry,
) -> Result<Box<dyn Query>, QueryBuildError> {
    let res: Box<dyn Query> = match layer {
        QueryKind::Term(selector) => match selector {
            MultiValueSelector::Single(value) => {
                let term = value.as_term(field, field_entry)?;
                Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs))
            },
            MultiValueSelector::Multi(values) => {
                let mut queries = vec![];
                for value in values {
                    let term = value.as_term(field, field_entry)?;
                    let query =
                        Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                    queries.push((Occur::Should, query as Box<dyn Query>));
                }

                Box::new(BooleanQuery::new(queries))
            },
        },
        QueryKind::All {} => Box::new(AllQuery {}),
        QueryKind::Phrase(selector) => {
            let query = selector.as_inner_query();
            let tokenizer = settings.index.tokenizer_for_field(field)?;
            let mut stream = tokenizer.token_stream(&query);

            let mut terms = vec![];
            while let Some(token) = stream.next() {
                let term = token.text.as_term(field, field_entry)?;
                terms.push(term);
            }

            Box::new(PhraseQuery::new(terms))
        },
        QueryKind::Regex(selector) => match selector {
            MultiValueSelector::Single(selector) => {
                let query = selector.as_inner_query();
                let regex = RegexQuery::from_pattern(&query, field)
                    .map_err(|e| QueryBuildError::BadValue(e.to_string()))?;

                Box::new(regex)
            },
            MultiValueSelector::Multi(values) => {
                let mut queries = vec![];
                for selector in values {
                    let query = selector.as_inner_query();
                    let regex = RegexQuery::from_pattern(&query, field)
                        .map_err(|e| QueryBuildError::BadValue(e.to_string()))?;

                    queries.push((Occur::Should, Box::new(regex) as Box<dyn Query>));
                }

                Box::new(BooleanQuery::new(queries))
            },
        },
        QueryKind::Range(selector) => match selector {
            MultiValueSelector::Single(value) => value.as_query(field, field_entry)?,
            MultiValueSelector::Multi(values) => {
                let mut queries = vec![];
                for value in values {
                    let query = value.as_query(field, field_entry)?;
                    queries.push((Occur::Should, query as Box<dyn Query>));
                }

                Box::new(BooleanQuery::new(queries))
            },
        },
        QueryKind::Fuzzy(selector) => match selector {
            MultiValueSelector::Single(selector) => {
                build_fuzzy_query(selector, settings, field, field_entry)?
            },
            MultiValueSelector::Multi(values) => {
                let mut queries = vec![];
                for selector in values {
                    let query =
                        build_fuzzy_query(selector, settings, field, field_entry)?;
                    queries.push((Occur::Should, query));
                }

                Box::new(BooleanQuery::new(queries))
            },
        },
        QueryKind::FastFuzzy(selector) => match selector {
            MultiValueSelector::Single(selector) => {
                build_fast_fuzzy_query(selector, settings, field, field_entry)?
            },
            MultiValueSelector::Multi(values) => {
                let mut queries = vec![];
                for selector in values {
                    let query =
                        build_fast_fuzzy_query(selector, settings, field, field_entry)?;
                    queries.push((Occur::Should, query));
                }

                Box::new(BooleanQuery::new(queries))
            },
        },
    };

    Ok(res)
}

fn build_pipeline_op(
    pipeline: &HelperOps,
    settings: &BuilderSettings<'_>,
    fields: &[Field],
    boost_factors: &HashMap<String, Score>,
) -> Result<Box<dyn Query>, QueryBuildError> {
    let mut queries: Vec<(Occur, Box<dyn Query>)> = vec![];
    match pipeline {
        HelperOps::All(layers) => {
            for layer in layers {
                let query = build_query(layer, settings, fields, boost_factors)?;
                queries.push((Occur::Must, query));
            }
        },
        HelperOps::Any(layers) => {
            for layer in layers {
                let query = build_query(layer, settings, fields, boost_factors)?;
                queries.push((Occur::Should, query));
            }
        },
        HelperOps::None(layers) => {
            for layer in layers {
                let query = build_query(layer, settings, fields, boost_factors)?;
                queries.push((Occur::MustNot, query));
            }
        },
    };

    Ok(Box::new(BooleanQuery::new(queries)))
}

fn build_fuzzy_query(
    selector: &QuerySelector<FuzzyQueryContext>,
    settings: &BuilderSettings,
    field: Field,
    field_entry: &FieldEntry,
) -> Result<Box<dyn Query>, QueryBuildError> {
    let query = selector.as_inner_query();
    let occur = query.word_occurrence.into_tantivy_occur();
    let tokenizer = settings.index.tokenizer_for_field(field)?;
    let mut stream = tokenizer.token_stream(&query.value);

    let mut queries = vec![];
    while let Some(token) = stream.next() {
        let term = token.text.as_term(field, field_entry)?;
        let distance = query.edit_distance_bounds.get_word_distance(&token.text);
        let query = Box::new(FuzzyTermQuery::new(
            term,
            distance,
            !query.transposition_costs_two,
        ));
        queries.push((occur, query as Box<dyn Query>));
    }

    Ok(Box::new(BooleanQuery::new(queries)))
}

fn build_fast_fuzzy_query(
    selector: &QuerySelector<FastFuzzyQueryContext>,
    settings: &BuilderSettings,
    field: Field,
    field_entry: &FieldEntry,
) -> Result<Box<dyn Query>, QueryBuildError> {
    let query = selector.as_inner_query();
    let occur = query.word_occurrence.into_tantivy_occur();
    let tokenizer = settings.index.tokenizer_for_field(field)?;
    let mut stream = tokenizer.token_stream(&query.value);

    let mut queries = vec![];
    while let Some(token) = stream.next() {
        let term = token.text.as_term(field, field_entry)?;
        let distance = query.edit_distance_bounds.get_word_distance(&token.text);
        let query = Box::new(FuzzyTermQuery::new(term, distance, true));
        queries.push((occur, query as Box<dyn Query>));
    }

    Ok(Box::new(BooleanQuery::new(queries)))
}
