mod range;
mod base;
mod term_value;
mod fuzzy;
mod fast_fuzzy;

use std::borrow::Cow;
use tantivy::Score;
use utoipa::ToSchema;
use serde::Deserialize;
use validator::Validate;

use self::{range::Range, term_value::TermValue};

pub use fuzzy::FuzzyQueryContext;
pub use fast_fuzzy::FastFuzzyQueryContext;
pub use base::{AsQueryTerm, InvalidTermValue, BuildQueryError, AsQuery};

#[derive(Debug, Clone, Validate, Deserialize, ToSchema)]
#[validate(schema(function = "validators::validate_ops", arg = "&'v_a mut validators::ValidationConfig"))]
/// A `QueryLayer` is a potentially recursive structure that allows
/// contains the actual query logic and builders but is not schema-aware.
pub struct QueryLayer {
    #[schema(inline)]
    #[serde(flatten)]
    pub query: Option<QueryKind>,

    #[schema(inline)]
    #[serde(flatten)]
    /// A set of helper ops.
    /// 
    /// Due to the nature of these helpers only one can be used
    /// per query level.
    /// 
    /// *This is a recursive type and as such can be subject
    /// to potential DOS attacks so care should be taken when exposing
    /// the the outside world.*
    /// 
    /// Internally the system will only deserialize the query to a depth of `128`
    /// and will not build a query that goes beyond the set max depth (defaults to `3`).
    pub pipeline: Option<Box<HelperOps>>,

    #[schema(inline)]
    #[serde(flatten)]
    pub fields: Option<FieldSelector>,

    #[schema(example = json!(1.5))]
    #[serde(default, rename = "$boost")]
    /// An optional score multiplier to adjust bias towards the query.
    pub boost: Option<Score>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub enum HelperOps {
    #[serde(rename = "$all")]
    /// A set of queries which all must match for a given document to be retrieved.
    All(Vec<QueryLayer>),

    #[serde(rename = "$any")]
    /// A set of queries where only one needs to match for a given document to be retrieved.
    Any(Vec<QueryLayer>),

    #[serde(rename = "$none")]
    /// A set of queries which all must **not* match for a given document to be retrieved.
    None(Vec<QueryLayer>),
}

impl HelperOps {
    pub fn as_layers(&self) -> &[QueryLayer] {
        match self {
            HelperOps::All(layers) => layers,
            HelperOps::Any(layers) => layers,
            HelperOps::None(layers) => layers,
        }
    }
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({ "text": { "$should": "Hello, world" } }))]
pub enum QueryKind {
    /// A query match document containing a single raw term.
    /// 
    /// This does not perform any scoring, it simply selects
    /// all documents that contain the given term.
    Term(MultiValueSelector<TermValue>),

    /// A query that matches all documents.
    All {},

    /// A query that matches a specific sequence of words.
    /// 
    /// There must be at least two terms, and all terms must belong to the same field.
    /// 
    /// For instance the phrase query for "part time" will match the sentence:
    /// 
    /// `Alan just got a part time job.`
    /// 
    /// On the other hand it will not match the sentence.
    /// 
    /// `This is my favorite part of the job.`
    /// 
    /// Using a PhraseQuery on a field requires positions to be indexed for this field.
    /// 
    /// Slop can be provided with the query to adjust whether some terms can be missing
    /// or not from the phrase and still match.
    Phrase(QuerySelector<String>),

    /// A `regex` query matches all of the documents containing a specific term that
    ///  matches a regex pattern.
    /// 
    /// Wildcard queries (e.g. `ho*se`) can be achieved by converting them to their 
    /// regex counterparts.
    Regex(MultiValueSelector<QuerySelector<String>>),

    /// A query that matches all documents that have at least one term within
    /// a defined range.
    /// 
    /// Matched document will all get a constant Score of one.
    Range(MultiValueSelector<Range>),

    /// A query that matches all of the documents containing a specific
    /// set of terms that is within Levenshtein distance for each term. 
    /// (Typo-Tolerance)
    /// 
    /// This is typically what you want for user facing search queries when
    /// `fast-fuzzy` is unsuitable or unable to be used due to system resources constraints.
    Fuzzy(MultiValueSelector<QuerySelector<FuzzyQueryContext>>),
    
    /// A query that behaves similarly to the `fuzzy` query except using a pre-computation
    /// based algorithm.
    /// 
    /// This query requires `fast-fuzzy` mode being enabled on the index settings itself 
    /// (disabled by default) as this requires more memory when indexing and potentially
    /// when searching.
    FastFuzzy(MultiValueSelector<QuerySelector<FastFuzzyQueryContext>>),
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub enum FieldSelector {
    #[serde(rename = "$field")]
    /// Apply the query to a single specific field.
    Single(String),

    #[serde(rename = "$fields")]
    /// Apply the query to the specific fields.
    Multi(Vec<String>),
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum MultiValueSelector<V> {
    Single(V),
    Multi(Vec<V>),
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum QuerySelector<Q: From<String> + Clone> {
    Str(String),
    Detailed(Q)
}

impl<Q: From<String> + Clone> QuerySelector<Q> {
    /// Converts the `Str` variant to the default 
    /// inner version of the query if applicable.
    pub fn as_inner_query(&self) -> Cow<Q> {
        match self {
            QuerySelector::Str(v) => Cow::Owned(Q::from(v.clone())),
            QuerySelector::Detailed(q) => Cow::Borrowed(q),
        }
    } 
}

#[derive(Debug, Copy, Clone, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Occur {
    /// The value *should* appear in the document but is not required to be contained
    /// within a document if another term matched instead. (default)
    Should,

    /// The value *must* appear in the document.
    Must,

    /// The value *must not* appear in the document.
    MustNot,
}

impl Occur {
    pub fn into_tantivy_occur(self) -> tantivy::query::Occur {
        match self {
            Occur::Should => tantivy::query::Occur::Should,
            Occur::Must => tantivy::query::Occur::Must,
            Occur::MustNot => tantivy::query::Occur::MustNot,
        }
    }
}

impl Default for Occur {
    fn default() -> Self {
        Self::Should
    }
}


mod validators {
    use std::borrow::Cow;

    use super::*;
    use validator::ValidationError;

    #[derive(Copy, Clone)]
    pub struct ValidationConfig {
        pub max_depth: usize,
        pub current_depth: usize,
        pub max_pipeline_queries: usize,
        pub max_sub_queries_total: usize,
        pub sub_queries_counter: usize,
    }

    impl ValidationConfig {
        fn inc_depth(&mut self) {
            self.current_depth += 1;
        }

        fn inc_sub_queries(&mut self) {
            self.sub_queries_counter += 1;
        }
    }

    /// Checks ops and the recursion limit.
    /// 
    /// Technically this does not prevent the system from deserializing the JSON recursively
    /// but it does provide protection against building an incredibly expensive query.
    pub fn validate_ops(layer: &QueryLayer, cfg: &mut ValidationConfig) -> Result<(), ValidationError> {
        let mut error = ValidationError::new("bad_query");

        if cfg.current_depth >= cfg.max_depth {
            error.message = Some(Cow::Borrowed("Maximum allowed query depth has been reached."));
            error.add_param(Cow::Borrowed("max_depth"), &cfg.max_depth);
            return Err(error)
        }

        if layer.query.is_none() && layer.pipeline.is_none() {
            error.message = Some(Cow::Borrowed("Either a pipeline operation must be specified ('$all', '$any', '$none') or a query field specified."));
            return Err(error)
        }

        if let Some(inner_layer) = layer.pipeline.as_ref() {

            let layers = inner_layer.as_layers();

            if layers.len() > cfg.max_pipeline_queries {
                error.message = Some(Cow::Borrowed("Too many sub queries."));
                error.add_param(Cow::Borrowed("max_sub_queries_per_pipeline"), &cfg.max_pipeline_queries);
                error.add_param(Cow::Borrowed("current_sub_queries_per_pipeline"), &layers.len());
                error.add_param(Cow::Borrowed("max_sub_queries_total"), &cfg.max_sub_queries_total);
                error.add_param(Cow::Borrowed("current_sub_queries_total"), &cfg.sub_queries_counter);
                return Err(error)
            }

            cfg.inc_depth();

            for layer in layers {
                cfg.inc_sub_queries();

                validate_ops(layer, cfg)?;
            }
        }        

        Ok(())
    }
}