mod selector;
mod range;
mod base;
mod term_value;

use tantivy::Score;
use utoipa::ToSchema;
use serde::Deserialize;
use validator::Validate;

use self::{selector::ValueSelector, range::Range, term_value::TermValue};

pub use base::AsQueryTerm;


#[derive(Debug, Clone, Validate, Deserialize, ToSchema)]
#[validate(schema(function = "validators::validate_ops", arg = "(usize, usize)"))]
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

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({ "text": { "$should": "Hello, world" } }))]
pub enum QueryKind {
    /// A query match document containing a single raw term.
    /// 
    /// This does not perform any scoring, it simply selects
    /// all documents that contain the given term.
    Term(QuerySelector<QueryContext<TermValue>>),

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
    Phrase(QuerySelector<QueryContext<String>>),

    /// A `regex` query matches all of the documents containing a specific term that
    ///  matches a regex pattern.
    /// 
    /// Wildcard queries (e.g. `ho*se`) can be achieved by converting them to their 
    /// regex counterparts.
    Regex(QuerySelector<QueryContext<String>>),

    /// A query that matches all documents that have at least one term within
    /// a defined range.
    /// 
    /// Matched document will all get a constant Score of one.
    Range(QueryContext<Range>),

    /// A query that matches all of the documents containing a specific
    /// set of terms that is within Levenshtein distance for each term. 
    /// (Typo-Tolerance)
    /// 
    /// This is typically what you want for user facing search queries when
    /// `fast-fuzzy` is unsuitable or unable to be used due to system resources constraints.
    Fuzzy(QuerySelector<QueryContext<String>>),
    
    /// A query that behaves similarly to the `fuzzy` query except using a pre-computation
    /// based algorithm.
    /// 
    /// This query requires `fast-fuzzy` mode being enabled on the index settings itself 
    /// (disabled by default) as this requires more memory when indexing and potentially
    /// when searching.
    FastFuzzy(QuerySelector<QueryContext<String>>),
}


#[derive(Debug, Clone, Deserialize, ToSchema)]
pub enum Occur<V> {
    #[serde(rename = "$should")]
    /// The documents in the doc set should match the query but it is not strictly required if another
    /// query matches as well.
    Should(V),

    #[serde(rename = "$must")]
    /// The documents in the doc set must match the query.
    Must(V),

    #[serde(rename = "$not")]
    /// The document in the doc set must not match the query.
    MustNot(V),
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct QueryContext<V> {
    #[schema(inline)]
    #[serde(flatten)]
    pub occur: Occur<ValueSelector<V>>,

    #[schema(inline)]
    #[serde(flatten)]
    pub fields: Option<FieldSelector>,

    #[schema(example = json!(1.5))]
    #[serde(default, rename = "$boost")]
    /// An optional score multiplier to adjust bias towards the query.
    pub boost: Score,
}

impl<V: From<String>> From<String> for QueryContext<V> {
    fn from(v: String) -> Self {
        Self {
            occur: Occur::Should(ValueSelector::Single(V::from(v))),
            fields: None,
            boost: 0.0
        }
    }
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
pub enum QuerySelector<Q: From<String>> {
    Str(String),
    Detailed(Q)
}

impl<Q: From<String>> QuerySelector<Q> {
    /// Converts the `Str` variant to the default 
    /// inner version of the query if applicable.
    pub fn into_inner_query(self) -> Q {
        match self {
            QuerySelector::Str(v) => Q::from(v),
            QuerySelector::Detailed(q) => q,
        }
    } 
}


mod validators {
    use std::borrow::Cow;

    use super::*;
    use validator::ValidationError;

    /// Checks ops and the recursion limit.
    /// 
    /// Technically this does not prevent the system from deserializing the JSON recursively
    /// but it does provide protection against building an incredibly expensive query.
    pub fn validate_ops(layer: &QueryLayer, depth_tracker: (usize, usize)) -> Result<(), ValidationError> {
        let (max_depth, current_depth) = depth_tracker;

        if current_depth >= max_depth {
            let mut error = ValidationError::new("bad_query");
            error.message = Some(Cow::Borrowed("Maximum allowed query depth has been reached."));
            error.add_param(Cow::Borrowed("max_depth"), &max_depth);
            return Err(error)
        }

        if layer.query.is_none() && layer.pipeline.is_none() {
            let mut error = ValidationError::new("bad_query");
            error.message = Some(Cow::Borrowed("Either a pipeline operation must be specified ('$all', '$any', '$none') or a query field specified."));
            return Err(error)
        }

        if let Some(inner_layer) = layer.pipeline.as_ref() {
            validate_ops(inner_layer, (max_depth, current_depth + 1))?;
        }        

        Ok(())
    }
}