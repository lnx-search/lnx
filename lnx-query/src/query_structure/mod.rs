mod selector;
mod range;
mod base;
mod term_value;

use tantivy::Score;
use utoipa::ToSchema;
use serde::Deserialize;

use self::{selector::ValueSelector, range::Range};

pub use base::AsQueryTerm;


#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryKind {
    /// A query match document containing a single raw term.
    /// 
    /// This does not perform any scoring, it simply selects
    /// all documents that contain the given term.
    Term {
        // TODO: Have this be a term value for ints/floats/etc...
        #[schema(inline)]
        #[serde(flatten)]
        ctx: QueryContext<String>,
    },

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
    Phrase {
        #[schema(inline)]
        #[serde(flatten)]
        ctx: QueryContext<String>,
    },

    /// A `regex` query matches all of the documents containing a specific term that
    ///  matches a regex pattern.
    /// 
    /// Wildcard queries (e.g. `ho*se`) can be achieved by converting them to their 
    /// regex counterparts.
    Regex {
        #[schema(inline)]
        #[serde(flatten)]
        ctx: QueryContext<String>,
    },

    /// A query that matches all documents that have at least one term within
    /// a defined range.
    /// 
    /// Matched document will all get a constant Score of one.
    Range {
        #[schema(inline)]
        #[serde(flatten)]
        ctx: QueryContext<Range>,
    },

    /// An alias for a `term` query for better readability of queries.
    Facet {
        // TODO: Use custom facet validator
        #[schema(inline)]
        #[serde(flatten)]
        ctx: QueryContext<String>,
    },

    /// A query that matches all of the documents containing a specific
    /// set of terms that is within Levenshtein distance for each term. 
    /// (Typo-Tolerance)
    /// 
    /// This is typically what you want for user facing search queries when
    /// `fast-fuzzy` is unsuitable or unable to be used due to system resources constraints.
    Fuzzy {
        #[schema(inline)]
        #[serde(flatten)]
        ctx: QueryContext<String>,
    },
    
    /// A query that behaves similarly to the `fuzzy` query except using a pre-computation
    /// based algorithm.
    /// 
    /// This query requires `fast-fuzzy` mode being enabled on the index settings itself 
    /// (disabled by default) as this requires more memory when indexing and potentially
    /// when searching.
    FastFuzzy {
        #[schema(inline)]
        #[serde(flatten)]
        ctx: QueryContext<String>,
    },
}


#[derive(Debug, Deserialize, ToSchema)]
pub enum Occur<V> {
    #[serde(rename = "$should")]
    /// The documents in the doc set should match the query but it is not strictly required if another
    /// query matches as well.
    Should(V),

    #[serde(rename = "$must")]
    /// The documents in the doc set must match the query.
    Must(V),

    #[serde(rename = "$must_not")]
    /// The document in the doc set must not match the query.
    MustNot(V),
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct QueryContext<V> {
    #[schema(inline)]
    #[serde(flatten)]
    pub occur: Occur<ValueSelector<V>>,

    #[schema(inline)]
    #[serde(flatten)]
    pub fields: Option<FieldSelector>,

    #[schema(example = json!(1.5))]
    #[serde(default, rename = "$boost_by")]
    /// An optional score multiplier to adjust bias towards the query.
    pub boost: Score,
}


#[derive(Debug, Deserialize, ToSchema)]
pub enum FieldSelector {
    #[serde(rename = "$field")]
    /// Apply the query to a single specific field.
    Single(String),

    #[serde(rename = "$fields")]
    /// Apply the query to the specific fields.
    Multi(Vec<String>),
}