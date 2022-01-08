use core::fmt;
use std::convert::TryInto;
use std::sync::Arc;

use anyhow::{Error, Result};
use serde::de::value::{MapAccessDeserializer, SeqAccessDeserializer};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use tantivy::collector::TopDocs;
use tantivy::query::{
    BooleanQuery,
    BoostQuery,
    EmptyQuery,
    FuzzyTermQuery,
    MoreLikeThisQuery,
    Query,
    QueryParser,
    TermQuery,
};
use tantivy::schema::{
    Facet,
    FacetParseError,
    Field,
    FieldEntry,
    FieldType,
    IndexRecordOption,
    Schema,
};
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};
use tantivy::{DateTime, Index, Score, Term};

use crate::corrections::SymSpellCorrectionManager;
use crate::stop_words::StopWordManager;
use crate::structures::DocumentValue;

pub type DocumentId = u64;

#[derive(Debug, Clone)]
pub(crate) struct QueryContext {
    pub(crate) set_conjunction_by_default: bool,
    pub(crate) use_fast_fuzzy: bool,
    pub(crate) strip_stop_words: bool,
    pub(crate) id_field: Field,
    pub(crate) default_search_fields: Vec<(Field, Score)>,
    pub(crate) fuzzy_search_fields: Vec<(Field, Score)>,
}

/// The query data for a given search requests.
///
/// This defines everything for a individual query
/// including it's occurrence rules, kind and value.
#[derive(Debug, Deserialize)]
pub struct QueryData {
    /// Defines the kind of query additional context for each query is
    /// contained within the kind.
    #[serde(flatten)]
    kind: QueryKind,

    /// Defines whether the query must be present,
    /// should be present or must be not present.
    #[serde(default)]
    occur: Occur,
}

/// The kind of query to perform.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueryKind {
    /// This is a fuzzy search.
    ///
    /// This will give a typo-tolerant aspect to the query; spelling mistakes
    /// within reason will be corrected and not invalidate all the results.
    ///
    /// Things like `trueman show` will match `the truman show`.
    Fuzzy { ctx: DocumentValue },

    /// The normal query search using the tantivy query parser.
    ///
    /// This will expect the given value to follow the query specification
    /// as defined in the tantivy docs.
    Normal { ctx: DocumentValue },

    /// Gets similar documents based on the reference document.
    ///
    /// This expects a document id as the value, anything else will be rejected.
    MoreLikeThis { ctx: DocumentValue },

    /// Get results matching the given term for the given field.
    Term {
        ctx: DocumentValue,
        fields: FieldSelector,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FieldSelector {
    Single(String),
    Multi(Vec<String>),
}

/// Defines whether a term in a query must be present,
/// should be present or must be not present.
#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Occur {
    /// For a given document to be considered for scoring,
    /// at least one of the document with the Should or the Must
    /// Occur constraint must be within the document.
    ///
    /// This is the default.
    Should,

    /// Document without the term are excluded from the search.
    Must,

    /// Document that contain the term are excluded from the
    /// search.
    MustNot,
}

impl Default for Occur {
    fn default() -> Self {
        Self::Should
    }
}

impl Occur {
    fn as_tantivy_value(&self) -> tantivy::query::Occur {
        use tantivy::query::Occur;

        match self {
            Self::Should => Occur::Should,
            Self::Must => Occur::Must,
            Self::MustNot => Occur::MustNot,
        }
    }
}

/// A helper selector that allows either individual querying or
/// multi queries.
#[derive(Debug)]
pub enum QuerySelector {
    /// A singular query.
    ///
    /// This just behaves as expected except that the `Occur` changes
    /// from `should` to `must` if applicable.
    Single(QueryData),

    /// Many queries.
    Multi(Vec<QueryData>),
}

impl QuerySelector {
    /// Consumes the selector and returns a list of queries to process.
    fn into_queries(self) -> Vec<QueryData> {
        match self {
            Self::Multi(queries) => queries,
            Self::Single(mut query) => {
                if let Occur::Should = &query.occur {
                    query.occur = Occur::Must;
                }

                vec![query]
            },
        }
    }
}

impl<'de> Deserialize<'de> for QuerySelector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct QuerySelectorVisitor;

        impl<'de> Visitor<'de> for QuerySelectorVisitor {
            type Value = QuerySelector;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "A single `DocumentPayload` or a list of `DocumentPayload`s",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(QuerySelector::Single(QueryData {
                    kind: QueryKind::Fuzzy {
                        ctx: DocumentValue::Text(v.to_string()),
                    },
                    occur: Occur::default(),
                }))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(QuerySelector::Single(QueryData {
                    kind: QueryKind::Fuzzy {
                        ctx: DocumentValue::Text(v),
                    },
                    occur: Occur::default(),
                }))
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                QueryData::deserialize(MapAccessDeserializer::new(map))
                    .map(QuerySelector::Single)
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                Vec::deserialize(SeqAccessDeserializer::new(seq))
                    .map(QuerySelector::Multi)
            }
        }

        deserializer.deserialize_any(QuerySelectorVisitor)
    }
}

/// A factory that builds a tantivy query based off of a given
/// payload.
#[derive(Clone)]
pub(crate) struct QueryBuilder {
    /// Relevant configuration settings.
    ctx: Arc<QueryContext>,

    /// The manager over the indexes' given stop words.
    stop_words: StopWordManager,

    /// The wrapping manager over the SymSpell correction system.
    corrections: SymSpellCorrectionManager,

    /// The schema of the index the handler belongs to.
    schema: Schema,

    /// The standard tantivy parser for `QueryKind::Normal` kinds.
    query_parser: Arc<QueryParser>,

    /// A thread pool that produces awaitable futures while executing.
    pool: crate::ReaderExecutor,

    /// A basic word tokenizers for fuzzy queries.
    tokenizer: TextAnalyzer,
}

impl QueryBuilder {
    /// Creates a new `QueryHandler` from the given parameters.
    pub(crate) fn new(
        ctx: QueryContext,
        stop_words: StopWordManager,
        corrections: SymSpellCorrectionManager,
        index: &Index,
        pool: crate::ReaderExecutor,
    ) -> Self {
        let parser = get_parser(&ctx, index);
        let tokenizer = TextAnalyzer::from(SimpleTokenizer).filter(LowerCaser);

        Self {
            ctx: Arc::new(ctx),
            corrections,
            stop_words,
            query_parser: Arc::new(parser),
            pool,
            schema: index.schema(),
            tokenizer,
        }
    }

    /// Builds a query from the given query selector.
    // TODO add-back #[instrument(name = "query-builder", level = "trace", skip_all)]
    pub(crate) async fn build_query(
        &self,
        selector: QuerySelector,
    ) -> Result<Box<dyn Query>> {
        let queries = selector.into_queries();

        let mut parts = Vec::with_capacity(queries.len());
        for query in queries {
            let occur = query.occur.as_tantivy_value();
            let built = self.get_query_from_payload(query).await?;

            parts.push((occur, built));
        }

        Ok(Box::new(BooleanQuery::new(parts)))
    }

    /// Gets a list of suggested corrections based off of the index corpus.
    pub(crate) fn get_corrections(&self, query: &str) -> Vec<String> {
        // TODO: reflect single output changes
        vec![self.corrections.correct(query)]
    }

    /// Gets the unique document id field.
    #[inline]
    pub(crate) fn id_field(&self) -> Field {
        self.ctx.id_field
    }

    /// Builds a query from the given query payload.
    async fn get_query_from_payload(&self, qry: QueryData) -> Result<Box<dyn Query>> {
        match qry.kind {
            QueryKind::Fuzzy { ctx: query } => self.make_fuzzy_query(query),
            QueryKind::Normal { ctx: query } => self.make_normal_query(query),
            QueryKind::MoreLikeThis { ctx: query } => {
                self.make_more_like_this_query(query).await
            },
            QueryKind::Term { ctx: query, fields } => {
                self.make_term_query(query, fields)
            },
        }
    }

    /// Produces a fuzzy query based off of the document value and
    /// the context of the handler.
    ///
    /// If `use_fast_fuzzy` is enabled both on server and index this will
    /// produce a fast-fuzzy query. Otherwise this will produce a feature
    /// fuzzy search.
    // TODO add-back #[instrument(name = "fuzzy-query", level = "trace", skip_all)]
    fn make_fuzzy_query(&self, value: DocumentValue) -> Result<Box<dyn Query>> {
        use tantivy::query::Occur;

        let mut query = value.as_string();
        if query.is_empty() {
            return Ok(Box::new(EmptyQuery {}));
        }

        if self.ctx.use_fast_fuzzy {
            query = self.corrections.correct(&query);
        }

        let mut parts: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        let mut words = vec![];
        let mut tokens = self.tokenizer.token_stream(&query);
        let mut ignore_stop_words = false;

        while let Some(token) = tokens.next() {
            words.push(token.text.to_string())
        }

        if self.ctx.strip_stop_words && words.len() > 1 {
            for word in words.iter() {
                if !self.stop_words.is_stop_word(word) {
                    ignore_stop_words = true;
                    break;
                }
            }
        }

        for search_term in words.iter() {
            if ignore_stop_words && self.stop_words.is_stop_word(search_term) {
                debug!("ignoring stop word {}", search_term);
                continue;
            }

            debug!("making fuzzy term for {}", search_term);
            for (field, boost) in self.ctx.fuzzy_search_fields.iter() {
                let term = Term::from_field_text(*field, search_term);

                let query: Box<dyn Query> = if self.ctx.use_fast_fuzzy {
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs))
                } else {
                    let edit_distance = if search_term.len() < 5 {
                        0
                    } else if search_term.len() < 9 {
                        1
                    } else {
                        2
                    };

                    Box::new(FuzzyTermQuery::new_prefix(term, edit_distance, true))
                };

                if *boost > 0.0f32 {
                    parts
                        .push((Occur::Should, Box::new(BoostQuery::new(query, *boost))));
                    continue;
                }

                parts.push((Occur::Should, query));
            }
        }

        Ok(Box::new(BooleanQuery::new(parts)))
    }

    /// Makes a new query by feeding the value into the tantivy QueryParser.
    // TODO add-back #[instrument(name = "normal-query", level = "trace", skip_all)]
    fn make_normal_query(&self, value: DocumentValue) -> Result<Box<dyn Query>> {
        let value = value.as_string();

        let query = match self.query_parser.parse_query(&value) {
            Ok(qry) => qry,
            Err(e) => return Err(Error::msg(format!("invalid query: {:?}", e))),
        };

        Ok(query)
    }

    /// Makes a new query that matches documents that are similar to a
    /// given reference document.
    ///
    /// The reference document should be referenced by it's id.
    // TODO add-back #[instrument(name = "more-like-this-query", level = "trace", skip_all)]
    async fn make_more_like_this_query(
        &self,
        value: DocumentValue,
    ) -> Result<Box<dyn Query>> {
        let id: DocumentId = value.try_into()?;

        let id_field = self.ctx.id_field;
        let address = self
            .pool
            .spawn(move |searcher, executor| {
                let qry = TermQuery::new(
                    Term::from_field_u64(id_field, id),
                    IndexRecordOption::Basic,
                );

                let mut results = searcher.search_with_executor(
                    &qry,
                    &TopDocs::with_limit(1),
                    executor,
                )?;
                if results.is_empty() {
                    return Err(Error::msg(format!(
                        "no document exists with id: '{}'",
                        id
                    )));
                }

                let (_, addr) = results.remove(0);

                Ok(addr)
            })
            .await??;

        let query = MoreLikeThisQuery::builder()
            .with_min_doc_frequency(1)
            .with_max_doc_frequency(10)
            .with_min_term_frequency(1)
            .with_min_word_length(2)
            .with_max_word_length(18)
            .with_boost_factor(1.0)
            .with_stop_words(self.stop_words.get_stop_words())
            .with_document(address);

        Ok(Box::new(query))
    }

    /// Makes a query based on a set term.
    ///
    /// This expects the value to match exactly with the term.
    ///
    /// This is useful for things like facet searches.
    // TODO add-back #[instrument(name = "term-query", level = "trace", skip_all)]
    fn make_term_query(
        &self,
        value: DocumentValue,
        field: FieldSelector,
    ) -> Result<Box<dyn Query>> {
        use tantivy::query::Occur;

        let fields = {
            match field {
                FieldSelector::Single(field) => {
                    vec![self.get_searchable_field(&field)?]
                },
                FieldSelector::Multi(fields) => {
                    let mut search_fields = Vec::with_capacity(fields.len());
                    for field in fields {
                        search_fields.push(self.get_searchable_field(&field)?);
                    }

                    search_fields
                },
            }
        };

        let mut queries: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(fields.len());
        for field in fields {
            let entry = self.schema.get_field_entry(field);
            let term = convert_to_term(value.clone(), field, entry)?;

            let query = TermQuery::new(term, IndexRecordOption::Basic);
            queries.push((Occur::Must, Box::new(query)));
        }

        Ok(Box::new(BooleanQuery::new(queries)))
    }

    fn get_searchable_field(&self, field: &str) -> Result<Field> {
        let field = self.schema.get_field(field).ok_or_else(|| {
            Error::msg(format!("no field exists with name: {:?}", field))
        })?;

        let entry = self.schema.get_field_entry(field);
        if !entry.is_indexed() {
            return Err(Error::msg(
                "the given field is not indexed and therefore not searchable",
            ));
        }

        Ok(field)
    }
}

fn get_parser(ctx: &QueryContext, index: &Index) -> QueryParser {
    let mut default_fields = vec![];
    for (field, _) in ctx.default_search_fields.iter() {
        default_fields.push(*field);
    }

    let mut parser = QueryParser::for_index(index, default_fields);
    for (field, boost) in ctx.default_search_fields.iter() {
        if *boost == 0f32 {
            continue;
        };
        parser.set_field_boost(*field, *boost);
    }

    if ctx.set_conjunction_by_default {
        parser.set_conjunction_by_default();
    }

    parser
}

fn convert_to_term(
    value: DocumentValue,
    field: Field,
    entry: &FieldEntry,
) -> Result<Term> {
    let term = match entry.field_type() {
        FieldType::U64(_) => Term::from_field_u64(field, value.try_into()?),
        FieldType::I64(_) => Term::from_field_i64(field, value.try_into()?),
        FieldType::F64(_) => Term::from_field_f64(field, value.try_into()?),
        FieldType::Str(_) => {
            let value: String = value.try_into()?;
            Term::from_field_text(field, &value)
        },
        FieldType::HierarchicalFacet(_) => {
            let facet: String = value.try_into()?;

            let facet = Facet::from_text(&facet).map_err(|e| {
                let FacetParseError::FacetParseError(e) = e;
                Error::msg(e)
            })?;

            Term::from_facet(field, &facet)
        },
        FieldType::Date(_) => {
            let dt: DateTime = value.try_into()?;
            Term::from_field_date(field, &dt)
        },
        _ => return Err(Error::msg("the given field is a unsupported type")),
    };

    Ok(term)
}
