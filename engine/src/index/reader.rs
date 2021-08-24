use std::sync::Arc;

use anyhow::{Error, Result};
use serde::Serialize;

use tokio::sync::oneshot;
use tokio::sync::Semaphore;

use crossbeam::queue::ArrayQueue;

use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, EmptyQuery, Occur, Query, QueryParser, TermQuery, FuzzyTermQuery};
use tantivy::query::{BoostQuery, MoreLikeThisQuery};
use tantivy::schema::{Field, FieldType, IndexRecordOption, NamedFieldDocument, Schema, Value};
use tantivy::{DocAddress, Executor, IndexReader, LeasedItem, Score, Searcher, Term};

use crate::correction::{self, correct_sentence};
use crate::structures::{QueryMode, QueryPayload};

/// Attempts to get a document otherwise sending an error
/// back to the resolve channel.
macro_rules! try_get_doc {
    ($resolve:expr, $searcher:expr, $doc:expr) => {{
        let res = $searcher.search(
            &TermQuery::new($doc, IndexRecordOption::Basic),
            &TopDocs::with_limit(1),
        );

        let res: Vec<(f32, DocAddress)> = match res {
            Err(e) => {
                let _ = $resolve.send(Err(Error::from(e)));
                return;
            }
            Ok(res) => res,
        };

        if res.len() == 0 {
            let _ = $resolve.send(Err(Error::msg("no document exists with this id")));
            return;
        }

        res[0].1
    }};
}

/// A async manager around the tantivy index reader.
///
/// This system executes the read operations in a given thread pool
/// managed by rayon which will allow a concurrency upto the set
/// `max_concurrency`.
///
/// If the system is at it's maximum concurrency already and search
/// is called again, it will temporarily suspend operations until
/// a reader has been freed.
///
/// This system will also spawn `n` executors with `y` reader threads
/// where `n` is the max concurrency set and `y` is the reader threads.
///
/// #### WARNING: HIGH THREAD USAGE
/// This system has the potential to spawn and incredibly large amount of
/// threads, when setting the `max_concurrency` and `reader_threads` the total
/// will result in `max_concurrency` * `reader_threads` threads spawned.
pub(super) struct IndexReaderHandler {
    /// The name of the index the handler belongs to.
    name: String,

    /// The internal tantivy index reader.
    reader: IndexReader,

    /// The reader thread pool executors.
    ///
    /// This creates n amount of executors equal to the max_concurrency
    /// **WARNING:** THIS CAN CAUSE AN *INSANE* AMOUNT OF THREADS TO BE SPAWNED.
    ///
    /// If the number of reader threads is > 1 this is a MultiThreaded executor
    /// otherwise it's SingleThreaded.
    executors: Arc<ArrayQueue<Executor>>,

    /// A concurrency semaphore.
    limiter: Semaphore,

    /// The maximum concurrency of searches at one time.
    max_concurrency: usize,

    /// The execution thread pool.
    thread_pool: rayon::ThreadPool,

    /// The configured query parser pre-weighted.
    parser: Arc<QueryParser>,

    /// The set of indexed fields to search in a given query.
    search_fields: Arc<Vec<(Field, Score)>>,

    /// A cheaply cloneable schema reference.
    schema: Schema,

    /// Whether or not to use the fast fuzzy symspell correction system or not.
    ///
    /// This greatly improves the performance of searching at the cost
    /// of document indexing time and memory usage (standard dict set uses 1.2GB generally).
    use_fast_fuzzy: bool,
}

impl IndexReaderHandler {
    /// Creates a new reader handler from an existing tantivy index reader.
    ///
    /// This will spawn a thread pool with `n` amount of threads equal
    /// to the set `max_concurrency`.
    pub(super) fn create(
        index_name: String,
        max_concurrency: usize,
        reader: IndexReader,
        reader_threads: usize,
        parser: QueryParser,
        search_fields: Vec<(Field, Score)>,
        schema_copy: Schema,
        use_fast_fuzzy: bool,
    ) -> Result<Self> {
        let limiter = Semaphore::new(max_concurrency);

        let name = index_name.clone();
        let thread_pool = {
            rayon::ThreadPoolBuilder::new()
                .num_threads(max_concurrency)
                .thread_name(move |n| format!("index-{}-worker-{}", name.clone(), n))
                .build()?
        };

        let executors = ArrayQueue::new(max_concurrency);
        for i in 0..max_concurrency {
            let executor = if reader_threads > 1 {
                info!(
                    "[ READER {} @ {} ] executor startup, mode: multi-threaded, threads: {}",
                    i, &index_name, reader_threads
                );
                Executor::multi_thread(reader_threads, "index-reader-")?
            } else {
                info!(
                    "[ READER {} @ {} ] executor startup, mode: single-threaded (no-op)",
                    i, &index_name,
                );
                Executor::single_thread()
            };

            if let Err(_) = executors.push(executor) {
                panic!("executor pool was full yet executor was in use, this is a bug.")
            };
        }

        let executors = Arc::new(executors);

        Ok(Self {
            name: index_name,
            reader,
            executors,
            limiter,
            max_concurrency,
            thread_pool,
            parser: Arc::new(parser),
            search_fields: Arc::new(search_fields),
            schema: schema_copy,
            use_fast_fuzzy,
        })
    }

    /// Gets a document with a given address.
    ///
    /// This counts as a concurrent action.
    pub(super) async fn get_doc(&self, doc_address: u64) -> Result<NamedFieldDocument> {
        let _permit = self.limiter.acquire().await?;

        let (resolve, waiter) = oneshot::channel();
        let searcher = self.reader.searcher();
        let field = self
            .schema
            .get_field("_id")
            .ok_or_else(|| Error::msg("missing a required private field, this is a bug."))?;

        self.thread_pool.spawn(move || {
            let term = Term::from_field_u64(field, doc_address);
            let doc = try_get_doc!(resolve, searcher, term);
            let doc = searcher.doc(doc).map_err(Error::from);
            let _ = resolve.send(doc);
        });

        let result = waiter.await??;
        let doc = self.schema.to_named_doc(&result);

        Ok(doc)
    }

    /// Shuts down the thread pools and acquires all permits
    /// shutting the index down.
    ///
    /// Thread pools are shutdown asynchronously via Rayon's handling.
    pub(super) async fn shutdown(&self) -> Result<()> {
        // Wait till all searches have been completed.
        let _ = self
            .limiter
            .acquire_many(self.max_concurrency as u32)
            .await?;
        self.limiter.close();

        while let Some(executor) = self.executors.pop() {
            drop(executor);
        }

        Ok(())
    }

    /// Searches the index with a given query.
    ///
    /// The index will use fuzzy matching based on levenshtein distance
    /// if set to true.
    pub(super) async fn search(&self, payload: QueryPayload) -> Result<QueryResults> {
        let _permit = self.limiter.acquire().await?;

        let (resolve, waiter) = oneshot::channel();

        let doc_id = match (self.schema.get_field("_id"), payload.document) {
            (None, _) => Err(Error::msg(
                "missing a required private field, this is a bug.",
            )),
            (_, None) => Ok(None),
            (Some(field), Some(doc_id)) => Ok(Some(Term::from_field_u64(field, doc_id))),
        }?;

        let order_by = if let Some(ref field) = payload.order_by {
            // We choose to ignore the order by if the field doesnt exist.
            // While this may be surprising to be at first as long as it's
            // document this should be fine.
            self.schema.get_field(field)
        } else {
            None
        };

        let schema = self.schema.clone();
        let parser = self.parser.clone();
        let limit = payload.limit;
        let offset = payload.offset;
        let mode = payload.mode;
        let fast_fuzzy = self.use_fast_fuzzy && correction::enabled();
        let search_fields = self.search_fields.clone();
        let searcher = self.reader.searcher();
        let executors = self.executors.clone();

        let start = std::time::Instant::now();
        self.thread_pool.spawn(move || {
            let executor = executors.pop().expect("get executor");

            let ref_doc = match doc_id {
                None => None,
                Some(doc) => {
                    let doc = try_get_doc!(resolve, searcher, doc);
                    Some(doc)
                }
            };

            let query =
                match parse_query(parser, search_fields, payload.query, ref_doc, payload.mode, fast_fuzzy) {
                    Err(e) => {
                        let _ = resolve.send(Err(e));
                        return;
                    }
                    Ok(q) => q,
                };

            let res = search(query, searcher, &executor, limit, offset, schema, order_by);

            // This can never happen right?
            if let Err(_) = executors.push(executor) {
                panic!("executor pool was full yet executor was in use, this is a bug.")
            };

            let _ = resolve.send(res);
        });

        let res = waiter.await??;
        info!(
            "[ SEARCH @ {} ] took {:?} with limit={}, mode={:?} and {} results total",
            &self.name,
            start.elapsed(),
            limit,
            mode,
            res.count
        );

        Ok(res)
    }
}

/// Generates a query from any of the 3 possible systems to
/// query documents.
fn parse_query(
    parser: Arc<QueryParser>,
    search_fields: Arc<Vec<(Field, Score)>>,
    query: Option<String>,
    ref_document: Option<DocAddress>,
    mode: QueryMode,
    use_fast_fuzzy: bool,
) -> Result<Box<dyn Query>> {
    let start = std::time::Instant::now();
    let out = match (mode, &query, ref_document) {
        (QueryMode::Normal, None, _) => Err(Error::msg(
            "query mode was `Normal` but query string is `None`",
        )),
        (QueryMode::Normal, Some(query), _) => Ok(parser.parse_query(query)?),
        (QueryMode::Fuzzy, None, _) => Err(Error::msg(
            "query mode was `Fuzzy` but query string is `None`",
        )),
        (QueryMode::Fuzzy, Some(query), _) => {
            let qry = if use_fast_fuzzy {
                parse_fuzzy_query(query, search_fields)
            } else {
                parse_fast_fuzzy_query(query, search_fields)?
            };
            Ok(qry)
        },
        (QueryMode::MoreLikeThis, _, None) => Err(Error::msg(
            "query mode was `MoreLikeThis` but reference document is `None`",
        )),
        (QueryMode::MoreLikeThis, _, Some(ref_document)) => Ok(parse_more_like_this(ref_document)),
    };

    debug!(
        "constructing query {:?} or ref_doc {:?} with mode={:?} took {:?}",
        query,
        ref_document,
        &mode,
        start.elapsed(),
    );

    return out;
}

/// Creates a fuzzy matching query, this allows for an element
/// of fault tolerance with spelling. This is the default
/// config as it its the most plug and play setup.
fn parse_fuzzy_query(
    query: &str,
    search_fields: Arc<Vec<(Field, Score)>>,
) -> Box<dyn Query> {
    let mut parts: Vec<(Occur, Box<dyn Query>)> = Vec::new();

    for search_term in query.to_lowercase().split(" ") {
        if search_term.is_empty() {
            continue;
        }

        for (field, boost) in search_fields.iter() {
            let query = Box::new(FuzzyTermQuery::new_prefix(
                Term::from_field_text(*field, search_term),
                1,
                true,
            ));

            if *boost > 0.0f32 {
                parts.push((Occur::Should, Box::new(BoostQuery::new(query, *boost))));
                continue;
            }

            parts.push((Occur::Should, query))
        }
    }

    Box::new(BooleanQuery::from(parts))
}

/// Uses the fast fuzzy system to match similar documents with
/// typo tolerance.
///
/// Unlike the standard fuzzy query which uses Levenshtein distance
/// this system uses pre-computation via symspell which is considerably
/// quicker than the standard method.
///
/// However, this requires additional private fields in the schema to
/// be affective with relevancy as names often get corrected to dictionary
/// words which alters the behaviour of the ranking.
/// To counter act this, the system runs the same correction on indexed
/// text fields to counter act this name handling issue.
fn parse_fast_fuzzy_query(
    query: &str,
    search_fields: Arc<Vec<(Field, Score)>>,
) -> Result<Box<dyn Query>> {
    if query.is_empty() {
        return Ok(Box::new(EmptyQuery {}));
    }

    let mut parts: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    for search_term in correct_sentence(query).split(" ") {
        for (field, boost) in search_fields.iter() {
            let term = Term::from_field_text(*field, &search_term);
            let query = Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));

            if *boost > 0.0f32 {
                parts.push((Occur::Should, Box::new(BoostQuery::new(query, *boost))));
                continue;
            }

            parts.push((Occur::Should, query));
        }
    }

    Ok(Box::new(BooleanQuery::from(parts)))
}

/// Generates a MoreLikeThisQuery which matches similar documents
/// as the given reference document.
fn parse_more_like_this(ref_document: DocAddress) -> Box<dyn Query> {
    let query = MoreLikeThisQuery::builder()
        .with_min_doc_frequency(1)
        .with_max_doc_frequency(10)
        .with_min_term_frequency(1)
        .with_min_word_length(2)
        .with_max_word_length(12)
        .with_boost_factor(1.0)
        .with_stop_words(vec!["for".to_string(), "the".to_string()])
        .with_document(ref_document);

    Box::new(query)
}

/// Represents a single query result.
#[derive(Serialize)]
pub struct QueryHit {
    /// The address of the given document, this can be used for
    /// 'more like this' queries.
    pub(super) document_id: String,

    /// The content of the document itself.
    pub(super) doc: NamedFieldDocument,

    /// The ratio calculated for the search term and doc.
    pub(super) ratio: serde_json::Value,
}

/// Represents the overall query result(s)
#[derive(Serialize)]
pub struct QueryResults {
    /// The retrieved documents.
    hits: Vec<QueryHit>,

    /// The total amount of documents matching the search
    count: usize,

    /// The amount of time taken to search in seconds.
    time_taken: f32,
}

macro_rules! order_and_search {
    ( $search:expr, $collector:expr, $field:expr, $query:expr, $executor:expr) => {{
        let collector = $collector.order_by_fast_field($field);
        $search.search_with_executor($query, &(collector, Count), $executor)
    }};
}

macro_rules! process_search {
    ( $search:expr, $schema:expr, $top_docs:expr ) => {{
        let mut hits = Vec::with_capacity($top_docs.len());
        for (ratio, ref_address) in $top_docs {
            let retrieved_doc = $search.doc(ref_address)?;
            let mut doc = $schema.to_named_doc(&retrieved_doc);
            let id = doc.0
                .remove("_id")
                .ok_or_else(|| Error::msg("document has been missed labeled (missing identifier tag), the dataset is invalid"))?;

            if let Value::U64(v) = id[0] {
                hits.push(QueryHit {
                    document_id: format!("{}", v),
                    doc,
                    ratio: serde_json::json!(ratio),
                });
            } else {
                return Err(Error::msg("document has been missed labeled (missing identifier tag), the dataset is invalid"))
            }
        }

        hits
    }};
}

/// Executes a search for a given query with a given searcher, limit and schema.
///
/// This will process and time the execution time to build into the exportable
/// data.
fn search(
    query: Box<dyn Query>,
    searcher: LeasedItem<Searcher>,
    executor: &Executor,
    limit: usize,
    offset: usize,
    schema: Schema,
    order_by: Option<Field>,
) -> Result<QueryResults> {
    let start = std::time::Instant::now();

    let collector = TopDocs::with_limit(limit).and_offset(offset);

    let (hits, count) = if let Some(field) = order_by {
        match schema.get_field_entry(field).field_type() {
            FieldType::I64(_) => {
                let out: (Vec<(i64, DocAddress)>, usize) =
                    order_and_search!(searcher, collector, field, &query, executor)?;
                (process_search!(searcher, schema, out.0), out.1)
            }
            FieldType::U64(_) => {
                let out: (Vec<(u64, DocAddress)>, usize) =
                    order_and_search!(searcher, collector, field, &query, executor)?;
                (process_search!(searcher, schema, out.0), out.1)
            }
            FieldType::F64(_) => {
                let out: (Vec<(f64, DocAddress)>, usize) =
                    order_and_search!(searcher, collector, field, &query, executor)?;
                (process_search!(searcher, schema, out.0), out.1)
            }
            FieldType::Date(_) => {
                let out: (Vec<(i64, DocAddress)>, usize) =
                    order_and_search!(searcher, collector, field, &query, executor)?;
                (process_search!(searcher, schema, out.0), out.1)
            }
            _ => return Err(Error::msg("field is not a fast field")),
        }
    } else {
        let (out, count) = searcher.search_with_executor(&query, &(collector, Count), executor)?;
        (process_search!(searcher, schema, out), count)
    };

    let elapsed = start.elapsed();
    let time_taken = elapsed.as_secs_f32();

    debug!(
        "thread runtime took {:?}s with limit: {} and {} results total",
        elapsed, limit, count
    );

    Ok(QueryResults {
        time_taken,
        hits,
        count,
    })
}
