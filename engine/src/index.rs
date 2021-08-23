use std::sync::Arc;

use anyhow::{Error, Result};
use parking_lot::Mutex;
use serde::Serialize;

use tokio::fs;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;

use crossbeam::channel;
use crossbeam::queue::{ArrayQueue, SegQueue};

use tantivy::collector::{Count, TopDocs};
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, EmptyQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::query::{BoostQuery, MoreLikeThisQuery};
use tantivy::schema::{Field, FieldType, IndexRecordOption, NamedFieldDocument, Schema, Value};
use tantivy::{
    DocAddress, Document, Executor, Index, IndexBuilder, IndexReader, IndexWriter, LeasedItem,
    ReloadPolicy, Score, Searcher, Term,
};

use crate::correction::correct_sentence;
use crate::helpers::hash;
use crate::structures::{FieldValue, IndexStorageType, LoadedIndex, QueryMode, QueryPayload};

static INDEX_DATA_PATH: &str = "./lnx/index-data";

/// A writing operation to be sent to the `IndexWriterWorker`.
#[derive(Debug)]
enum WriterOp {
    /// Commits the current changes and flushes to storage.
    Commit,

    /// Removes any changes since the last commit.
    Rollback,

    /// Adds a document to the index.
    AddDocument(Document),

    /// Deletes any documents matching the given term.
    DeleteTerm(Term),

    /// Removes all documents from the index.
    DeleteAll,

    /// Shutdown the handler.
    __Shutdown,
}

/// A background task that applies write operations to the index.
///
/// This system uses the actor model receiving a stream of messages
/// and processes them in order of being sent.
///
/// Messages are ran in a new thread.
pub struct IndexWriterWorker {
    index_name: String,
    writer: IndexWriter,
    waiters: Arc<SegQueue<oneshot::Sender<()>>>,
    rx: channel::Receiver<WriterOp>,
    shutdown: async_channel::Sender<()>,
}

impl IndexWriterWorker {
    /// Starts processing messages until a shutdown operation is sent.
    ///
    /// This processes operations in waves before waking up waiters,
    /// this means all operations currently in the queue will be processed
    /// first before any waiters are woken up to send more data.
    fn start(mut self) {
        loop {
            if self.process_messages() {
                break;
            };

            // Wake up waiters once a message has been removed.
            while let Some(waiter) = self.waiters.pop() {
                let _ = waiter.send(());
            }
        }

        // Unlock waiters so that they dont deadlock the system.
        while let Some(waiter) = self.waiters.pop() {
            let _ = waiter.send(());
        }

        let _ = self.shutdown.try_send(());
        info!("[ WRITER @ {} ] shutdown complete!", &self.index_name);
    }

    /// Purges all pending operations from the receiver.
    fn process_messages(&mut self) -> bool {
        while let Ok(msg) = self.rx.try_recv() {
            debug!(
                "[ WRITER @ {} ] handling operation {:?}",
                &self.index_name, msg
            );
            match self.handle_msg(msg) {
                Err(e) => error!(
                    "[ WRITER @ {} ] failed handling writer operation on index due to error: {:?}",
                    &self.index_name, e,
                ),
                Ok(true) => return true,
                _ => {}
            }
        }

        false
    }

    fn handle_msg(&mut self, op: WriterOp) -> Result<bool> {
        let (transaction_id, type_) = match op {
            WriterOp::__Shutdown => return Ok(true),
            WriterOp::Commit => (self.writer.commit()?, "COMMIT"),
            WriterOp::Rollback => (self.writer.rollback()?, "ROLLBACK"),
            WriterOp::AddDocument(docs) => (self.writer.add_document(docs), "ADD-DOCUMENT"),
            WriterOp::DeleteAll => (self.writer.delete_all_documents()?, "DELETE-ALL"),
            WriterOp::DeleteTerm(term) => (self.writer.delete_term(term), "DELETE-TERM"),
        };

        info!(
            "[ WRITER @ {} ][ TRANSACTION {} ] completed operation {}",
            &self.index_name, transaction_id, type_
        );

        Ok(false)
    }
}

/// A simple wrapper handler around a set of queues and a worker.
///
/// This manages creating the waiters and scheduling the operations
/// in a new thread.
struct IndexWriterHandler {
    index_name: String,
    writer_waiters: Arc<SegQueue<oneshot::Sender<()>>>,
    writer_sender: crossbeam::channel::Sender<WriterOp>,
}

impl IndexWriterHandler {
    /// Creates a new writer handler from a given index name and
    /// a given index writer.
    ///
    /// This creates a bounded queue with a capacity of 20 and
    /// spawns a worker in a new thread.
    fn create(
        index_name: String,
        writer: IndexWriter,
        shutdown: async_channel::Sender<()>,
    ) -> Self {
        let name = index_name.clone();
        let waiters = Arc::new(SegQueue::new());
        let (tx, rx) = channel::bounded(20);
        let worker = IndexWriterWorker {
            index_name: index_name.clone(),
            writer,
            waiters: waiters.clone(),
            rx,
            shutdown,
        };

        std::thread::Builder::new()
            .name(format!("index-worker-{}", &index_name))
            .spawn(move || {
                let id = std::thread::current().id();
                info!(
                    "[ WRITER @ {} ] writer thread started with id {:?}",
                    name, id
                );
                worker.start()
            })
            .expect("spawn worker thread");

        Self {
            index_name,
            writer_sender: tx,
            writer_waiters: waiters,
        }
    }

    /// Sends a message to the writer worker
    ///
    /// If there is space in the queue this will complete immediately
    /// otherwise this will wait until it's woken up again.
    async fn send_op(&self, op: WriterOp) -> anyhow::Result<()> {
        let mut op = op;
        loop {
            op = match self.writer_sender.try_send(op) {
                Ok(()) => return Ok(()),
                Err(channel::TrySendError::Disconnected(_)) => {
                    return Err(Error::msg("writer worker has shutdown"))
                }
                Err(channel::TrySendError::Full(v)) => v,
            };

            debug!(
                "[ WRITER @ {} ] operation queue full, waiting for wakeup",
                &self.index_name
            );

            let (resolve, waiter) = oneshot::channel();
            self.writer_waiters.push(resolve);
            let _ = waiter.await;
        }
    }
}

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
struct IndexReaderHandler {
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
}

impl IndexReaderHandler {
    /// Creates a new reader handler from an existing tantivy index reader.
    ///
    /// This will spawn a thread pool with `n` amount of threads equal
    /// to the set `max_concurrency`.
    fn create(
        index_name: String,
        max_concurrency: usize,
        reader: IndexReader,
        reader_threads: usize,
        parser: QueryParser,
        search_fields: Vec<(Field, Score)>,
        schema_copy: Schema,
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
        })
    }

    /// Gets a document with a given address.
    ///
    /// This counts as a concurrent action.
    async fn get_doc(&self, doc_address: u64) -> Result<NamedFieldDocument> {
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
    async fn shutdown(&self) -> Result<()> {
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
    async fn search(&self, payload: QueryPayload) -> Result<QueryResults> {
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
                match parse_query(parser, search_fields, payload.query, ref_doc, payload.mode) {
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
        (QueryMode::Fuzzy, Some(query), _) => Ok(parse_fuzzy_query(query, search_fields)?),
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
    document_id: String,

    /// The content of the document itself.
    doc: NamedFieldDocument,

    /// The ratio calculated for the search term and doc.
    ratio: serde_json::Value,
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

    let time_taken = start.elapsed().as_secs_f32();

    debug!(
        "thread runtime took {:.2}s with limit: {} and {} results total",
        time_taken, limit, count
    );

    Ok(QueryResults {
        time_taken,
        hits,
        count,
    })
}

/// Converts an array of items into selecting one or rejecting
/// it all together.
macro_rules! add_values_to_terms {
    ($t:ident::$cb:ident, $field:expr, &$sv:expr) => {{
        if $sv.len() == 0 {
            Err(Error::msg("field must have one value"))
        } else {
            Ok($t::$cb($field, &$sv[0]))
        }
    }};

    ($t:ident::$cb:ident, $field:expr, $sv:expr) => {{
        if $sv.len() == 0 {
            Err(Error::msg("field must have one value"))
        } else {
            Ok($t::$cb($field, $sv[0]))
        }
    }};
}

/// A search engine index.
///
/// Each index maintains a rayon thread pool which searches are executed
/// as well as an worker thread which is used to interact with the index writer.
///
/// The amount of threads `n` is determined by the the `max_concurrency` parameter.
///
/// ### Mutating the index behaviour:
/// This system simple schedules the operations in the order they are invoked
/// however, this system does not wait for the operation to be completed.
/// This essentially follows the behaviour of eventual consistency; The operations
/// are guaranteed to be applied within some time in the near future.
pub struct IndexHandler {
    /// The name of the index.
    pub(crate) name: String,

    /// The internal tantivy index.
    _index: Mutex<Option<Index>>,

    /// The internal tantivy schema.
    schema: Schema,

    /// A writer actor to handle the index writer.
    writer: IndexWriterHandler,

    /// The index reader handler
    reader: IndexReaderHandler,

    /// An indicator if the system is still alive or not
    alive: async_channel::Receiver<()>,

    /// The optional storage directory of the index.
    dir: Option<String>,

    /// The set of fields which are indexed.
    indexed_text_fields: Vec<String>,
}

impl IndexHandler {
    /// Gets a tantivy Index either from an existing directory or
    /// makes a new system.
    async fn get_index_from_loader(loader: &LoadedIndex) -> Result<(Index, Option<String>)> {
        if let IndexStorageType::FileSystem = &loader.storage_type {
            let path = format!("{}/{}", INDEX_DATA_PATH, &loader.name);
            if std::path::Path::new(&path).exists() {
                info!(
                    "[ SETUP @ {} ] using existing schema metadata",
                    &loader.name
                );
                return Ok((Index::open_in_dir(&path)?, Some(path.clone())));
            }
        }

        let index = IndexBuilder::default().schema(loader.schema.clone());

        let out = match &loader.storage_type {
            IndexStorageType::TempDir => {
                info!(
                    "[ SETUP @ {} ] creating index in a temporary directory",
                    &loader.name
                );
                (index.create_from_tempdir()?, None)
            }
            IndexStorageType::Memory => {
                info!("[ SETUP @ {} ] creating index in memory", &loader.name);
                (index.create_in_ram()?, None)
            }
            IndexStorageType::FileSystem => {
                info!("[ SETUP @ {} ] creating index in directory", &loader.name);

                let path = format!("{}/{}", INDEX_DATA_PATH, &loader.name);
                fs::create_dir_all(&path).await?;

                let dir = MmapDirectory::open(&path)?;
                (index.open_or_create(dir)?, Some(path.clone()))
            }
        };

        Ok(out)
    }

    /// Creates a new index handler from a given loaded index.
    ///
    /// This constructs both the Tantivy index, thread pool and worker thread.
    ///
    /// ### Important note about performance:
    /// The concurrency limit should be set according to the machine
    /// this system is being deployed on hence being a required field.
    /// The amount of threads spawned is equal the the (`max_concurrency` * `reader_threads`) + `1`
    /// as well as the tokio runtime threads.
    pub(crate) async fn build_loaded(loader: LoadedIndex) -> Result<Self> {
        let schema_copy = loader.schema.clone();

        let (index, dir) = Self::get_index_from_loader(&loader).await?;

        // We need to extract out the fields from name to id.
        let mut raw_search_fields = vec![];
        let mut search_fields = vec![];
        for ref_field in loader.search_fields {
            let id = format!("_{}", hash(&ref_field));

            // This checks if a search field is a indexed text field (it has a private field)
            // that's used internally, since we pre-compute the correction behaviour before
            // hand, we want to actually target those fields not the inputted fields.
            match (
                loader.schema.get_field(&ref_field),
                loader.schema.get_field(&id),
            ) {
                (Some(_), Some(field)) => {
                    raw_search_fields.push(field);

                    if let Some(boost) = loader.boost_fields.get(&ref_field) {
                        debug!("boosting field for query parser {} {}", &ref_field, boost);
                        search_fields.push((field, *boost));
                    } else {
                        search_fields.push((field, 0.0f32));
                    };
                }
                (Some(field), None) => {
                    if let Some(boost) = loader.boost_fields.get(&ref_field) {
                        debug!("boosting field for query parser {} {}", &ref_field, boost);
                        search_fields.push((field, *boost));
                    } else {
                        search_fields.push((field, 0.0f32));
                    };
                }
                (None, _) => {
                    let fields: Vec<String> = loader
                        .schema
                        .fields()
                        .map(|(_, v)| v.name().to_string())
                        .collect();

                    return Err(Error::msg(format!(
                        "you defined the schema with the following fields: {:?} \
                        and declared the a search_field {:?} but this does not exist in the defined fields.",
                        fields, &ref_field
                    )));
                }
            };
        }

        let mut parser = QueryParser::for_index(&index, raw_search_fields);
        for (field, boost) in search_fields.iter() {
            if *boost != 0.0f32 {
                parser.set_field_boost(*field, *boost);
            }
        }

        let writer = index.writer_with_num_threads(loader.writer_threads, loader.writer_buffer)?;
        info!(
            "[ WRITER @ {} ] index writer has been allocated with {} threads and {} byte allocation",
            &loader.name ,loader.writer_threads, loader.writer_buffer
        );

        let reader = index
            .reader_builder()
            .num_searchers(loader.max_concurrency as usize)
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        info!(
            "[ READER @ {} ] index reader has been allocated with {} searchers",
            &loader.name, loader.max_concurrency
        );

        let (sender, receiver) = async_channel::bounded(1);
        let worker_handler = IndexWriterHandler::create(loader.name.clone(), writer, sender);

        let reader_handler = IndexReaderHandler::create(
            loader.name.clone(),
            loader.max_concurrency as usize,
            reader,
            loader.reader_threads as usize,
            parser,
            loader.fuzzy_search_fields,
            schema_copy,
        )?;

        Ok(Self {
            name: loader.name,
            _index: Mutex::new(Some(index)),
            schema: loader.schema,
            writer: worker_handler,
            reader: reader_handler,
            alive: receiver,
            dir,
            indexed_text_fields: loader.indexed_text_fields,
        })
    }

    #[inline]
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    #[inline]
    pub fn indexed_fields(&self) -> &Vec<String> {
        &self.indexed_text_fields
    }

    /// Builds a `Term` from a given field and value.
    ///
    /// This assumes that the value type matches up with the field type.
    pub fn get_term(&self, field: &str, value: FieldValue) -> Result<Term> {
        let field = self
            .schema
            .get_field(field)
            .map(|v| Ok(v))
            .unwrap_or_else(|| Err(Error::msg("unknown field")))?;

        let v = match value {
            FieldValue::I64(v) => add_values_to_terms!(Term::from_field_i64, field, v)?,
            FieldValue::F64(v) => add_values_to_terms!(Term::from_field_f64, field, v)?,
            FieldValue::U64(v) => add_values_to_terms!(Term::from_field_u64, field, v)?,
            FieldValue::Datetime(v) => add_values_to_terms!(Term::from_field_date, field, &v)?,
            FieldValue::Text(v) => add_values_to_terms!(Term::from_field_text, field, &v)?,
        };

        Ok(v)
    }

    /// Gets a document with a given document address.
    ///
    /// This uses a concurrency permit while completing the operation.
    pub async fn get_doc(&self, doc_address: u64) -> Result<QueryHit> {
        let mut doc = self.reader.get_doc(doc_address).await?;

        let id = doc.0.remove("_id").ok_or_else(|| {
            Error::msg(
                "document has been missed labeled (missing identifier tag), the dataset is invalid",
            )
        })?;

        if let Value::U64(v) = id[0] {
            Ok(QueryHit {
                document_id: format!("{}", v),
                doc,
                ratio: serde_json::json!(100.0),
            })
        } else {
            Err(Error::msg(
                "document has been missed labeled (missing identifier tag), the dataset is invalid",
            ))
        }
    }

    /// Submits a document to be processed by the index writer.
    pub async fn add_document(&self, mut document: Document) -> Result<()> {
        let field = self.schema.get_field("_id").ok_or_else(|| {
            Error::msg(
                "system has not correctly initialised this schema,\
                 are you upgrading from a older version? If yes, you need to re-create the schema.",
            )
        })?;

        let id = uuid::Uuid::new_v4();
        document.add_u64(field, hash(&id));

        self.writer.send_op(WriterOp::AddDocument(document)).await
    }

    /// Submits many documents to the index writer.
    ///
    /// This is just an alias for adding documents in a loop.
    pub async fn add_many_documents(&self, documents: Vec<Document>) -> Result<()> {
        for doc in documents {
            self.add_document(doc).await?
        }

        Ok(())
    }

    /// Submits the delete all operation to the index writer.
    ///
    /// This will delete all documents in the index which were
    /// added since the last commit.
    pub async fn clear_documents(&self) -> Result<()> {
        self.writer.send_op(WriterOp::DeleteAll).await
    }

    /// Submits the delete term operation to the index writer.
    ///
    /// This will delete all documents matching the term which were
    /// added since the last commit.
    pub async fn delete_documents_with_term(&self, term: Term) -> Result<()> {
        self.writer.send_op(WriterOp::DeleteTerm(term)).await
    }

    /// Submits the commit operation to the index writer.
    ///
    /// This will finalize any operations and save the changes, flushing them
    /// to disk.
    ///
    /// Any additions and deletions will become visible to readers once
    /// the operation is complete.
    pub async fn commit(&self) -> Result<()> {
        self.writer.send_op(WriterOp::Commit).await
    }

    /// Submits the rollback operation to the index writer.
    ///
    /// This will undo / drop any changes made between the last commit
    /// and the rollback operation.
    pub async fn rollback(&self) -> Result<()> {
        self.writer.send_op(WriterOp::Rollback).await
    }

    /// Searches the index with the given query.
    pub async fn search(&self, payload: QueryPayload) -> Result<QueryResults> {
        self.reader.search(payload).await
    }

    /// Clears all documents from the index and commits.
    pub async fn clear_and_commit(&self) -> Result<()> {
        self.clear_documents().await?;
        self.writer.send_op(WriterOp::Commit).await?;

        Ok(())
    }

    /// Shuts down the index system cleaning up all pools.
    pub async fn shutdown(&self) -> Result<()> {
        self.writer.send_op(WriterOp::__Shutdown).await?;

        debug!("[ ENGINE ] waiting on reader shutdown...");
        self.reader.shutdown().await?;

        debug!("[ ENGINE ] waiting on writer shutdown...");
        self.alive.recv().await?;

        let item = self._index.lock().take();
        drop(item); // lets see if this closes the dir?

        debug!("[ ENGINE ] cleaning up directory");
        if let Some(dir) = self.dir.as_ref() {
            fs::remove_dir_all(dir).await?;
        }
        Ok(())
    }
}
