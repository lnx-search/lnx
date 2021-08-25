use anyhow::{Error, Result};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::fs;

use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, Value, NamedFieldDocument};
use tantivy::{Index, IndexBuilder, ReloadPolicy, Term, Document};

use crate::helpers::{self, hash};
use crate::index::reader::QueryHit;
use crate::structures::{FieldValue, IndexStorageType, LoadedIndex, QueryPayload};
use crate::correction;

pub(super) mod reader;
pub(super) mod writer;

static INDEX_DATA_PATH: &str = "./lnx/index-data";

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
    writer: writer::IndexWriterHandler,

    /// The index reader handler
    reader: reader::IndexReaderHandler,

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
        let worker_handler =
            writer::IndexWriterHandler::create(loader.name.clone(), writer, sender);

        let reader_handler = reader::IndexReaderHandler::create(
            loader.name.clone(),
            loader.max_concurrency as usize,
            reader,
            loader.reader_threads as usize,
            parser,
            loader.fuzzy_search_fields,
            schema_copy,
            loader.use_fast_fuzzy,
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
    pub async fn add_document(&self, mut document: NamedFieldDocument) -> Result<()> {
        let field = self.schema.get_field("_id").ok_or_else(|| {
            Error::msg(
                "system has not correctly initialised this schema,\
                 are you upgrading from a older version? If yes, you need to re-create the schema.",
            )
        })?;

        if correction::enabled() {
            helpers::correct_doc_fields(&mut document, self.indexed_fields());
        }

        let mut doc = self.schema.convert_named_doc(document)?;

        let id = uuid::Uuid::new_v4();
        doc.add_u64(field, hash(&id));

        self.writer
            .send_op(writer::WriterOp::AddDocument(doc))
            .await
    }

    /// Submits many documents to the index writer.
    ///
    /// This does have significant performance improvements when using
    /// the fast fuzzy system, as this does parallel correction vs
    /// linear.
    ///
    /// If fast fuzzy is not enabled however, this just calls add_docs in a loop.
    pub async fn add_many_documents(&self, documents: Vec<NamedFieldDocument>) -> Result<()> {
        let field = self.schema.get_field("_id").ok_or_else(|| {
            Error::msg(
                "system has not correctly initialised this schema,\
                 are you upgrading from a older version? If yes, you need to re-create the schema.",
            )
        })?;

        if !correction::enabled() {
            for doc in documents {
                self.add_document(doc).await?;
            }
            return Ok(())
        }

        let fields = Arc::new(self.indexed_fields().clone());
        let schema = self.schema.clone();
        let (tx, rx) = crossbeam::channel::unbounded();
        let handles: Vec<JoinHandle<Result<Vec<Document>>>> = (0..num_cpus::get())
            .map(|_| {
                let fields = fields.clone();
                let schema = schema.clone();
                let receiver = rx.clone();
                tokio::task::spawn_blocking(move || -> Result<Vec<Document>> {
                    let mut processed_documents = vec![];
                    while let Ok(mut doc) = receiver.recv() {
                        helpers::correct_doc_fields(&mut doc, fields.as_ref());
                        let doc = schema.convert_named_doc(doc)?;

                        processed_documents.push(doc);
                    }

                    Ok(processed_documents)
                })
            })
            .collect();

        for doc in documents {
            let _ = tx.send(doc)?;
        }
        drop(tx);

        for handle in handles {
            let documents = handle.await??;
            for mut doc in documents {
                let id = uuid::Uuid::new_v4();
                doc.add_u64(field, hash(&id));


                self.writer
                    .send_op(writer::WriterOp::AddDocument(doc))
                    .await?;
            }
        }

        Ok(())
    }

    /// Submits the delete all operation to the index writer.
    ///
    /// This will delete all documents in the index which were
    /// added since the last commit.
    pub async fn clear_documents(&self) -> Result<()> {
        self.writer.send_op(writer::WriterOp::DeleteAll).await
    }

    /// Submits the delete term operation to the index writer.
    ///
    /// This will delete all documents matching the term which were
    /// added since the last commit.
    pub async fn delete_documents_with_term(&self, term: Term) -> Result<()> {
        self.writer
            .send_op(writer::WriterOp::DeleteTerm(term))
            .await
    }

    /// Submits the commit operation to the index writer.
    ///
    /// This will finalize any operations and save the changes, flushing them
    /// to disk.
    ///
    /// Any additions and deletions will become visible to readers once
    /// the operation is complete.
    pub async fn commit(&self) -> Result<()> {
        self.writer.send_op(writer::WriterOp::Commit).await
    }

    /// Submits the rollback operation to the index writer.
    ///
    /// This will undo / drop any changes made between the last commit
    /// and the rollback operation.
    pub async fn rollback(&self) -> Result<()> {
        self.writer.send_op(writer::WriterOp::Rollback).await
    }

    /// Searches the index with the given query.
    pub async fn search(&self, payload: QueryPayload) -> Result<reader::QueryResults> {
        self.reader.search(payload).await
    }

    /// Clears all documents from the index and commits.
    pub async fn clear_and_commit(&self) -> Result<()> {
        self.clear_documents().await?;
        self.writer.send_op(writer::WriterOp::Commit).await?;

        Ok(())
    }

    /// Shuts down the index system cleaning up all pools.
    pub async fn shutdown(&self) -> Result<()> {
        self.writer.send_op(writer::WriterOp::__Shutdown).await?;

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
