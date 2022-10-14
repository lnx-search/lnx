use std::mem;
use std::ops::Deref;
use std::path::PathBuf;

use datacake_crdt::HLCTimestamp;
use lnx_common::schema::WriterSettings;
use lnx_segments::{Delete, Deletes};
use lnx_storage::DirectoryWriter;
use tantivy::schema::Schema;
use tantivy::{Document, Index, IndexSettings, IndexWriter};

use crate::{WriterError, WriterStatistics};

/// The maximum capacity the segment should hold.
pub const MAX_INITIAL_SEGMENT_SIZE: usize = if cfg!(test) { 1 << 10 } else { 3 << 30 };

#[derive(Debug, Clone)]
pub struct IndexerFactory {
    index_name: String,
    schema: Schema,
    tmp_path: PathBuf,
    settings: WriterSettings,
}

impl IndexerFactory {
    pub fn new(
        index_name: String,
        schema: Schema,
        tmp_path: PathBuf,
        settings: WriterSettings,
    ) -> Self {
        Self {
            index_name,
            schema,
            tmp_path,
            settings,
        }
    }

    fn new_indexer(&self) -> Result<Indexer, WriterError> {
        let path = self.tmp_path.join(uuid::Uuid::new_v4().to_string());

        let dir = DirectoryWriter::create(&path)?;
        let index =
            Index::create(dir.clone(), self.schema.clone(), IndexSettings::default())?;
        let writer = index.writer_with_num_threads(
            self.settings.num_threads,
            self.settings.memory_buffer_size,
        )?;
        let segment_id = crate::clock().get_time_blocking();
        let index_name = self.index_name.clone();

        info!(
            tmp_path = ?path,
            segment_id = %segment_id,
            index = %index_name,
            num_threads = self.settings.num_threads,
            memory_allowance = self.settings.memory_buffer_size,
            "Creating new indexer."
        );

        Ok(Indexer {
            path,
            dir,
            index_name,
            writer,
            segment_id,
            deletes: Default::default(),
            num_documents: 0,
        })
    }
}

pub(crate) struct Indexer {
    path: PathBuf,
    pub(crate) dir: DirectoryWriter,
    pub(crate) index_name: String,
    pub(crate) writer: IndexWriter,
    pub(crate) segment_id: HLCTimestamp,
    pub(crate) deletes: Deletes,
    pub(crate) num_documents: usize,
}

impl Deref for Indexer {
    type Target = IndexWriter;

    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

impl Indexer {
    pub(crate) fn estimated_disk_usage(&self) -> usize {
        self.dir.estimated_disk_usage() + self.deletes.estimated_size_on_disk()
    }

    fn add_delete(&mut self, delete: Delete) {
        self.deletes.push(delete);
    }

    fn inc_doc_count(&mut self) {
        self.num_documents += 1;
    }
}

/// A indexing pipeline.
///
/// This manages creating and writing out a new segment to disk.
/// Once an existing segment is nearly full it will be sent off to a background
/// handler to finalize the commit and generate a new segment.
pub(crate) struct IndexerPipeline {
    factory: IndexerFactory,
    live_indexer: Indexer,
    stats: WriterStatistics,
}

impl IndexerPipeline {
    pub fn create(
        factory: IndexerFactory,
        stats: WriterStatistics,
    ) -> Result<Self, WriterError> {
        let live_indexer = factory.new_indexer()?;
        stats.inc_segments();

        Ok(Self {
            factory,
            live_indexer,
            stats,
        })
    }

    #[inline]
    pub fn index_name(&self) -> &str {
        self.live_indexer.index_name.as_str()
    }

    pub fn set_factory(&mut self, factory: IndexerFactory) {
        self.factory = factory;
    }

    pub fn add_documents(
        &mut self,
        docs: Vec<Document>,
    ) -> Result<HLCTimestamp, WriterError> {
        let num_docs = docs.len();
        for doc in docs {
            self.live_indexer.add_document(doc)?;
            self.live_indexer.inc_doc_count();
        }

        self.stats.inc_documents_by(num_docs);

        let segment_id = self.live_indexer.segment_id;

        self.check_then_flush_segment()?;

        Ok(segment_id)
    }

    pub fn add_deletes(
        &mut self,
        deletes: Vec<Delete>,
    ) -> Result<HLCTimestamp, WriterError> {
        let segment_id = self.live_indexer.segment_id;

        self.stats.inc_deletes_by(deletes.len());
        for delete in deletes {
            self.live_indexer.add_delete(delete);
        }

        self.check_then_flush_segment()?;

        Ok(segment_id)
    }

    pub fn flush_segment(&mut self) -> Result<(), WriterError> {
        let new_segment = self.factory.new_indexer()?;
        self.stats.inc_segments();

        let old_segment = mem::replace(&mut self.live_indexer, new_segment);

        crate::finalizer::submit_indexer(old_segment)
            .expect("Finalizer has shutdown leading to a stall in state processing.");

        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), WriterError> {
        let new_segment = self.factory.new_indexer()?;

        let old_segment = mem::replace(&mut self.live_indexer, new_segment);
        let writer = old_segment.writer;
        let path = old_segment.path;
        drop(old_segment.dir);
        writer.wait_merging_threads()?;

        let _ = std::fs::remove_dir(path);

        Ok(())
    }

    fn check_then_flush_segment(&mut self) -> Result<(), WriterError> {
        let disk_usage = self.live_indexer.estimated_disk_usage();
        if disk_usage >= MAX_INITIAL_SEGMENT_SIZE {
            let pretty = humansize::format_size(disk_usage, humansize::DECIMAL);
            info!(
                estimated_disk_usage = pretty,
                segment_id = %self.live_indexer.segment_id,
                "Segment is full! Flushing and sealing segment."
            );
            self.flush_segment()?;
        }

        Ok(())
    }
}
