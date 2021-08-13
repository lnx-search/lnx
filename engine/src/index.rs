use std::hash::{Hash, Hasher};
use std::sync::Arc;
use async_channel::{Sender, Receiver, TryRecvError};
use slab::Slab;
use parking_lot::RwLock;
use anyhow::{Result, Error};

use tantivy::{IndexWriter, Document};
use tantivy::schema::Schema;
use tantivy::{IndexBuilder, Index};

use crate::structures::{LoadedIndex, IndexStorageType};


#[derive(Debug)]
enum WriterOp {
    Commit,
    Rollback,
    AddDocument(Document),
    DeleteAll,
    __Shutdown
}


pub struct IndexWriterWorker {
    index_name: String,
    writer: IndexWriter,
    rx: Receiver<WriterOp>
}

impl IndexWriterWorker {
    async fn start(mut self) {
        while let Ok(msg) = self.rx.recv().await {
            match self.handle_msg(msg).await {
                Err(e) => error!(
                    "[ WRITER @ {} ] failed handling writer operation on index due to error: {:?}",
                    &self.index_name, e,
                ),
                Ok(shutdown) => if shutdown { break },
            }
        }
    }

    async fn handle_msg(&mut self, op: WriterOp) -> Result<bool> {
        let (transaction_id, type_) = match op {
            WriterOp::__Shutdown =>
                return Ok(true),
            WriterOp::Commit =>
                (self.writer.commit()?, "COMMIT"),
            WriterOp::Rollback =>
                (self.writer.rollback()?, "ROLLBACK"),
            WriterOp::AddDocument(docs) =>
                (self.writer.add_document(docs), "ADD-DOCUMENT"),
            WriterOp::DeleteAll =>
                (self.writer.delete_all_documents()?, "DELETE-ALL"),
        };

        info!(
            "[ WRITER @ {} ][ {} ] completed operation {}",
            &self.index_name, transaction_id, type_
        );

        Ok(false)
    }
}


pub struct IndexHandler {
    name: String,
    index: Index,
    schema: Schema,
    writer_sender: Sender<WriterOp>,
}

impl IndexHandler {
    pub async fn build_loaded(loader: LoadedIndex) -> Result<Self> {
        let (tx, rx) = async_channel::bounded(10);

        let index = IndexBuilder::default()
            .schema(loader.schema.clone());

        let index = match loader.storage_type {
            IndexStorageType::TempFile => index.create_from_tempdir()?,
            IndexStorageType::Memory => index.create_in_ram()?,
            IndexStorageType::FileSystem(path) => index.create_in_dir(path)?
        };

        let writer = index.writer_with_num_threads(
            loader.writer_threads,
            loader.writer_buffer
        )?;

        let worker = IndexWriterWorker {
            writer,
            rx,
            index_name: loader.name.clone(),
        };

        tokio::spawn(worker.start());

        Ok(Self {
            name: loader.name,
            index,
            schema: loader.schema,
            writer_sender: tx,
        })
    }
}
