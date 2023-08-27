mod clock;
mod document;
mod indexing_queue;
mod ops;
mod tracker;

pub type IndexWriter = tantivy::IndexWriter<document::IndexingDoc>;


