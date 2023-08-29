mod clock;
mod document;
mod ops;
mod tracker;
mod queue;

pub type IndexWriter = tantivy::IndexWriter<document::IndexingDoc>;


