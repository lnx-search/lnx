mod actors;
mod clock;
mod document;
mod pipeline;

pub(crate) type IndexWriter = tantivy::IndexWriter<document::IndexingDoc>;