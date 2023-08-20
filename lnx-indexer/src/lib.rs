mod clock;
mod document;
mod metastore;

pub type IndexWriter = tantivy::IndexWriter<document::IndexingDoc>;
