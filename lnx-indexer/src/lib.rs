mod clock;
mod document;

pub type IndexWriter = tantivy::IndexWriter<document::IndexingDoc>;
