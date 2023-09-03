mod document;
mod directory;

pub type IndexWriter = tantivy::IndexWriter<document::IndexingDoc>;
