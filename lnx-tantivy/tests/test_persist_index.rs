use lnx_fs::VirtualFileSystem;
use lnx_tantivy::LnxIndex;
use tantivy::collector::TopDocs;
use tantivy::query::TermQuery;
use tantivy::schema::{IndexRecordOption, SchemaBuilder, Value, FAST, STORED, TEXT};
use tantivy::{doc, Term};

#[tokio::test]
async fn test_full_indexing_flow() {
    let _ = tracing_subscriber::fmt::try_init();

    let (vfs, _guard) = VirtualFileSystem::create_for_test().await.unwrap();

    let bucket = vfs.create_bucket("indexing_flow").await.unwrap();

    let mut builder = SchemaBuilder::new();
    let title = builder.add_text_field("title", TEXT | STORED);
    let body = builder.add_text_field("description", TEXT | STORED);
    let id = builder.add_u64_field("id", FAST | STORED);
    let schema = builder.build();

    let index = LnxIndex::create("full", bucket.clone(), schema)
        .await
        .unwrap();

    let mut indexer = index.new_indexer(1);
    indexer
        .add_document(doc!(
           id => 123u64,
           title => "The Old Man and the Sea",
           body => "He was an old man who fished alone in a skiff in \
                   the Gulf Stream and he had gone eighty-four days \
                   now without taking a fish."
        ))
        .unwrap();
    let memory = indexer.finish().unwrap();

    index.add_segment(memory).await.expect("Add segment");
    index.reload_readers().await;

    // Drop and re-open the index.
    drop(index);
    let index = LnxIndex::open("full", bucket).await.unwrap();
    
    let reader = index.reader();
    let searcher = reader.searcher();
    let results = tokio::task::spawn_blocking(move || {
        let query = TermQuery::new(
            Term::from_field_text(title, "man"),
            IndexRecordOption::WithFreqs,
        );
        searcher
            .search(&query, &TopDocs::with_limit(10))
            .expect("Search index")
    })
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    let searcher = reader.searcher();
    let doc: tantivy::TantivyDocument = searcher
        .doc_async(results[0].1)
        .await
        .expect("Fetch async doc");
    let id_field = doc.get_first(id).expect("Field should exist");
    assert_eq!(id_field.as_u64(), Some(123));
}
