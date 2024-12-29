use lnx_fs::VirtualFileSystem;
use lnx_tantivy::LnxIndex;
use tantivy::doc;
use tantivy::indexer::NoMergePolicy;
use tantivy::schema::{SchemaBuilder, FAST, STORED, TEXT};

#[tokio::test]
async fn test_full_indexing_merges() {
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

    let mut indexer = index.new_indexer();
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

    let mut indexer = index.new_indexer();
    indexer
        .add_document(doc!(
           id => 1234u64,
           title => "The Old Man and the Sea",
           body => "He was an old man who fished alone in a skiff in \
                   the Gulf Stream and he had gone eighty-four days \
                   now without taking a fish."
        ))
        .unwrap();
    let memory = indexer.finish().unwrap();
    index.add_segment(memory).await.expect("Add segment");

    index.reload_readers().await;

    let segment_ids = index.searchable_segment_ids().await;
    assert_eq!(segment_ids.len(), 2);

    let seg_meta = index
        .merge(&segment_ids)
        .await
        .expect("Merge segments")
        .expect("New segment should be created");
    assert_eq!(seg_meta.num_docs(), 2);

    let segment_ids = index.searchable_segment_ids().await;
    assert_eq!(segment_ids.len(), 1);
}
