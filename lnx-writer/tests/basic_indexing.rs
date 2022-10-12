use std::env::temp_dir;
use std::time::Duration;
use tantivy::doc;
use lnx_segments::{Delete, DeleteValue};
use lnx_writer::WriterSettings;

#[tokio::test]
async fn test_basic_indexing_flow() {
    lnx_writer::init(0, &temp_dir()).await;

    let app_ctx = lnx_common::test_utils::test_app_ctx();
    let (index_ctx, title, body) = lnx_common::test_utils::test_index_ctx();
    let settings = WriterSettings {
        num_threads: 1,
        auto_commit_duration: 2,
        ..Default::default()
    };

    let mut writer = lnx_writer::Writer::create(app_ctx, index_ctx, settings)
        .await
        .expect("Create new writer");


    let docs = vec![
        doc!(title => "Hello, world!", body => "This is some example document for me to test."),
        doc!(title => "Goodbye, world!", body => "This is some example document for me to test."),
    ];

    writer
        .add_documents(docs)
        .await
        .expect("Add documents to writer.");

    // Wait for auto commit.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(writer.stats().segments_produced(), 2, "Two segments should exist after the auto commit period has elapsed.");
    assert_eq!(writer.stats().documents_ingested(), 2, "Two documents should have been ingested.");
}

#[tokio::test]
async fn test_basic_delete_flow() {
    lnx_writer::init(0, &temp_dir()).await;

    let app_ctx = lnx_common::test_utils::test_app_ctx();
    let (index_ctx, _, _) = lnx_common::test_utils::test_index_ctx();
    let settings = WriterSettings {
        num_threads: 1,
        auto_commit_duration: 2,
        ..Default::default()
    };

    let mut writer = lnx_writer::Writer::create(app_ctx, index_ctx, settings)
        .await
        .expect("Create new writer");


    let docs = vec![
        Delete { field: "title".to_string(), value: DeleteValue::U64(0) },
        Delete { field: "title".to_string(), value: DeleteValue::U64(1) },
    ];

    writer
        .delete_documents(docs)
        .await
        .expect("Add documents to writer.");

    // Wait for auto commit.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(writer.stats().segments_produced(), 2, "Two segments should exist after the auto commit period has elapsed.");
    assert_eq!(writer.stats().deletes_registered(), 2, "Deletes should have been registered.");
}
