use std::env::temp_dir;
use std::time::Duration;

use lnx_common::schema::WriterSettings;
use lnx_writer::SegmentStatus;
use tantivy::doc;
use tokio::time::timeout;

#[tokio::test]
async fn test_segment_flushing() {
    let _ = tracing_subscriber::fmt::try_init();

    lnx_writer::init(0, &temp_dir()).await;
    let mut status = lnx_writer::subscribe().expect("Subscribe to finalizer.");

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

    // This will block until auto commit flushes the changes out.
    let (segment_id, status) = timeout(Duration::from_secs(20), status.recv())
        .await
        .unwrap()
        .expect("Get latest change.");

    assert_eq!(segment_id.node(), 0, "Node id should match provided id.");
    assert_eq!(
        status,
        SegmentStatus::Success,
        "Segment status should be marked as a success."
    );
}
