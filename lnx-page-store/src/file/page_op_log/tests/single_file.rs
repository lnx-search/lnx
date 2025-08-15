use std::sync::Arc;

use crate::file::page_op_log::file::LogFileWriter;
use crate::file::{ctx, scheduler};
use crate::layout::log::{LogEntry, LogOp};
use crate::layout::page_metadata::PageMetadata;
use crate::{PageFileId, PageId};

#[rstest::rstest]
#[case::no_encryption_1_entries(false, 1)]
#[case::no_encryption_4_entries(false, 4)]
#[case::no_encryption_8_entries(false, 8)]
#[case::no_encryption_32_entries(false, 32)]
#[case::yes_encryption_1_entries(false, 1)]
#[case::yes_encryption_4_entries(false, 4)]
#[case::yes_encryption_8_entries(false, 8)]
#[case::yes_encryption_32_entries(false, 32)]
#[tokio::test]
async fn test_log_writer_all_entries(
    #[case] encryption: bool,
    #[case] number_of_entries: usize,
) {
    let ctx = Arc::new(ctx::FileContext::for_test(encryption));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let mut writer = LogFileWriter::new(ctx, file, 0);

    for id in 0..number_of_entries {
        let entry = LogEntry {
            sequence_id: 0,
            last_flush_sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 0,
            page_file_id: PageFileId(id as u64),
            op: LogOp::Free,
        };
        writer.write_log(entry, None).await.expect("write log");
    }

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);
    let sequence_id = writer.flushed_sequence_id();
    assert_eq!(sequence_id, 0);

    writer.sync().await.expect("flush");

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);
    let sequence_id = writer.flushed_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);
}

#[rstest::rstest]
#[case::no_encryption_1_entries(false, 1)]
#[case::no_encryption_4_entries(false, 4)]
#[case::no_encryption_8_entries(false, 8)]
#[case::no_encryption_32_entries(false, 32)]
#[case::yes_encryption_1_entries(false, 1)]
#[case::yes_encryption_4_entries(false, 4)]
#[case::yes_encryption_8_entries(false, 8)]
#[case::yes_encryption_32_entries(false, 32)]
#[tokio::test]
async fn test_log_writer_entries_and_metadata(
    #[case] encryption: bool,
    #[case] number_of_entries: usize,
) {
    let ctx = Arc::new(ctx::FileContext::for_test(encryption));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let mut writer = LogFileWriter::new(ctx, file, 0);

    for id in 0..number_of_entries {
        let entry = LogEntry {
            sequence_id: 0,
            last_flush_sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 0,
            page_file_id: PageFileId(id as u64),
            op: LogOp::Free,
        };

        let metadata = PageMetadata {
            id: PageId(id as u32),
            ..PageMetadata::empty()
        };

        writer
            .write_log(entry, Some(metadata))
            .await
            .expect("write log");
    }

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);
    let sequence_id = writer.flushed_sequence_id();
    assert_eq!(sequence_id, 0);

    writer.sync().await.expect("flush");

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);
    let sequence_id = writer.flushed_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);
}
