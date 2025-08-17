use std::sync::Arc;

use rstest::rstest;

use crate::PageFileId;
use crate::file::ctx::associated_data;
use crate::file::page_op_log::file::LogFileWriter;
use crate::file::{DISK_ALIGN, ctx, scheduler};
use crate::layout::log;
use crate::layout::log::{LogEntry, LogOp};

#[rstest]
#[case::without_offset(0)]
#[case::with_offset(4096)]
#[tokio::test]
async fn test_single_block_correct_associated_data_tagging(#[case] log_offset: u64) {
    const FILE_ID: u64 = 1;

    let _ = tracing_subscriber::fmt::try_init();

    let ctx = Arc::new(ctx::FileContext::for_test(true));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let (file, path) = tmp_file.into_parts();

    let file = scheduler
        .make_ring_file(FILE_ID, file)
        .await
        .expect("Failed to make ring file");

    let mut writer = LogFileWriter::new(ctx.clone(), file, log_offset);

    let entry = LogEntry {
        sequence_id: 1,
        last_flush_sequence_id: 0,
        transaction_id: 6,
        transaction_n_entries: 7,
        page_file_id: PageFileId(1),
        op: LogOp::Free,
    };
    writer.write_log(entry, None).await.unwrap();
    writer.sync().await.unwrap();

    let mut content = std::fs::read(&path).expect("read log file");
    assert_eq!(content.len(), DISK_ALIGN + log_offset as usize);

    let buffer = &mut content[log_offset as usize..][..log::LOG_BLOCK_SIZE];
    let expected_associated_data = associated_data(FILE_ID, log_offset);
    log::decode_log_block(ctx.cipher(), &expected_associated_data, buffer)
        .expect("block should be decodable");
}

#[rstest]
#[case::without_offset(0)]
#[case::with_offset(4096)]
#[tokio::test]
async fn test_multi_block_correct_associated_data_tagging(#[case] log_offset: u64) {
    const FILE_ID: u64 = 1;

    let _ = tracing_subscriber::fmt::try_init();

    let ctx = Arc::new(ctx::FileContext::for_test(true));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let (file, path) = tmp_file.into_parts();

    let file = scheduler
        .make_ring_file(FILE_ID, file)
        .await
        .expect("Failed to make ring file");

    let mut writer = LogFileWriter::new(ctx.clone(), file, log_offset);

    for _ in 0..15 {
        let entry = LogEntry {
            sequence_id: 1,
            last_flush_sequence_id: 0,
            transaction_id: 6,
            transaction_n_entries: 7,
            page_file_id: PageFileId(1),
            op: LogOp::Free,
        };
        writer.write_log(entry, None).await.unwrap();
    }
    writer.sync().await.unwrap();

    let mut content = std::fs::read(&path).expect("read log file");
    assert_eq!(content.len(), DISK_ALIGN + log_offset as usize);

    let buffer = &mut content[log_offset as usize..][..log::LOG_BLOCK_SIZE];
    let expected_associated_data = associated_data(FILE_ID, log_offset);
    log::decode_log_block(ctx.cipher(), &expected_associated_data, buffer)
        .expect("block should be decodable");

    let buffer =
        &mut content[log_offset as usize + log::LOG_BLOCK_SIZE..][..log::LOG_BLOCK_SIZE];
    let expected_associated_data =
        associated_data(FILE_ID, log_offset + log::LOG_BLOCK_SIZE as u64);
    log::decode_log_block(ctx.cipher(), &expected_associated_data, buffer)
        .expect("block should be decodable");
}
