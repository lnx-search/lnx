use std::io;
use std::io::ErrorKind;
use std::sync::Arc;

use crate::file::page_op_log::op_log_associated_data;
use crate::file::page_op_log::writer::LogFileWriter;
use crate::file::{DISK_ALIGN, ctx, scheduler};
use crate::layout::log;
use crate::layout::log::{LogEntry, LogOp};
use crate::layout::page_metadata::PageMetadata;
use crate::{PageFileId, PageId};

#[tokio::test]
async fn test_close() {
    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let mut writer = LogFileWriter::new(ctx, file, 0);
    writer
        .close()
        .await
        .expect("writer should close gracefully");
}

#[tokio::test]
async fn test_auto_flush() {
    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let initial_len = file.get_len().await.unwrap();
    assert_eq!(initial_len, 0);

    let mut writer = LogFileWriter::new(ctx, file, 0);

    let entry = LogEntry {
        sequence_id: 0,
        transaction_id: 0,
        transaction_n_entries: 0,
        page_id: PageId(1),
        page_file_id: PageFileId(1),
        op: LogOp::Free,
    };

    for _ in 0..5_000 {
        writer.write_log(entry, None).await.expect("write entry");
    }

    let file = writer.into_ring_file();
    let post_write_len = file.get_len().await.unwrap();
    assert_eq!(post_write_len, 128 << 10); // Flush single mem buffer.
}

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_all_entries_flush(
    #[values(true, false)] encryption: bool,
    #[values(1, 4, 8, 32)] number_of_entries: usize,
) {
    let ctx = Arc::new(ctx::FileContext::for_test(encryption));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let initial_len = file.get_len().await.unwrap();
    assert_eq!(initial_len, 0);

    let mut writer = LogFileWriter::new(ctx, file, 0);

    for id in 0..number_of_entries {
        let entry = LogEntry {
            sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 0,
            page_id: PageId(1),
            page_file_id: PageFileId(id as u32),
            op: LogOp::Free,
        };
        writer.write_log(entry, None).await.expect("write log");
    }

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);

    writer.sync().await.expect("flush");

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);

    let file = writer.into_ring_file();
    let post_flush_len = file.get_len().await.unwrap();
    assert_eq!(post_flush_len, DISK_ALIGN as u64);
}

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_entries_and_metadata(
    #[values(false, true)] encryption: bool,
    #[values(1, 4, 8, 32)] number_of_entries: usize,
) {
    let ctx = Arc::new(ctx::FileContext::for_test(encryption));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let initial_len = file.get_len().await.unwrap();
    assert_eq!(initial_len, 0);

    let mut writer = LogFileWriter::new(ctx, file, 0);

    for id in 0..number_of_entries {
        let entry = LogEntry {
            sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 0,
            page_id: PageId(1),
            page_file_id: PageFileId(id as u32),
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

    writer.sync().await.expect("flush");

    let sequence_id = writer.current_sequence_id();
    assert_eq!(sequence_id, number_of_entries as u32);

    let file = writer.into_ring_file();
    let post_flush_len = file.get_len().await.unwrap();
    assert_eq!(post_flush_len, DISK_ALIGN as u64);
}

#[tokio::test]
async fn test_no_close_on_write_error_but_lockout() {
    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let scenario = fail::FailScenario::setup();
    fail::cfg("ringfile::submit_write", "return").unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let mut writer = LogFileWriter::new(ctx, file, 0);
    let error = writer.sync().await.expect_err("write should error");
    assert_eq!(error.kind(), ErrorKind::Other);

    let entry = LogEntry {
        sequence_id: 0,
        transaction_id: 0,
        transaction_n_entries: 0,
        page_id: PageId(1),
        page_file_id: PageFileId(1),
        op: LogOp::Free,
    };
    let err = writer
        .write_log(entry, None)
        .await
        .expect_err("write should return lockout");
    assert_eq!(err.kind(), ErrorKind::ReadOnlyFilesystem);
    assert_eq!(err.to_string(), "writer is locked due to prior error");

    scenario.teardown();
}

#[tokio::test]
async fn test_propagate_lockout_error() {
    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let scenario = fail::FailScenario::setup();
    fail::cfg("ringfile::fdatasync", "return").unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let mut writer = LogFileWriter::new(ctx, file, 0);
    let error = writer.sync().await.expect_err("sync should error");
    assert_eq!(error.kind(), ErrorKind::Other);

    let entry = LogEntry {
        sequence_id: 0,
        transaction_id: 0,
        transaction_n_entries: 0,
        page_id: PageId(1),
        page_file_id: PageFileId(1),
        op: LogOp::Free,
    };
    let error = writer
        .write_log(entry, None)
        .await
        .expect_err("write should error");
    assert_eq!(error.kind(), ErrorKind::ReadOnlyFilesystem);

    scenario.teardown();
}

#[tokio::test]
async fn test_flush_mem_buffer_i2o2_error() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::try_get_result", "return(-12)").unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");
    let mut writer = LogFileWriter::new(ctx, file, 0);

    let error = fill_buffer(&mut writer)
        .await
        .expect_err("write should error");
    assert_eq!(error.kind(), ErrorKind::OutOfMemory);

    assert!(!writer.is_closed());
    assert!(writer.is_locked_out());

    scenario.teardown();
}

#[tokio::test]
async fn test_storage_full() {
    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::try_get_result", "return(20)").unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");
    let mut writer = LogFileWriter::new(ctx, file, 0);

    let entry = LogEntry {
        sequence_id: 0,
        transaction_id: 0,
        transaction_n_entries: 0,
        page_id: PageId(1),
        page_file_id: PageFileId(1),
        op: LogOp::Free,
    };
    writer.write_log(entry, None).await.unwrap();
    let error = writer
        .sync()
        .await
        .expect_err("sync should error as data is flushed to disk");
    assert_eq!(error.kind(), ErrorKind::StorageFull);

    assert!(!writer.is_closed());
    assert!(writer.is_locked_out());

    scenario.teardown();
}

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_readable_results_fuzz(
    #[values(false, true)] encryption: bool,
    #[values(352352352, 934572, 1526491)] rng_seed: u64,
    #[values(1, 4, 16, 423)] num_blocks: u32,
) {
    fastrand::seed(rng_seed);

    let ctx = Arc::new(ctx::FileContext::for_test(encryption));
    let scheduler = scheduler::IoScheduler::for_test();
    let named_tmp_file = tempfile::NamedTempFile::new().unwrap();
    let (tmp_file, path) = named_tmp_file.into_parts();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");
    let mut writer = LogFileWriter::new(ctx.clone(), file, 0);

    // Write initial pages that should go through as normal.
    for page_id in 0..num_blocks {
        let entry = LogEntry {
            sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 0,
            page_id: PageId(page_id),
            page_file_id: PageFileId(1),
            op: LogOp::Free,
        };
        writer.write_log(entry, None).await.unwrap();
    }
    writer.sync().await.unwrap();

    let entry = LogEntry {
        sequence_id: 0,
        transaction_id: 0,
        transaction_n_entries: 0,
        page_id: PageId(num_blocks),
        page_file_id: PageFileId(1),
        op: LogOp::Free,
    };
    writer.write_log(entry, None).await.unwrap();
    writer.sync().await.unwrap();

    let expected_block_position = writer.position() - log::LOG_BLOCK_SIZE as u64;
    let mut buffer = std::fs::read(path).unwrap();

    let block_buffer =
        &mut buffer[expected_block_position as usize..][..log::LOG_BLOCK_SIZE];
    let block = log::decode_log_block(
        ctx.cipher(),
        &op_log_associated_data(
            1,
            PageId(
                (log::MAX_BLOCK_NO_METADATA_ENTRIES as u32
                    * (num_blocks / log::MAX_BLOCK_NO_METADATA_ENTRIES as u32))
                    .saturating_sub(1),
            ),
            expected_block_position,
        ),
        block_buffer,
    )
    .expect("block should be decodable from expected byte position");
    assert_eq!(block.last_page_id(), Some(PageId(num_blocks)));
}

#[rstest::rstest]
#[case::zero_offset(0)]
#[should_panic(expected = "log offset must be a multiple of the disk alignment")]
#[case::unaligned_offset1(13)]
#[should_panic(expected = "log offset must be a multiple of the disk alignment")]
#[case::unaligned_offset2(1024)]
#[case::aligned_offset1(4096)]
#[case::aligned_offset2(4096 * 3)]
#[tokio::test]
async fn test_log_offset(#[case] log_offset: u64) {
    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");
    let _writer = LogFileWriter::new(ctx, file, log_offset);
}

async fn fill_buffer(writer: &mut LogFileWriter) -> io::Result<()> {
    let entry = LogEntry {
        sequence_id: 0,
        transaction_id: 0,
        transaction_n_entries: 0,
        page_id: PageId(1),
        page_file_id: PageFileId(1),
        op: LogOp::Free,
    };

    for _ in 0..8_000 {
        writer.write_log(entry, None).await?;
    }

    Ok(())
}
