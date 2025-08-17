use std::sync::Arc;

use crate::PageFileId;
use crate::file::page_op_log::file::LogFileWriter;
use crate::file::{DISK_ALIGN, ctx, scheduler};
use crate::layout::log;
use crate::layout::log::{LogEntry, LogOp};

#[tokio::test]
async fn test_single_entry_write_layout() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let (file, path) = tmp_file.into_parts();

    let file = scheduler
        .make_ring_file(1, file)
        .await
        .expect("Failed to make ring file");

    let mut writer = LogFileWriter::new(ctx, file, 0);

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
    assert_eq!(content.len(), DISK_ALIGN);

    let expected_buffer = &mut content[..log::LOG_BLOCK_SIZE];
    let block: &rkyv::Archived<log::LogBlock> =
        log::decode_log_block(None, b"", expected_buffer)
            .expect("block should be decodable");
    assert_eq!(block.num_entries(), 1);

    let mut iter = block.iter_pairs();
    let pair = iter.next().unwrap();
    assert_eq!(pair.log, entry);
}
