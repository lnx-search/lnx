use std::io;
use std::io::{Seek, Write};
use std::sync::Arc;

use crate::PageFileId;
use crate::file::ctx::associated_data;
use crate::file::page_op_log::reader::LogFileReader;
use crate::file::{ctx, scheduler};
use crate::layout::log;
use crate::layout::log::{LogEntry, LogOp};

const FILE_ID: u64 = 1;

#[rstest::rstest]
#[trace]
#[tokio::test]
async fn test_log_reader(
    #[values(true, false)] encryption: bool,
    #[values(0, 4096)] offset: u64,
    #[values(0, 5, 18)] num_blocks: usize,
) {
    let ctx = Arc::new(ctx::FileContext::for_test(encryption));
    let scheduler = scheduler::IoScheduler::for_test();
    let mut tmp_file = tempfile::tempfile().unwrap();

    make_sample_file(&ctx, &mut tmp_file, num_blocks, offset).unwrap();

    let file = scheduler
        .make_ring_file(FILE_ID, tmp_file)
        .await
        .expect("Failed to make ring file");

    let mut reader = LogFileReader::new(ctx, file, offset);

    let mut blocks = Vec::new();
    while let Some(block) = reader.next_block().await.expect("Failed to read block") {
        blocks.push(block);
    }
    assert_eq!(blocks.len(), num_blocks);

    for (block_id, block) in blocks.into_iter().enumerate() {
        assert_eq!(block.num_entries(), 7);
        let expected_first_sequence_id = block_id * 7;

        let entry = &block.entries()[0];
        assert_eq!(entry.log.sequence_id, expected_first_sequence_id as u32);
    }
}

fn make_sample_file(
    ctx: &ctx::FileContext,
    file: &mut std::fs::File,
    num_blocks: usize,
    offset: u64,
) -> io::Result<()> {
    dbg!(num_blocks);
    file.set_len(offset)?;
    file.seek(io::SeekFrom::Start(offset))?;

    let mut seq_id = 0;
    for block_id in 0..num_blocks {
        let mut block = log::LogBlock::default();
        for _ in 0..7 {
            let entry = LogEntry {
                sequence_id: seq_id,
                last_flush_sequence_id: 0,
                transaction_id: 0,
                transaction_n_entries: 0,
                page_file_id: PageFileId(1),
                op: LogOp::Write,
            };
            block.push_entry(entry, None).unwrap();

            seq_id += 1;
        }

        let mut buffer = [0; log::LOG_BLOCK_SIZE];
        log::encode_log_block(
            ctx.cipher(),
            &associated_data(FILE_ID, offset + (block_id * log::LOG_BLOCK_SIZE) as u64),
            &block,
            &mut buffer,
        )
        .unwrap();

        file.write_all(&buffer)?;
        file.sync_all()?;
    }

    Ok(())
}
