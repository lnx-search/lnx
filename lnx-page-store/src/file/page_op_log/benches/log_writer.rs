extern crate test;

use std::sync::Arc;

use crate::file::page_op_log::writer::LogFileWriter;
use crate::file::{ctx, scheduler};
use crate::layout::log::{LogEntry, LogOp};
use crate::{PageFileId, PageId};

const NUM_ITER: usize = 100;

#[bench]
fn single_entry_flush_encryption_false(
    bencher: &mut test::Bencher,
) -> anyhow::Result<()> {
    run_log_writer_single_entry_flush::<1>(bencher, false)
}

#[bench]
fn single_entry_flush_encryption_true(
    bencher: &mut test::Bencher,
) -> anyhow::Result<()> {
    run_log_writer_single_entry_flush::<1>(bencher, true)
}

#[bench]
fn four_entry_flush_encryption_false(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    run_log_writer_single_entry_flush::<4>(bencher, false)
}

#[bench]
fn four_entry_flush_encryption_true(bencher: &mut test::Bencher) -> anyhow::Result<()> {
    run_log_writer_single_entry_flush::<4>(bencher, true)
}

fn run_log_writer_single_entry_flush<const N_ITERS: usize>(
    bencher: &mut test::Bencher,
    encryption: bool,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let ctx = Arc::new(ctx::FileContext::for_test(encryption));
    let scheduler = scheduler::IoScheduler::for_test();
    let tmp_file = tempfile::tempfile()?;
    let file = rt.block_on(scheduler.make_ring_file(1, tmp_file))?;
    let mut writer = LogFileWriter::new(ctx, file, 0);

    let entry = LogEntry {
        sequence_id: 0,
        transaction_id: 0,
        transaction_n_entries: 0,
        page_file_id: PageFileId(1),
        page_id: PageId(1),
        op: LogOp::Free,
    };

    bencher.iter(|| {
        rt.block_on(async {
            for _ in 0..N_ITERS {
                writer.write_log(entry, None).await.unwrap();
            }
            writer.sync().await.unwrap();
        })
    });

    rt.block_on(async move {
        drop(writer);
    });

    Ok(())
}
