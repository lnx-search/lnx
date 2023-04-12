//! Runs the storage system under high load with many small blocks of data
//! being added to fragments.

use std::time::{Duration, Instant};

use humansize::DECIMAL;
use itertools::Itertools;
use lnx_storage::LnxStorageHandle;
use tracing::info;

const SMALL_BLOCK_SIZE: usize = 5 << 10; // 5KB
const BIG_BLOCK_SIZE: usize = 512 << 10; // 512KB
const NUM_BLOCKS: usize = 5_000;
const FRAGMENT_ID: u64 = 1;

pub async fn run_small_blocks_bench() -> anyhow::Result<()> {
    let data = vec![0u8; SMALL_BLOCK_SIZE];

    run_local_no_batching(data.clone()).await?;
    run_local_batching(data.clone()).await?;

    Ok(())
}

pub async fn run_big_blocks_bench() -> anyhow::Result<()> {
    let data = vec![0u8; BIG_BLOCK_SIZE];

    run_local_no_batching(data.clone()).await?;
    run_local_batching(data.clone()).await?;

    Ok(())
}

macro_rules! timeit {
    ($dur:expr, $code:block) => {{
        let start = Instant::now();
        let res = $code;
        (*$dur) += start.elapsed();
        res
    }};
}

async fn run_local_no_batching(data: Vec<u8>) -> anyhow::Result<()> {
    info!(
        block_size = humansize::format_size(data.len(), DECIMAL),
        "Running local test with no batching (Worst case)"
    );

    super::cluster_harness::single_node_test_harness(
        |store: LnxStorageHandle| async move {
            let mut storage_time = Duration::default();
            let mut hashing_time = Duration::default();
            for block_id in 0..NUM_BLOCKS {
                let checksum = timeit!(&mut hashing_time, { crc32fast::hash(&data) });
                timeit!(&mut storage_time, {
                    store
                        .add_block(FRAGMENT_ID, block_id as u64, data.clone(), checksum)
                        .await
                        .expect("Add block");
                });
            }
            let total_elapsed = storage_time + hashing_time;
            let blocks_per_sec = NUM_BLOCKS as f32 / total_elapsed.as_secs_f32();
            let transfer_rate = (blocks_per_sec * data.len() as f32) as usize;

            info!(
                total_elapsed = ?total_elapsed,
                hashing_time = ?hashing_time,
                storage_time = ?storage_time,
                blocks_per_sec = blocks_per_sec,
                transfer_rate = %humansize::format_size(transfer_rate, DECIMAL),
                "Test complete",
            );
        },
    )
    .await
}

async fn run_local_batching(data: Vec<u8>) -> anyhow::Result<()> {
    info!(
        block_size = humansize::format_size(data.len(), DECIMAL),
        "Running local test with batching",
    );

    for batch_size in [5, 10, 50, 100, 250, 1000] {
        super::cluster_harness::single_node_test_harness(|store: LnxStorageHandle| {
            let data = data.clone();
            async move {
                let mut storage_time = Duration::default();
                let mut hashing_time = Duration::default();

                let blocks_chunks = (0..NUM_BLOCKS)
                    .into_iter()
                    .map(|block_id| {
                        let checksum =
                            timeit!(&mut hashing_time, { crc32fast::hash(&data) });
                        (block_id as u64, data.clone(), checksum)
                    })
                    .chunks(batch_size);

                for blocks in &blocks_chunks {
                    timeit!(&mut storage_time, {
                        store
                            .add_many_blocks(FRAGMENT_ID, blocks)
                            .await
                            .expect("Add block");
                    });
                }
                let total_elapsed = storage_time + hashing_time;
                let blocks_per_sec = NUM_BLOCKS as f32 / total_elapsed.as_secs_f32();
                let transfer_rate = (blocks_per_sec * data.len() as f32) as usize;

                info!(
                    batch_size = batch_size,
                    total_elapsed = ?total_elapsed,
                    hashing_time = ?hashing_time,
                    storage_time = ?storage_time,
                    blocks_per_sec = blocks_per_sec,
                    transfer_rate_per_sec = %humansize::format_size(transfer_rate, DECIMAL),
                    "Test complete",
                );
            }
        })
        .await?;
    }

    Ok(())
}
