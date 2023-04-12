//! Runs the storage system under high load with many small fragments being produced.

use std::time::{Duration, Instant};

use humansize::DECIMAL;
use itertools::Itertools;
use lnx_storage::{FragmentInfo, LnxStorageHandle};
use tracing::info;

const BLOCK_SIZE: usize = 128 << 10; // 128KB¬
const TINY_FRAGMENT_SIZE: usize = 100 << 10; // 100 KB
const SMALL_FRAGMENT_SIZE: usize = 100 << 20; // 100 MB
const MEDIUM_FRAGMENT_SIZE: usize = 1 << 30; // 1 GB
const LARGE_FRAGMENT_SIZE: usize = 3 << 30; // 3 GB

pub async fn run_fragments_bench() -> anyhow::Result<()> {
    // run_local(TINY_FRAGMENT_SIZE).await?;
    // run_local(SMALL_FRAGMENT_SIZE).await?;
    // run_local(MEDIUM_FRAGMENT_SIZE).await?;
    // // run_local(LARGE_FRAGMENT_SIZE).await?;
    //
    // run_cluster(3, TINY_FRAGMENT_SIZE).await?;
    // run_cluster(3, SMALL_FRAGMENT_SIZE).await?;
    run_cluster(3, MEDIUM_FRAGMENT_SIZE).await?;

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

async fn run_local(target_size: usize) -> anyhow::Result<()> {
    info!(
        target_size = humansize::format_size(target_size, DECIMAL),
        "Running local test"
    );

    super::cluster_harness::single_node_test_harness(
        |store: LnxStorageHandle| async move {
            let data = vec![0u8; BLOCK_SIZE];
            let num_blocks = target_size / BLOCK_SIZE;

            let mut total_elapsed = Duration::default();
            for fragment_id in 0..10 {
                timeit!(&mut total_elapsed, {
                    create_fragment_and_commit(&store, target_size, fragment_id).await;
                });
            }

            let blocks_per_sec =
                (num_blocks as f32 * 10.0) / total_elapsed.as_secs_f32();
            let transfer_rate = (blocks_per_sec * data.len() as f32) as usize;

            info!(
                total_elapsed = ?total_elapsed,
                blocks_per_sec = blocks_per_sec,
                transfer_rate = %humansize::format_size(transfer_rate, DECIMAL),
                "Test complete",
            );
        },
    )
    .await
}

async fn run_cluster(num_nodes: u8, target_size: usize) -> anyhow::Result<()> {
    info!(
        target_size = humansize::format_size(target_size, DECIMAL),
        "Running cluster test"
    );

    super::cluster_harness::multi_node_test_harness(
        num_nodes,
        |stores: Vec<LnxStorageHandle>| async move {
            let store = &stores[0];
            let data = vec![0u8; BLOCK_SIZE];
            let num_blocks = target_size / BLOCK_SIZE;
            info!("Using {num_blocks} blocks");

            let mut total_elapsed = Duration::default();
            timeit!(&mut total_elapsed, {
                create_fragment_and_commit(store, target_size, 1).await;
            });

            let blocks_per_sec =
                (num_blocks as f32 * 10.0) / total_elapsed.as_secs_f32();
            let transfer_rate = (blocks_per_sec.max(1.0) * data.len() as f32) as usize;

            info!(
                total_elapsed = ?total_elapsed,
                blocks_per_sec = blocks_per_sec,
                transfer_rate = %humansize::format_size(transfer_rate, DECIMAL),
                "Test complete",
            );
        },
    )
    .await
}

async fn create_fragment_and_commit(
    store: &LnxStorageHandle,
    target_size: usize,
    fragment_id: u64,
) {
    let data = vec![0u8; BLOCK_SIZE];
    let num_blocks = target_size / BLOCK_SIZE;

    let blocks_chunks = (0..num_blocks)
        .into_iter()
        .map(|block_id| {
            let checksum = crc32fast::hash(&data);
            (block_id as u64, data.clone(), checksum)
        })
        .chunks(1000);

    for blocks in &blocks_chunks {
        store
            .add_many_blocks(fragment_id, blocks)
            .await
            .expect("Add block");
    }

    let start = Instant::now();
    store
        .commit_fragment(
            fragment_id,
            FragmentInfo {
                fragment_id,
                num_blocks: num_blocks as u32,
                num_docs: 0,
                orphaned_id: None,
                num_bytes_total: (num_blocks * data.len()) as u64,
                child_of_fragments: vec![],
            },
        )
        .await
        .expect("COMMIT LIED TO US");

    info!("Commit took {:?}", start.elapsed());
}
