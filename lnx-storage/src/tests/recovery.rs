use std::time::Duration;

use datacake::node::{ConnectionConfig, DCAwareSelector, DatacakeNodeBuilder};

use crate::fragments::BlockInfo;
use crate::{
    EnvCtx,
    FragmentInfo,
    LnxStorageExtension,
    LnxStorageHandle,
    SharedSlice,
    StorageGuard,
};

#[tokio::test]
async fn test_partially_written_fragment_recovery() -> anyhow::Result<()> {
    let env = EnvCtx::for_test();
    crate::resolvers::init_folders(&env.root_path)?;
    lnx_executor::build_default_pools(1)?;
    let _ = tracing_subscriber::fmt::try_init();

    let (guard, store) = create_node_from_env(env.clone()).await?;

    store
        .add_file(
            1,
            "my-path.txt",
            SharedSlice::copy_from_slice(b"hello, world"),
        )
        .await
        .expect("Add file");

    let block_data = b"hello, world".to_vec();
    store
        .add_block(1, 1, block_data.clone(), 1)
        .await
        .expect("Add file");

    // Drop the node simulating a shutdown.
    drop(store);
    drop(guard);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Re-create the node
    let (_guard, store) = create_node_from_env(env.clone()).await?;

    let state = store
        .writers
        .get_current_writer_state(1)
        .await
        .expect("Writer should exist after reload");

    // It's an important note that we do not recover files written to the fragment.
    assert!(
        state.files.is_empty(),
        "We should not have a file anymore after shutdown"
    );

    assert_eq!(
        state.existing_blocks,
        vec![
            // This is a little specialised for a test, but we're tracking that the position
            // we expect the block to be written at matches.
            (
                1,
                BlockInfo {
                    location: 28..40,
                    checksum: 1,
                }
            )
        ],
        "Loaded blocks should match",
    );

    Ok(())
}

#[tokio::test]
async fn test_comitted_fragment_recovery() -> anyhow::Result<()> {
    let env = EnvCtx::for_test();
    crate::resolvers::init_folders(&env.root_path)?;
    lnx_executor::build_default_pools(1)?;
    let _ = tracing_subscriber::fmt::try_init();

    let (guard, store) = create_node_from_env(env.clone()).await?;

    store
        .add_file(
            1,
            "my-path.txt",
            SharedSlice::copy_from_slice(b"hello, world"),
        )
        .await
        .expect("Add file");

    let block_data = b"hello, world";
    store
        .add_block(1, 1, block_data.to_vec(), 1)
        .await
        .expect("Add file");

    store
        .commit_fragment(
            1,
            FragmentInfo {
                // Not validated
                fragment_id: 1,
                orphaned_id: None,
                num_blocks: 0,
                num_bytes_total: 0,
                num_docs: 0,
                child_of_fragments: vec![],
            },
        )
        .await
        .expect("Commit fragment");

    // Drop the node simulating a shutdown.
    drop(store);
    drop(guard);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Re-create the node
    let (_guard, store) = create_node_from_env(env.clone()).await?;

    let state = store.writers.get_current_writer_state(1).await;
    assert!(
        state.is_none(),
        "Fragment should not be treated as a writer"
    );

    let fragment = store
        .readers
        .get_reader(1)
        .expect("Fragment should exist as a reader");

    // It's an important note that we do not recover files written to the fragment.
    assert!(
        fragment.read_file("my-path.txt").is_some(),
        "The file should be recovered"
    );

    assert_eq!(
        fragment.read_block(1).as_deref(),
        Some(block_data.as_ref()),
        "Loaded blocks should match",
    );

    Ok(())
}

async fn create_node_from_env(
    env: EnvCtx,
) -> anyhow::Result<(StorageGuard, LnxStorageHandle)> {
    let addr = test_helper::get_unused_addr();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
        .connect()
        .await?;

    node.add_extension(LnxStorageExtension::new(env))
        .await
        .map_err(anyhow::Error::from)
}
