use std::time::Duration;

use crate::{FragmentInfo, LnxStorageHandle, SharedSlice};

#[tokio::test]
async fn test_local_file_write() -> anyhow::Result<()> {
    super::single_node_test_harness(|store, ops_logger| async move {
        store
            .add_file(
                1,
                "my-path.txt",
                SharedSlice::copy_from_slice(b"hello, world"),
            )
            .await
            .expect("Add file");

        // Since notifications are executed asynchronously, we need to wait temporarily.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            ops_logger.files(),
            [(1, "my-path.txt".to_string())],
            "Added files should match."
        );
    })
    .await
}

#[tokio::test]
async fn test_local_commit_no_data() -> anyhow::Result<()> {
    super::single_node_test_harness(|store, ops_logger| async move {
        store
            .commit_fragment(
                1,
                FragmentInfo {
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

        // Since notifications are executed asynchronously, we need to wait temporarily.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(ops_logger.files(), [], "Added files should match.");
        assert_eq!(ops_logger.sealed(), [1], "Sealed fragments should match.");
    })
    .await
}

#[tokio::test]
async fn test_local_commit_with_block_data() -> anyhow::Result<()> {
    super::single_node_test_harness(|store, ops_logger| async move {
        let block_data = b"Hello, world".to_vec();
        store
            .add_block(1, 1, block_data.clone(), 1)
            .await
            .expect("Add block locally");

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

        // Since notifications are executed asynchronously, we need to wait temporarily.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(ops_logger.files(), [], "Added files should match.");
        assert_eq!(
            ops_logger.blocks(),
            [(1, vec![(1, block_data)])],
            "Added blocks should match."
        );
        assert_eq!(ops_logger.sealed(), [1], "Sealed fragments should match.");
    })
    .await
}

#[tokio::test]
async fn test_local_commit_with_file_data() -> anyhow::Result<()> {
    super::single_node_test_harness(|store, ops_logger| async move {
        store
            .add_file(
                1,
                "my-path.txt",
                SharedSlice::copy_from_slice(b"hello, world"),
            )
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

        // Since notifications are executed asynchronously, we need to wait temporarily.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            ops_logger.files(),
            [(1, "my-path.txt".to_string())],
            "Added files should match."
        );
        assert_eq!(ops_logger.blocks(), [], "Added blocks should match.");
        assert_eq!(ops_logger.sealed(), [1], "Sealed fragments should match.");
    })
    .await
}

#[tokio::test]
async fn test_local_commit_with_data() -> anyhow::Result<()> {
    super::single_node_test_harness(|store, ops_logger| async move {
        let block_data = b"Hello, world".to_vec();
        store
            .add_block(1, 1, block_data.clone(), 1)
            .await
            .expect("Add block locally");

        store
            .add_file(
                1,
                "my-path.txt",
                SharedSlice::copy_from_slice(b"hello, world"),
            )
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

        // Since notifications are executed asynchronously, we need to wait temporarily.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            ops_logger.files(),
            [(1, "my-path.txt".to_string())],
            "Added files should match."
        );
        assert_eq!(
            ops_logger.blocks(),
            [(1, vec![(1, block_data)])],
            "Added blocks should match."
        );
        assert_eq!(ops_logger.sealed(), [1], "Sealed fragments should match.");
    })
    .await
}

#[tokio::test]
async fn test_local_fragment_delete() -> anyhow::Result<()> {
    super::single_node_test_harness(|store: LnxStorageHandle, ops_logger| async move {
        store
            .add_file(
                1,
                "my-path.txt",
                SharedSlice::copy_from_slice(b"hello, world"),
            )
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

        // Since notifications are executed asynchronously, we need to wait temporarily.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            ops_logger.files(),
            [(1, "my-path.txt".to_string())],
            "Added files should match."
        );
        assert_eq!(ops_logger.blocks(), [], "Added blocks should match.");
        assert_eq!(ops_logger.sealed(), [1], "Sealed fragments should match.");

        store.delete_fragment(1).await.expect("Delete fragment");

        // Since notifications are executed asynchronously, we need to wait temporarily.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(ops_logger.deletes(), [1], "Deleted fragments should match");
    })
    .await
}

#[tokio::test]
async fn test_cluster_commit() -> anyhow::Result<()> {
    super::multi_node_test_harness(
        3,
        |nodes: Vec<LnxStorageHandle>, _ops_logger| async move {
            let first_node = &nodes[0];

            let blocks = [
                (1, b"Hello 1".to_vec(), 1),
                (2, b"Hello 1".to_vec(), 1),
                (3, b"Hello 1".to_vec(), 1),
            ];
            first_node
                .add_many_blocks(1, blocks.clone())
                .await
                .expect("Add block locally");

            info!("Add blocks!");
            first_node
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

            info!("Commited!");

            // Since notifications are executed asynchronously, we need to wait temporarily.
            tokio::time::sleep(Duration::from_millis(50)).await;
        },
    )
    .await
}
