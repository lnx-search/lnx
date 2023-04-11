use crate::HEARTBEAT;

#[tokio::test]
async fn test_basic_setup() -> anyhow::Result<()> {
    super::single_node_test_harness(|_, _| async move {}).await
}

#[tokio::test]
async fn test_local_block_store() -> anyhow::Result<()> {
    super::single_node_test_harness(|store, ops_logger| async move {
        let block_data = b"Hello, world!";
        store
            .add_block(1, 1, block_data.to_vec(), 1)
            .await
            .expect("Add block locally");

        let events = ops_logger.blocks();
        assert_eq!(
            events,
            vec![(1, vec![(1, block_data.to_vec())]),],
            "Block events should match",
        );
    })
    .await
}

#[tokio::test]
async fn test_local_many_block_store() -> anyhow::Result<()> {
    super::single_node_test_harness(|store, ops_logger| async move {
        let blocks = [
            (1, b"Hello 1".to_vec(), 1),
            (2, b"Hello 1".to_vec(), 1),
            (3, b"Hello 1".to_vec(), 1),
        ];
        store
            .add_many_blocks(1, blocks.clone())
            .await
            .expect("Add block locally");

        let expected_blocks: Vec<(u64, Vec<u8>)> = blocks
            .into_iter()
            .map(|(block_id, data, _)| (block_id, data))
            .collect();

        let events = ops_logger.blocks();
        assert_eq!(
            events,
            vec![(1, expected_blocks),],
            "Block events should match",
        );
    })
    .await
}

#[tokio::test]
async fn test_cluster_block_store() -> anyhow::Result<()> {
    super::multi_node_test_harness(3, |nodes, ops_logger| async move {
        let first_node = &nodes[0];
        let block_data = b"Hello, world!";

        first_node
            .add_block(1, 1, block_data.to_vec(), 1)
            .await
            .expect("Add block locally");

        let events = ops_logger.blocks();
        assert_eq!(
            events,
            vec![
                // Two events should be immediately available due to the consistency requirements.
                (1, vec![(1, block_data.to_vec())]),
                (1, vec![(1, block_data.to_vec())]),
            ],
            "Block events should match",
        );

        // Let the batcher execute requests.
        // This can be super flaky at times as it really just depends on how long
        // the first op took.
        tokio::time::sleep(HEARTBEAT * 2).await;

        let events = ops_logger.blocks();
        assert_eq!(
            events,
            vec![
                // All 3 events should now be available.
                (1, vec![(1, block_data.to_vec())]),
                (1, vec![(1, block_data.to_vec())]),
                (1, vec![(1, block_data.to_vec())]),
            ],
            "Block events should match",
        );
    })
    .await
}

#[tokio::test]
async fn test_cluster_many_block_store() -> anyhow::Result<()> {
    super::multi_node_test_harness(3, |nodes, ops_logger| async move {
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

        let expected_blocks: Vec<(u64, Vec<u8>)> = blocks
            .into_iter()
            .map(|(block_id, data, _)| (block_id, data))
            .collect();

        let events = ops_logger.blocks();
        assert_eq!(
            events,
            vec![
                // Two events should be immediately available due to the consistency requirements.
                (1, expected_blocks.clone()),
                (1, expected_blocks.clone()),
            ],
            "Block events should match",
        );

        // Let the batcher execute requests.
        // This can be super flaky at times as it really just depends on how long
        // the first op took.
        tokio::time::sleep(HEARTBEAT * 2).await;

        let events = ops_logger.blocks();
        assert_eq!(
            events,
            vec![
                // All 3 events should now be available.
                (1, expected_blocks.clone()),
                (1, expected_blocks.clone()),
                (1, expected_blocks.clone()),
            ],
            "Block events should match",
        );
    })
    .await
}
