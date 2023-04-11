use std::time::Duration;

use crate::{FragmentInfo, LnxStorageHandle, SharedSlice};

#[tokio::test]
async fn test_fragment_read_block() -> anyhow::Result<()> {
    super::single_node_test_harness(|store: LnxStorageHandle, _ops_logger| async move {
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
            .expect("Add file");

        // Since notifications are executed asynchronously, we need to wait temporarily.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let reader = store.get_reader(1).expect("Reader should exist");

        let fetched = reader
            .read_block(1)
            .expect("Block should exist within reader");
        assert_eq!(
            fetched.as_ref(),
            block_data,
            "Block data returned should match"
        );

        let fetched = reader
            .read_file("my-path.txt")
            .expect("Block should exist within reader");
        assert_eq!(
            fetched.as_ref(),
            b"hello, world",
            "File data returned should match"
        );
    })
    .await
}
