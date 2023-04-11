use datacake::node::Consistency;

use crate::tests::OpsLogger;
use crate::LnxStorageHandle;

// TODO: A lot of this can be replace with datacake's test suite
//       once it's move into another feature.

#[tokio::test]
async fn test_basic_kv_ops() -> anyhow::Result<()> {
    super::single_node_test_harness(|store: LnxStorageHandle, ops_logger| async move {
        store
            .put(
                OpsLogger::KEYSPACE,
                1,
                b"Hello, world".to_vec(),
                Consistency::None,
            )
            .await
            .expect("Put basic kv");
        assert_eq!(
            ops_logger.kv_puts(),
            [(1, b"Hello, world".to_vec())],
            "KV op data should match"
        );

        let value = store
            .get(OpsLogger::KEYSPACE, 1)
            .await
            .expect("Put basic kv")
            .expect("Document should exist");
        assert_eq!(value.id(), 1, "Doc IDs should match");
        assert_eq!(value.data(), b"Hello, world", "Doc data should match");

        store
            .del(OpsLogger::KEYSPACE, 1, Consistency::None)
            .await
            .expect("Put basic kv");
        assert_eq!(ops_logger.kv_deletes(), [1], "KV op data should match");

        let value = store
            .get(OpsLogger::KEYSPACE, 1)
            .await
            .expect("Put basic kv");
        assert!(value.is_none(), "Document should not exist");
    })
    .await
}

#[tokio::test]
async fn test_many_basic_kv_ops() -> anyhow::Result<()> {
    super::single_node_test_harness(|store: LnxStorageHandle, ops_logger| async move {
        store
            .put_many(
                OpsLogger::KEYSPACE,
                [
                    (1, b"Hello, world".to_vec()),
                    (2, b"Hello, world 2".to_vec()),
                ],
                Consistency::None,
            )
            .await
            .expect("Put basic kv");
        assert_eq!(
            ops_logger.kv_puts(),
            [
                (1, b"Hello, world".to_vec()),
                (2, b"Hello, world 2".to_vec()),
            ],
            "KV ops data should match",
        );

        let values = store
            .get_many(OpsLogger::KEYSPACE, [1, 2])
            .await
            .expect("Put basic kv")
            .map(|doc| (doc.id(), doc.data().to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            [
                (1, b"Hello, world".to_vec()),
                (2, b"Hello, world 2".to_vec()),
            ],
            "Returned docs should match"
        );

        store
            .del_many(OpsLogger::KEYSPACE, [1, 2], Consistency::None)
            .await
            .expect("Put basic kv");
        assert_eq!(ops_logger.kv_deletes(), [1, 2], "KV ops data should match");

        let num_values = store
            .get_many(OpsLogger::KEYSPACE, [1, 2])
            .await
            .expect("Put basic kv")
            .count();
        assert_eq!(num_values, 0, "No docs should exist");
    })
    .await
}
