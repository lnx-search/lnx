use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use datacake::crdt::Key;
use datacake::eventual_consistency::Document;
use datacake::node::{ConnectionConfig, DCAwareSelector, DatacakeNodeBuilder};
use parking_lot::Mutex;

use crate::listeners::{FragmentListener, StorageListener};
use crate::{DatacakeNode, EnvCtx, LnxStorageExtension, LnxStorageHandle, StorageGuard};

mod block_replication;
mod fragment_read;
mod fragment_replication;
mod kv_ops;

/// A setup harness for a single node cluster
async fn single_node_test_harness<CB, F>(cb: CB) -> anyhow::Result<()>
where
    F: Future<Output = ()>,
    CB: FnOnce(LnxStorageHandle, OpsLogger) -> F,
{
    let env = EnvCtx::for_test();
    crate::resolvers::init_folders(&env.root_path)?;
    lnx_executor::build_default_pools(1)?;
    let _ = tracing_subscriber::fmt::try_init();

    let addr = test_helper::get_unused_addr();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
        .connect()
        .await?;

    let (_guard, store) = node.add_extension(LnxStorageExtension::new(env)).await?;

    let ops_logger = OpsLogger::default();
    store
        .listeners()
        .register_fragment_listener(ops_logger.clone());
    store
        .listeners()
        .register_storage_listener(OpsLogger::KEYSPACE, ops_logger.clone());

    (cb)(store, ops_logger).await;

    node.shutdown().await;

    Ok(())
}

/// A setup harness for a multi node cluster
async fn multi_node_test_harness<'a, CB, F>(num_nodes: u8, cb: CB) -> anyhow::Result<()>
where
    F: Future<Output = ()>,
    CB: FnOnce(Vec<LnxStorageHandle>, OpsLogger) -> F,
{
    lnx_executor::build_default_pools(1)?;
    let _ = tracing_subscriber::fmt::try_init();

    let (nodes, _guards) = connect_nodes(num_nodes).await?;

    let ops_logger = OpsLogger::default();
    for node in nodes.iter() {
        node.listeners()
            .register_fragment_listener(ops_logger.clone());
        node.listeners()
            .register_storage_listener(OpsLogger::KEYSPACE, ops_logger.clone());
    }

    (cb)(nodes, ops_logger).await;

    Ok(())
}

async fn connect_nodes(
    n: u8,
) -> anyhow::Result<(Vec<LnxStorageHandle>, Vec<(StorageGuard, DatacakeNode)>)> {
    let mut nodes = Vec::new();
    let mut guards = Vec::new();
    let mut previous_seeds = Vec::new();
    let mut previous_node_ids = Vec::new();
    for id in 0..n {
        let env = EnvCtx::for_test(); // Prevents nodes on the same process overlapping.
        crate::resolvers::init_folders(&env.root_path)?;

        let addr = test_helper::get_unused_addr();

        let connection_cfg = ConnectionConfig::new(addr, addr, &previous_seeds);
        let node = DatacakeNodeBuilder::<DCAwareSelector>::new(id, connection_cfg)
            .connect()
            .await
            .expect("Connect node.");

        node.wait_for_nodes(&previous_node_ids, Duration::from_secs(30))
            .await?;

        previous_node_ids.push(id);
        if previous_seeds.len() >= 2 {
            previous_seeds.pop();
        }
        previous_seeds.push(addr.to_string());

        let (guard, store) = node
            .add_extension(LnxStorageExtension::new(env))
            .await
            .expect("Create store.");

        guards.push((guard, node));
        nodes.push(store);
    }

    Ok((nodes, guards))
}

type BlockEvent = (u64, Vec<(u64, Vec<u8>)>);

#[derive(Default, Clone)]
/// A listener that tracks all the events that go
/// through it and stores them.
///
/// This is mainly for testing fragments but
/// can be used for general KV operations if the
/// `log-me` keyspace is used.
struct OpsLogger {
    blocks: Arc<Mutex<Vec<BlockEvent>>>,
    files: Arc<Mutex<Vec<(u64, String)>>>,
    sealed: Arc<Mutex<Vec<u64>>>,
    deletes: Arc<Mutex<Vec<u64>>>,
    kv_puts: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
    kv_deletes: Arc<Mutex<Vec<u64>>>,
}

impl OpsLogger {
    const KEYSPACE: &'static str = "log-me";

    fn blocks(&self) -> Vec<BlockEvent> {
        self.blocks.lock().clone()
    }

    fn files(&self) -> Vec<(u64, String)> {
        self.files.lock().clone()
    }

    fn sealed(&self) -> Vec<u64> {
        self.sealed.lock().clone()
    }

    fn deletes(&self) -> Vec<u64> {
        self.deletes.lock().clone()
    }

    fn kv_puts(&self) -> Vec<(u64, Vec<u8>)> {
        self.kv_puts.lock().clone()
    }

    fn kv_deletes(&self) -> Vec<u64> {
        self.kv_deletes.lock().clone()
    }
}

impl FragmentListener for OpsLogger {
    fn on_block_flush(&self, fragment_id: u64, blocks: &[Document]) {
        let blocks = blocks
            .iter()
            .map(|doc| (doc.id(), doc.data().to_vec()))
            .collect();
        self.blocks.lock().push((fragment_id, blocks))
    }

    fn on_file_add(&self, fragment_id: u64, file_path: &str) {
        self.files.lock().push((fragment_id, file_path.to_owned()));
    }

    fn on_seal(&self, fragment_id: u64) {
        self.sealed.lock().push(fragment_id);
    }

    fn on_delete(&self, fragment_id: u64) {
        self.deletes.lock().push(fragment_id)
    }
}

impl StorageListener for OpsLogger {
    fn on_put(&self, doc: Document) {
        self.kv_puts.lock().push((doc.id(), doc.data().to_vec()))
    }

    fn on_del(&self, doc_id: Key) {
        self.kv_deletes.lock().push(doc_id)
    }
}
