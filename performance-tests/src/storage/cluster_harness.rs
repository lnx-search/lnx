use std::env::temp_dir;
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use datacake::node::{
    ConnectionConfig,
    DCAwareSelector,
    DatacakeNode,
    DatacakeNodeBuilder,
};
use tracing::info;
use lnx_storage::{EnvCtx, LnxStorageExtension, LnxStorageHandle, StorageGuard};
use uuid::Uuid;

/// A setup harness for a single node cluster
pub(super) async fn single_node_test_harness<CB, F>(cb: CB) -> anyhow::Result<()>
where
    F: Future<Output = ()>,
    CB: FnOnce(LnxStorageHandle) -> F,
{
    let root = temp_dir()
        .join("lnx-tests")
        .join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root)?;

    let path = root.join(Uuid::new_v4().to_string());
    let env = EnvCtx::new(path);
    lnx_storage::resolvers::init_folders(&env.root_path)?;

    let addr = test_helper::get_unused_addr();
    let connection_cfg = ConnectionConfig::new(addr, addr, Vec::<String>::new());

    let node = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
        .connect()
        .await?;

    let (_guard, store) = node.add_extension(LnxStorageExtension::new(env)).await?;

    (cb)(store).await;

    node.shutdown().await;

    std::fs::remove_dir_all(&root)?;

    Ok(())
}

/// A setup harness for a multi node cluster
pub(super) async fn multi_node_test_harness<'a, CB, F>(
    num_nodes: u8,
    cb: CB,
) -> anyhow::Result<()>
where
    F: Future<Output = ()>,
    CB: FnOnce(Vec<LnxStorageHandle>) -> F,
{
    let root = temp_dir()
        .join("lnx-tests")
        .join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root)?;

    let (nodes, _guards) = connect_nodes(&root, num_nodes).await?;
    (cb)(nodes).await;

    std::fs::remove_dir_all(&root)?;

    Ok(())
}

async fn connect_nodes(
    root: &Path,
    n: u8,
) -> anyhow::Result<(Vec<LnxStorageHandle>, Vec<(StorageGuard, DatacakeNode)>)> {
    let mut nodes = Vec::new();
    let mut guards = Vec::new();
    let mut previous_seeds = Vec::new();
    let mut previous_node_ids = Vec::new();
    for id in 0..n {
        let path = root.join(Uuid::new_v4().to_string());
        let env = EnvCtx::new(path); // Prevents nodes on the same process overlapping.
        lnx_storage::resolvers::init_folders(&env.root_path)?;

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
