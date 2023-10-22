use async_trait::async_trait;
use time::OffsetDateTime;
use crate::{DfsMetadata, IndexesMetastore, IndexInfo, NodeInformation, NodesMetadata};

pub struct ScyllaMetastore {

}

#[async_trait]
impl IndexesMetastore for ScyllaMetastore {
    async fn create_index(&self, info: IndexInfo) -> anyhow::Result<()> {
        todo!()
    }

    async fn remove_index(&self, index_id: String) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_indexes(&self) -> anyhow::Result<Vec<IndexInfo>> {
        todo!()
    }
}

#[async_trait]
impl DfsMetadata for ScyllaMetastore {
    async fn add_file(&self, file_name: String, seed_nodes: Vec<String>) -> anyhow::Result<()> {
        todo!()
    }

    async fn del_file(&self, file_name: String) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_files(&self, since: Option<OffsetDateTime>) -> anyhow::Result<()> {
        todo!()
    }
}

#[async_trait]
impl NodesMetadata for ScyllaMetastore {
    async fn join_cluster(&self, info: NodeInformation) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_node_readiness(&self, node_id: String) -> anyhow::Result<()> {
        todo!()
    }

    async fn leave_cluster(&self, node_id: String) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_nodes(&self) -> anyhow::Result<NodeInformation> {
        todo!()
    }
}