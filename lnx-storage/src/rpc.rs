use bytecheck::CheckBytes;
use bytes::Bytes;
use datacake::rpc::{Handler, Request, RpcService, ServiceRegistry, Status};
use hashbrown::HashSet;
use rkyv::{Archive, Deserialize, Serialize};

use crate::fragments::{
    BlockId,
    FragmentStream,
    IndexFragmentsReaders,
    IndexFragmentsWriters,
    WriteDocBlock,
};

pub struct StorageService {
    writers: IndexFragmentsWriters,
    readers: IndexFragmentsReaders,
}

impl StorageService {
    pub fn new(writers: IndexFragmentsWriters, readers: IndexFragmentsReaders) -> Self {
        Self { writers, readers }
    }
}

impl RpcService for StorageService {
    fn service_name() -> &'static str {
        "lnx-storage-service"
    }

    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<GetFragment>();
        registry.add_handler::<AddDocBlock>();
        registry.add_handler::<AddManyDocBlocks>();
    }
}

#[datacake::rpc::async_trait]
impl Handler<GetFragment> for StorageService {
    type Reply = FragmentStream;

    async fn on_message(
        &self,
        msg: Request<GetFragment>,
    ) -> Result<Self::Reply, Status> {
        let remote = msg.remote_addr();
        let msg = msg.into_inner().to_owned().map_err(Status::internal)?;

        let reader = self.readers.get_reader(msg.fragment_id).ok_or_else(|| {
            Status::internal(format!("Unknown fragment {}", msg.fragment_id))
        })?;

        let lookup: HashSet<u64> = HashSet::from_iter(msg.blocks);

        let files = reader
            .get_file_locations()
            .map(|(key, range)| (key.clone(), (range.end - range.start) as u32))
            .collect();

        let blocks = reader
            .get_fragment_blocks()
            .filter(|(block_id, _)| !lookup.contains(&**block_id))
            .map(|(block_id, info)| (*block_id, info.len(), info.checksum))
            .collect();

        info!(remote_addr = %remote, fragment_id = msg.fragment_id, "Starting data stream for node");

        let (mut tx, body) = hyper::Body::channel();

        tokio::spawn(async move {
            for (path, _) in reader.get_file_locations() {
                let file = reader
                    .read_file(path)
                    .expect("File should exist in fragment, this is a bug");

                if let Err(e) = tx.send_data(Bytes::copy_from_slice(&file)).await {
                    error!(error = ?e, remote_addr = %remote, "Failed to send fragment body to peer");
                }
            }

            for (block_id, _) in reader.get_fragment_blocks() {
                let block = reader
                    .read_block(*block_id)
                    .expect("Block should exist in fragment, this is a bug");

                if let Err(e) = tx.send_data(Bytes::copy_from_slice(&block)).await {
                    error!(error = ?e, remote_addr = %remote, "Failed to send fragment body to peer");
                }
            }
        });

        Ok(FragmentStream {
            files,
            blocks,
            body: body.into(),
        })
    }
}

#[datacake::rpc::async_trait]
impl Handler<AddDocBlock> for StorageService {
    type Reply = ();

    async fn on_message(
        &self,
        msg: Request<AddDocBlock>,
    ) -> Result<Self::Reply, Status> {
        let msg = msg.into_inner().to_owned().map_err(Status::internal)?;

        self.writers
            .write_block(msg.fragment_id, msg.block)
            .await
            .map_err(Status::internal)?;

        Ok(())
    }
}

#[datacake::rpc::async_trait]
impl Handler<AddManyDocBlocks> for StorageService {
    type Reply = ();

    async fn on_message(
        &self,
        msg: Request<AddManyDocBlocks>,
    ) -> Result<Self::Reply, Status> {
        let msg = msg.into_inner().to_owned().map_err(Status::internal)?;

        self.writers
            .write_many_blocks(msg.fragment_id, &msg.blocks)
            .await
            .map_err(Status::internal)?;

        Ok(())
    }
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(CheckBytes, Debug))]
/// Retrieve a fragment.
pub struct GetFragment {
    /// The unique ID of the fragment.
    pub fragment_id: u64,
    /// A blocks which are already available to the client.
    pub blocks: Vec<BlockId>,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Clone)]
#[archive_attr(derive(CheckBytes))]
/// Add a block chunk to the fragment.
pub struct AddDocBlock {
    /// The unique ID of the fragment.
    pub fragment_id: u64,
    /// The document block.
    pub block: WriteDocBlock,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Clone)]
#[archive_attr(derive(CheckBytes))]
pub struct AddManyDocBlocks {
    /// The unique ID of the fragment.
    pub fragment_id: u64,
    /// The document blocks.
    pub blocks: Vec<WriteDocBlock>,
}
