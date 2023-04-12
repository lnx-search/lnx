use std::mem;
use std::time::Instant;

use bytecheck::CheckBytes;
use bytes::Bytes;
use datacake::rpc::{Handler, Request, RpcService, ServiceRegistry, Status};
use hashbrown::HashSet;
use humansize::DECIMAL;
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

        let (tx, rx) = flume::bounded(10);

        lnx_executor::spawn_task(async move {
            let mut total_bytes = 0;

            let start = Instant::now();
            for (path, _) in reader.get_file_locations() {
                let file = reader
                    .read_file(path)
                    .expect("File should exist in fragment, this is a bug");

                total_bytes += file.len();
                if let Err(e) = tx.send_async(Bytes::copy_from_slice(&file)).await {
                    error!(error = ?e, remote_addr = %remote, "Failed to send fragment body to peer");
                    return;
                }
            }

            let mut block_ids = reader
                .get_fragment_blocks()
                .map(|(id, _)| *id)
                .filter(|block_id| !lookup.contains(block_id))
                .collect::<Vec<BlockId>>();
            block_ids.sort_unstable();

            let mut buffered = Vec::with_capacity(5 << 20);
            for block_id in block_ids {
                let block = reader
                    .read_block(block_id)
                    .expect("Block should exist in fragment, this is a bug");
                buffered.extend_from_slice(&block);

                if buffered.len() < (4 << 20) {
                    continue;
                }

                total_bytes += buffered.len();
                let body = mem::replace(&mut buffered, Vec::with_capacity(2 << 20));
                if let Err(e) = tx.send_async(Bytes::from(body)).await {
                    error!(error = ?e, remote_addr = %remote, "Failed to send fragment body to peer");
                    return;
                }
            }

            if !buffered.is_empty() {
                if let Err(e) = tx.send_async(Bytes::from(buffered)).await {
                    error!(error = ?e, remote_addr = %remote, "Failed to send fragment body to peer");
                    return;
                }
            }

            let transfer_rate =
                (total_bytes as f32 / start.elapsed().as_secs_f32()) as usize;
            let transfer_rate_pretty = humansize::format_size(transfer_rate, DECIMAL);
            info!(
                elapsed = ?start.elapsed(),
                transfer_rate_bytes_sec = transfer_rate,
                transfer_rate = %format!("{transfer_rate_pretty}/s"),
                "Fragment streaming completed",
            );
        });

        Ok(FragmentStream {
            files,
            blocks,
            body: rx,
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
