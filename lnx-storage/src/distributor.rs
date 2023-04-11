use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datacake::node::{Consistency, ConsistencyError, DatacakeHandle};
use datacake::rpc::{
    Channel,
    Handler,
    RequestContents,
    RpcClient,
    RpcService,
    TryIntoBody,
};
use flume::TryRecvError;
use futures::future::join_all;
use tokio::sync::Semaphore;
use tokio::time::{interval, MissedTickBehavior};

use crate::fragments::WriteDocBlock;
use crate::rpc::{AddDocBlock, AddManyDocBlocks};
use crate::StorageService;

const NETWORK_TIMEOUT: Duration = Duration::from_secs(2);
/// The maximum memory usage of a batch.
///
/// The value of 250MB is fairly specific and is effectively
/// calculated as the theoretical time to transfer the batch
/// in 1 second on a 2.5gbps network with a 50MB buffer value
/// so we dont go beyond this value easily (targeting 300MB)
const MAX_MEMORY_USAGE_ON_BATCH: usize = 250 << 20;
const MAX_RPC_CONCURRENCY: usize = 10;
/// The maximum number of messages that can be queued up in the
/// batcher before backpressure begins to apply.
const CHANNEL_CAPACITY: usize = 500;
/// The duration between batching requests.
pub const HEARTBEAT: Duration = if cfg!(test) {
    Duration::from_millis(250)
} else {
    Duration::from_secs(1)
};

type FragmentBatches = BTreeMap<(u64, SocketAddr), Vec<WriteDocBlock>>;

#[derive(Clone)]
/// A network handler that distributes the task
/// of replicating doc blocks across nodes.
///
/// This replicates blocks using the [Consistency::Quorum]
/// consistency level, meaning it will wait for a quorum of nodes
/// to response and acknowledge that the block is safely stored before
/// acknowledging the client request.
///
/// Once the quorum has been fulfilled the rest of the tasks are batched up
/// and sent across in a batch.
pub struct TaskDistributor {
    node: DatacakeHandle,
    tx: flume::Sender<Mutation>,
}

impl TaskDistributor {
    /// Create a new task distributor.
    ///
    /// This spawns a background task and supervisor to ensure
    /// the background system is running as long as the distributor
    /// is not dropped.
    pub async fn create(node: DatacakeHandle) -> Self {
        let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);

        tokio::spawn(supervise_distributor_task(node.clone(), rx));

        Self { node, tx }
    }

    /// Send a block across the cluster.
    ///
    /// This will attempt to reach a minimum consistency level
    /// of [Consistency::Quorum] before adding the block
    /// to a batch which will be sent as a background task.
    pub async fn send_block(
        &self,
        fragment_id: u64,
        block: WriteDocBlock,
    ) -> Result<(), ConsistencyError> {
        let priority_nodes = self.node.select_nodes(Consistency::Quorum).await?;
        let memory_usage = block.block.data().len();

        let msg = AddDocBlock { fragment_id, block };

        self.submit_to_nodes::<StorageService, _>(&priority_nodes, msg.clone())
            .await?;

        let all_nodes = self.node.select_nodes(Consistency::All).await?;
        self.tx
            .send_async(Mutation::AddBlock {
                memory_usage,
                send_to: all_nodes
                    .into_iter()
                    .filter(|addr| !priority_nodes.contains(addr))
                    .collect(),
                msg,
            })
            .await
            .expect("Channel should not be disconnected");

        Ok(())
    }

    /// Send multiple blocks across the cluster.
    ///
    /// This will attempt to reach a minimum consistency level
    /// of [Consistency::Quorum] before adding the block
    /// to a batch which will be sent as a background task.
    pub async fn send_many_blocks(
        &self,
        fragment_id: u64,
        blocks: impl Iterator<Item = WriteDocBlock>,
    ) -> Result<(), ConsistencyError> {
        let priority_nodes = self.node.select_nodes(Consistency::Quorum).await?;
        let mut memory_usage = 0;

        let msg = AddManyDocBlocks {
            fragment_id,
            blocks: blocks
                .into_iter()
                .map(|block| {
                    memory_usage += block.block.data().len();
                    block
                })
                .collect(),
        };

        self.submit_to_nodes::<StorageService, _>(&priority_nodes, msg.clone())
            .await?;

        let all_nodes = self.node.select_nodes(Consistency::All).await?;
        self.tx
            .send_async(Mutation::AddManyBlocks {
                memory_usage,
                send_to: all_nodes
                    .into_iter()
                    .filter(|addr| !priority_nodes.contains(addr))
                    .collect(),
                msg,
            })
            .await
            .expect("Channel should not be disconnected");

        Ok(())
    }

    async fn submit_to_nodes<Svc, Msg>(
        &self,
        priority_nodes: &[SocketAddr],
        msg: Msg,
    ) -> Result<(), ConsistencyError>
    where
        Svc: RpcService + Handler<Msg> + Sync + Send + 'static,
        Msg: Clone + RequestContents + TryIntoBody + Send + 'static,
        <Svc as Handler<Msg>>::Reply: RequestContents + TryIntoBody + Send + 'static,
    {
        let num_required_nodes = priority_nodes.len();
        let mut tasks = Vec::with_capacity(priority_nodes.len());
        for addr in priority_nodes {
            let channel = self.node.network().get_or_connect(*addr);
            let mut client = RpcClient::<Svc>::new(channel);
            client.set_timeout(NETWORK_TIMEOUT);

            let msg = msg.clone();
            let task = tokio::spawn(async move { client.send_owned(msg).await });

            tasks.push(task);
        }

        let results = join_all(tasks).await.into_iter().enumerate();

        let mut num_successful = 0;
        for (i, result) in results {
            let result = result.expect("Join task");

            if let Err(e) = result {
                let remote_addr = priority_nodes[i];
                error!(error = ?e, remote_addr = %remote_addr, "Node failed to handle RPC request");
            } else {
                num_successful += 1;
            }
        }

        if num_successful != num_required_nodes {
            return Err(ConsistencyError::ConsistencyFailure {
                responses: num_successful + 1,
                required: num_required_nodes + 1,
                timeout: NETWORK_TIMEOUT,
            });
        }

        Ok(())
    }
}

enum Mutation {
    AddBlock {
        memory_usage: usize,
        send_to: Vec<SocketAddr>,
        msg: AddDocBlock,
    },
    AddManyBlocks {
        memory_usage: usize,
        send_to: Vec<SocketAddr>,
        msg: AddManyDocBlocks,
    },
}

/// Run the supervisor for the task distribution service.
///
/// Effectively this system watches for the `run_task_distributor`
/// to finish/exit and restart it if it exists unexpectedly (this should never happen).
async fn supervise_distributor_task(
    node: DatacakeHandle,
    mutations: flume::Receiver<Mutation>,
) {
    loop {
        let node_clone = node.clone();
        let events_clone = mutations.clone();

        let task_res =
            tokio::spawn(run_task_distributor(node_clone, events_clone)).await;

        if let Err(e) = task_res {
            error!(error = ?e, "Task distributor failed due to unknown error or panic, this is a bug");
            tokio::time::sleep(Duration::from_secs(2)).await;
        } else if mutations.is_disconnected() {
            info!("Distributor task supervisor shutting down");
            return;
        }
    }
}

async fn run_task_distributor(node: DatacakeHandle, events: flume::Receiver<Mutation>) {
    let limiter = Arc::new(Semaphore::new(MAX_RPC_CONCURRENCY));
    let mut interval = interval(HEARTBEAT);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    'runner: loop {
        interval.tick().await;

        // Mapping the fragment ID and the node to send the mutations.
        // This is largely based on Datacake's batching system, although
        // maybe a little bit more efficient in it's current state.
        let mut fragment_batches = FragmentBatches::new();
        let mut total_memory_usage = 0;
        'batch_loop: loop {
            if total_memory_usage >= MAX_MEMORY_USAGE_ON_BATCH {
                debug!(
                    memory_usage = total_memory_usage,
                    num_batches = fragment_batches.len(),
                    "Batch has reached max memory size, sending to nodes",
                );
                break 'batch_loop;
            }

            let maybe_mutation = events.try_recv();

            let mutation = match maybe_mutation {
                Ok(mutation) => mutation,
                Err(TryRecvError::Disconnected) => break 'runner,
                Err(TryRecvError::Empty) => break 'batch_loop,
            };

            process_mutation(&mut fragment_batches, &mut total_memory_usage, mutation);
        }

        let num_batches = fragment_batches.len();

        if num_batches == 0 {
            continue;
        }

        debug!(
            num_batches = num_batches,
            "Beginning RPC batch distribution"
        );

        let mut pending_tasks = Vec::with_capacity(num_batches);
        for ((fragment_id, node_addr), blocks) in fragment_batches {
            let channel = node.network().get_or_connect(node_addr);
            let limiter = limiter.clone();

            let fut = send_node_batch(limiter, channel, fragment_id, blocks);

            let task = tokio::spawn(fut);
            pending_tasks.push((node_addr, task));
        }

        let start = Instant::now();
        let mut num_errors = 0;
        let mut num_success = 0;
        for (node_addr, task) in pending_tasks {
            let result = task.await.expect("Join task");

            if let Err(e) = result {
                error!(error = ?e, node_addr = %node_addr, "Failed to send batch to node");
                num_errors += 1;
            } else {
                num_success += 1;
            }
        }
        debug!(
            elasped = ?start.elapsed(),
            num_batches = num_batches,
            num_errors = num_errors,
            num_success = num_success,
            "Completed batch RPCs",
        );
    }
}

#[instrument(skip(limiter, channel, blocks))]
async fn send_node_batch(
    limiter: Arc<Semaphore>,
    channel: Channel,
    fragment_id: u64,
    blocks: Vec<WriteDocBlock>,
) -> anyhow::Result<()> {
    let _permit = limiter.acquire().await?;

    let mut client = RpcClient::<StorageService>::new(channel);
    client.set_timeout(NETWORK_TIMEOUT);

    let msg = AddManyDocBlocks {
        fragment_id,
        blocks,
    };

    client.send_owned(msg).await?;

    Ok(())
}

fn process_mutation(
    fragment_batches: &mut FragmentBatches,
    total_memory_usage: &mut usize,
    mutation: Mutation,
) {
    match mutation {
        Mutation::AddBlock {
            memory_usage,
            send_to,
            msg,
        } => {
            (*total_memory_usage) += memory_usage;

            let fragment_id = msg.fragment_id;
            for addr in send_to {
                fragment_batches
                    .entry((fragment_id, addr))
                    .or_default()
                    .push(msg.block.clone());
            }
        },
        Mutation::AddManyBlocks {
            memory_usage,
            send_to,
            msg,
        } => {
            (*total_memory_usage) += memory_usage;

            let fragment_id = msg.fragment_id;
            for addr in send_to {
                fragment_batches
                    .entry((fragment_id, addr))
                    .or_default()
                    .extend_from_slice(&msg.blocks);
            }
        },
    }
}
