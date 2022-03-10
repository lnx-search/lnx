use std::time::{Duration, Instant};

use anyhow::Result;
use async_channel::Receiver;
use lnx_common::index::context::IndexContext;
use lnx_common::index::polling::PollingMode;
use lnx_storage::stores::IndexStore;
use lnx_storage::templates::change_log::ChangeLogEntry;
use lnx_storage::types::Timestamp;
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};

use crate::indexer::{self, WeakIndexer};

const MAX_TICKS: usize = 360; // 360 x 5 second ticks.
const MAX_CONCURRENCY: usize = 10; // todo change to use index specific max download concurrency
const CHUNK_SIZE: usize = 5_000; // todo optimise?
const POLLING_BASE_INTERVAL: u64 = 30; // todo change to use index specific polling interval

pub enum PollStatus {
    Ok,
    NoIndex,
    Err(anyhow::Error),
}

pub fn start_polling_for_index(ctx: IndexContext) -> JoinHandle<()> {
    tokio::spawn(run_poller(ctx))
}

#[instrument(name = "index-poller", skip_all)]
async fn run_poller(ctx: IndexContext) {
    let mut interval = interval(Duration::from_secs(POLLING_BASE_INTERVAL));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Use first tick to ensure the rest of our state is synced before polling.
    interval.tick().await;

    loop {
        interval.tick().await;

        match handle_poll(&ctx).await {
            PollStatus::Ok => continue,
            PollStatus::NoIndex => {
                info!("No index detected, aborting poller.");
                break;
            },
            PollStatus::Err(e) => {
                error!("Failed to handle poll. {}", e);
            },
        }
    }
}

async fn handle_poll(ctx: &IndexContext) -> PollStatus {
    let index = match lnx_controller::get(ctx.name()) {
        Some(index) => index,
        None => return PollStatus::NoIndex,
    };

    let maybe_last_ts = index.get_last_update_timestamp().await;

    let maybe_last_ts = match maybe_last_ts {
        Err(e) => return PollStatus::Err(e),
        Ok(ts) => ts,
    };

    let had_changed = match maybe_last_ts {
        Some(ref ts) => match handle_changes(ctx, &index, *ts).await {
            Ok(changed) => changed,
            Err(e) => return PollStatus::Err(e),
        },
        None => match handle_load_index(&index).await {
            Ok(()) => Some(Timestamp::default()),
            Err(e) => return PollStatus::Err(e),
        },
    };

    let ts = match had_changed {
        None => return PollStatus::Ok,
        Some(ts) => ts,
    };

    info!("Setting the node's last update timestamp...");
    let res = index.set_update_timestamp(ts).await;

    if let Err(e) = res {
        error!("Failed to set node timestamp.");
        PollStatus::Err(e)
    } else {
        PollStatus::Ok
    }
}

#[instrument(name = "index-loader", skip_all)]
async fn handle_load_index(index: &IndexStore) -> Result<()> {
    let engine = lnx_controller::engine::get();
    let output_path = index.ctx().root_storage_path(engine.base_path()); // todo: Adjust so it uses the given engine's path.

    info!("Attempting to load index from an existing peer...");
    match index.load_index_from_peer(&output_path).await {
        Ok(()) => {
            info!("Peer loading success. The node is now synced!");
            return Ok(());
        },
        Err(e) => {
            warn!("Cannot load index from existing peer, defaulting back to sequential load. Reason: {}", e);
        },
    }

    let start = Instant::now();
    let indexed_fields = index.ctx().schema().indexed_fields();

    let mut documents = index
        .iter_documents(Some(indexed_fields), CHUNK_SIZE, None)
        .await?;

    let mut indexer = indexer::new(index.clone()).await?;

    while let Some(docs) = documents.next().await {
        debug!("Handling document chunk with len={}", docs.len());
        indexer.add_documents(docs).await?;
    }
    indexer.finish();

    info!(
        "Indexing has completed! Took {:?} to complete.",
        start.elapsed()
    );

    Ok(())
}

#[instrument(name = "index-updater", skip_all)]
async fn handle_changes(
    ctx: &IndexContext,
    index: &IndexStore,
    last_update: Timestamp,
) -> Result<Option<Timestamp>> {
    debug!("Checking for changes since last update...");

    let count = index.count_pending_changes(last_update).await?;

    if count == 0 {
        debug!("No updates pending!");
        return Ok(None);
    }

    info!("Detected {} pending updates waiting.", count);

    let updated_to = match ctx.polling_mode() {
        PollingMode::Continuous { .. } => {
            handle_continuous_indexing(index, last_update).await?
        },
        PollingMode::Dynamic { .. } => {
            handle_dynamic_indexing(index, last_update).await?
        },
    };

    Ok(Some(updated_to))
}

#[inline]
#[instrument(name = "continuous-indexer", skip_all)]
async fn handle_continuous_indexing(
    index: &IndexStore,
    last_update: Timestamp,
) -> Result<Timestamp> {
    process_changes(index, last_update).await
}

#[instrument(name = "dynamic-indexer", skip_all)]
async fn handle_dynamic_indexing(
    index: &IndexStore,
    last_update: Timestamp,
) -> Result<Timestamp> {
    info!("Checking if changes are still in progress...");

    // We're checking to see if changes are still being added to the database.
    // If they are we wait 5 seconds and check again, this can help cut down
    // the indexing time in places where you may only do periodic updates and
    // want to have them indexed in one big bulk go.
    //
    // TODO: In future maybe we have an exponential backoff on the polling time
    //  to prevent too much load on the DB when it isn't really needed.
    let mut num_ticks = 0;
    let mut last_count = 0;
    let mut interval = interval(Duration::from_secs(30));
    while num_ticks <= MAX_TICKS {
        interval.tick().await;

        let count = index.count_pending_changes(last_update).await?;

        if count == last_count {
            break;
        }

        last_count = count;
        num_ticks += 1;
    }

    process_changes(index, last_update).await
}

async fn process_changes(
    index: &IndexStore,
    last_update: Timestamp,
) -> Result<Timestamp> {
    info!(
        "Beginning the indexing process. Updating to timestamp {}",
        last_update
    );

    let mut indexer = indexer::new(index.clone()).await?;
    indexer.clear_documents().await?;

    let indexed_fields = index.ctx().schema().indexed_fields();
    let mut changes = index.get_pending_changes(last_update).await?;

    let (tx, rx) = async_channel::bounded::<ChangeLogEntry>(MAX_CONCURRENCY);
    let mut handles = vec![];
    for _ in 0..MAX_CONCURRENCY {
        let handle = tokio::spawn(handle_segment_change(
            indexer.get_weak(),
            rx.clone(),
            index.clone(),
            indexed_fields.clone(),
        ));

        handles.push(handle);
    }

    while let Some(segment_change) = changes.next().await {
        if tx.send(segment_change).await.is_err() {
            break;
        }
    }
    tx.close();

    for handle in handles {
        handle.await??;
    }

    Ok(Timestamp::default())
}

async fn handle_segment_change(
    indexer: WeakIndexer,
    tasks: Receiver<ChangeLogEntry>,
    store: IndexStore,
    fields: Vec<String>,
) -> anyhow::Result<()> {
    while let Ok(segment_change) = tasks.recv().await {
        indexer
            .remove_segment(segment_change.affected_segment)
            .await?;

        let mut iter = store
            .iter_documents(
                Some(fields.clone()),
                5000,
                Some(segment_change.affected_segment),
            )
            .await?;

        while let Some(docs) = iter.next().await {
            indexer.add_documents(docs).await?;
        }
    }

    Ok(())
}
