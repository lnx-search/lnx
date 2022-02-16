// TODO: Heartbeater.

use std::time::Duration;

use anyhow::Result;
use lnx_storage::{ChangeKind, DocId, IndexStore, PollingMode, Timestamp};
use tantivy::chrono::Utc;
use tokio::task::JoinHandle;
use tokio::time::interval;

use super::handler::{self};
use crate::handler::Indexer;

const MAX_TICKS: usize = 360; // 360 x 5 second ticks.
const CHUNK_SIZE: usize = 10_000; // todo optimise?
const POLLING_BASE_INTERVAL: u64 = 30;

pub enum PollStatus {
    Ok,
    NoIndex,
    Err(anyhow::Error),
}

pub fn start_polling_for_index(index_name: String, mode: PollingMode) -> JoinHandle<()> {
    tokio::spawn(run_poller(index_name, mode))
}

#[instrument(name = "index-poller")]
async fn run_poller(index_name: String, mode: PollingMode) {
    let mut interval = interval(Duration::from_secs(POLLING_BASE_INTERVAL));

    // Use first tick to ensure the rest of our state is synced before polling.
    interval.tick().await;

    loop {
        interval.tick().await;

        match handle_poll(&index_name, mode).await {
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

async fn handle_poll(index_name: &str, mode: PollingMode) -> PollStatus {
    let indexes = lnx_storage::engine().indexes();
    let index = match indexes.get(index_name) {
        Some(index) => index,
        None => return PollStatus::NoIndex,
    };

    let maybe_last_ts = index.meta().get_last_update_timestamp().await;

    let maybe_last_ts = match maybe_last_ts {
        Err(e) => return PollStatus::Err(e),
        Ok(ts) => ts,
    };

    match maybe_last_ts {
        Some(ref ts) => match handle_changes(index, mode, *ts).await {
            Ok(()) => {},
            Err(e) => return PollStatus::Err(e),
        },
        None => match handle_load_index(index).await {
            Ok(()) => {},
            Err(e) => return PollStatus::Err(e),
        },
    }

    info!("Setting the node's last update timestamp...");
    let res = index
        .meta()
        .set_update_timestamp(
            maybe_last_ts.unwrap_or_else(|| Utc::now().timestamp_millis()),
        )
        .await;

    if let Err(e) = res {
        error!("Failed to set node timestamp.");
        return PollStatus::Err(e);
    }

    // See if we can cleanup the change log.
    info!("Attempting to run log garbage collections...");
    let aligned = index.meta().get_earliest_aligned_timestamp().await;

    match aligned {
        Ok(Some(ts)) => match index.docs().run_garbage_collection(ts).await {
            Ok(()) => PollStatus::Ok,
            Err(e) => PollStatus::Err(e),
        },
        Ok(None) => PollStatus::Ok,
        Err(e) => PollStatus::Err(e),
    }
}

#[instrument(name = "index-loader", skip_all)]
async fn handle_load_index(index: &IndexStore) -> Result<()> {
    let output_path = index.file_path();

    info!("Attempting to load index from an existing peer...");
    match index.meta().load_index_from_peer(output_path).await {
        Ok(()) => {
            info!("Peer loading success. The node is now synced!");
            return Ok(());
        },
        Err(e) => {
            warn!("Cannot load index from existing peer, defaulting back to sequential load. Reason: {}", e);
        },
    }

    let indexed_fields = index.schema().indexed_fields();

    let mut documents = index
        .docs()
        .iter_documents(Some(indexed_fields), CHUNK_SIZE)
        .await?;

    let mut indexer = handler::get().begin_indexing(index.name()).await?;

    while let Some(docs) = documents.next().await {
        debug!("Handling document chunk with len={}", docs.len());
        indexer.add_documents(docs).await?;
    }
    info!("Indexing has completed!");

    Ok(())
}

#[inline]
#[instrument(name = "index-updater", skip(index))]
async fn handle_changes(
    index: &IndexStore,
    mode: PollingMode,
    last_update: Timestamp,
) -> Result<()> {
    info!("Checking for changes since last update...");

    let count = index.docs().count_pending_changes(last_update).await?;

    if count == 0 {
        debug!("No updates pending!");
        return Ok(());
    }

    info!("{} Pending updates waiting.", count);

    match mode {
        PollingMode::Continuous => {
            handle_continuous_indexing(index, last_update).await?
        },
        PollingMode::Dynamic => handle_dynamic_indexing(index, last_update).await?,
    };

    Ok(())
}

#[inline]
#[instrument(name = "continuous-indexer", skip_all)]
async fn handle_continuous_indexing(
    index: &IndexStore,
    last_update: Timestamp,
) -> Result<()> {
    process_changes(index, last_update).await
}

#[instrument(name = "dynamic-indexer", skip_all)]
async fn handle_dynamic_indexing(
    index: &IndexStore,
    last_update: Timestamp,
) -> Result<()> {
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
    let mut interval = interval(Duration::from_secs(5));
    while num_ticks <= MAX_TICKS {
        interval.tick().await;

        let count = index.docs().count_pending_changes(last_update).await?;

        if count == last_count {
            break;
        }

        last_count = count;
        num_ticks += 1;
    }

    process_changes(index, last_update).await?;

    Ok(())
}

async fn process_changes(index: &IndexStore, last_update: Timestamp) -> Result<()> {
    let mut indexer = handler::get().begin_indexing(index.name()).await?;

    let mut changes = index
        .docs()
        .get_pending_changes(last_update, CHUNK_SIZE)
        .await?;

    let indexed_fields = index.schema().indexed_fields();

    while let Some(changes) = changes.next().await {
        for change in changes {
            match change.kind {
                ChangeKind::ClearAll => indexer.clear_documents().await?,
                ChangeKind::Append => {
                    add_documents(
                        index,
                        indexed_fields.clone(),
                        &mut indexer,
                        change.affected_docs,
                    )
                    .await?
                },
                ChangeKind::Delete => {
                    remove_documents(&mut indexer, change.affected_docs).await?
                },
                ChangeKind::Update => {
                    update_documents(
                        index,
                        indexed_fields.clone(),
                        &mut indexer,
                        change.affected_docs,
                    )
                    .await?
                },
            }
        }
    }

    Ok(())
}

async fn add_documents(
    index: &IndexStore,
    indexed_fields: Vec<String>,
    indexer: &mut Indexer,
    doc_ids: Vec<DocId>,
) -> Result<()> {
    let documents = index
        .docs()
        .fetch_documents(Some(indexed_fields), doc_ids)
        .await?;

    indexer.add_documents(documents).await
}

async fn remove_documents(indexer: &mut Indexer, doc_ids: Vec<DocId>) -> Result<()> {
    indexer.remove_documents(doc_ids).await?;
    Ok(())
}

async fn update_documents(
    index: &IndexStore,
    indexed_fields: Vec<String>,
    indexer: &mut Indexer,
    doc_ids: Vec<DocId>,
) -> Result<()> {
    remove_documents(indexer, doc_ids.clone()).await?;
    add_documents(index, indexed_fields, indexer, doc_ids).await?;

    Ok(())
}
