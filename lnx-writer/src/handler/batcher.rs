use std::future::Future;
use std::process::Output;
use std::sync::Arc;
use std::time::Instant;
use anyhow::anyhow;
use hashbrown::HashSet;
use lnx_storage::stores::IndexStore;
use lnx_storage::templates::change_log::ChangeLogEntry;
use lnx_storage::types::{SegmentId, Timestamp};


#[instrument(name = "concurrent-batcher", skip(data, store, callback))]
pub(crate) async fn batch<BATCH, CB, F>(
    max_concurrency: usize,
    store: IndexStore,
    data: Vec<BATCH>,
    callback: CB,
) -> anyhow::Result<()>
where
    BATCH: Send + Sync + 'static,
    F: Future<Output = anyhow::Result<HashSet<SegmentId>>> + Send + 'static,
    CB: Fn(&'static [BATCH], IndexStore) -> F + Sync + Send + 'static,
{
    let callback = Arc::new(callback);
    let (tx, rx) = async_channel::bounded::<&'static [BATCH]>(max_concurrency * 2);
    let mut handles = Vec::with_capacity(max_concurrency);
    for _ in 0..max_concurrency {
        let tasks = rx.clone();
        let local_store = store.clone();
        let local_cb = callback.clone();
        let handle = tokio::spawn(async move {
            let mut changed_segments = HashSet::new();
            while let Ok(chunk) = tasks.recv().await {
                let tokens = local_cb(chunk, local_store.clone()).await?;

                changed_segments.extend(tokens.into_iter());
            }

            for segment in changed_segments {
                local_store
                    .append_changes(ChangeLogEntry {
                        affected_segment: segment,
                        timestamp: Timestamp::default(),
                    })
                    .await?;
            }

            Ok::<_, anyhow::Error>(())
        });

        handles.push(handle);
    }

    let data_len = data.len();
    let chunks = data.chunks(max_concurrency);

    info!("Got {} entries(s) to process...", data_len);
    for chunk in chunks {
        // SAFETY: We know chunks is always going to outlive our tasks here.
        let chunk: &'static [BATCH] =
            unsafe { std::mem::transmute(chunk) };
        if tx.send(chunk).await.is_err() {
            break;
        };
    }

    // WARNING: This must be explicitly called in order to prevent deadlocks.
    tx.close();

    // TODO rollback changes on fail?
    let mut failed_tasks = vec![];
    for handle in handles {
        let res = handle.await;
        match res {
            Ok(Ok(())) => {},
            Ok(Err(e)) => failed_tasks.push(e),
            Err(e) => failed_tasks.push(anyhow::Error::from(e)),
        };
    }

    let num_errors = failed_tasks.len();
    if !failed_tasks.is_empty() {
        return Err(anyhow!("{} Tasks failed with the error: {}", num_errors, failed_tasks[0]))
    }

    Ok(())
}