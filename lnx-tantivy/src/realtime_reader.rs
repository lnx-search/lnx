use std::io::Cursor;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lnx_tools::kill_switch::{KillSwitch, KillSwitchWatcher};
use tantivy::query::{EnableScoring, Query};
use tantivy::{IndexReader, Searcher, SegmentId};
use tantivy_common::BitSet;
use tracing::{error, instrument};
use crate::doc_ids::SegmentDocIds;

/// The interval in which readers are refreshed.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
/// A index reader that can apply updates in almost real time.
///
/// This refreshes readers every `REFRESH_INTERVAL` if a change has occurred.
pub struct RealtimeIndexReader {
    /// The index ID of the reader.
    index_id: Arc<String>,
    /// The kill switch for the background refresh task.
    refresh_task_ks: Arc<KillSwitch>,
    /// The signal for telling the refresh task that an update has occurred.
    update_signal: Arc<AtomicU64>,
    /// The document IDs for the segments.
    doc_ids_filters: SegmentDocIds,
    /// The actual tantivy reader.
    reader: IndexReader,
}

impl RealtimeIndexReader {
    /// Creates a new realtime reader from the given tantivy reader.
    pub async fn create(
        index_id: &str,
        reader: IndexReader,
        doc_ids_filters: SegmentDocIds,
    ) -> Self {
        let refresh_task_ks = Arc::new(KillSwitch::default());
        let ks_handle = refresh_task_ks.watcher();
        let update_signal = Arc::new(AtomicU64::default());

        tokio::spawn(refresh_task(
            index_id.to_owned(),
            ks_handle,
            update_signal.clone(),
            reader.clone(),
        ));

        Self {
            index_id: Arc::new(index_id.to_owned()),
            refresh_task_ks,
            update_signal,
            doc_ids_filters,
            reader,
        }
    }

    #[inline]
    /// The index ID the reader belongs to.
    pub fn index_id(&self) -> &str {
        self.index_id.as_ref()
    }

    #[inline]
    /// Creates a new index searcher.
    pub fn searcher(&self) -> IndexSearcher {
        IndexSearcher {
            index_id: self.index_id.clone(),
            inner: self.reader.searcher(),
            doc_ids_filters: self.doc_ids_filters.clone(),
        }
    }
}

#[derive(Clone)]
/// A wrapper around a tantivy searcher.
pub struct IndexSearcher {
    index_id: Arc<String>,
    inner: Searcher,
    doc_ids_filters: SegmentDocIds,
}

impl IndexSearcher {
    #[inline]
    /// The index ID the reader belongs to.
    pub fn index_id(&self) -> &str {
        self.index_id.as_ref()
    }

    /// Execute a delete query and returns the alive bitset.
    ///
    /// This returns a new set of bitsets which have been serialized.
    pub fn get_delete_bitsets(
        &self,
        query: &dyn Query,
    ) -> tantivy::Result<Vec<(SegmentId, Bytes)>> {
        let weight = query.weight(EnableScoring::disabled_from_searcher(&self.inner))?;

        let mut delete_bitsets = Vec::new();
        for segment_reader in self.inner.segment_readers() {
            let max_doc = segment_reader.max_doc();
            let segment_id = segment_reader.segment_id();

            let mut new_bitset = BitSet::with_max_value_and_full(max_doc);
            let mut might_have_changed = false;
            weight.for_each_no_score(
                segment_reader,
                &mut |docs_matching_delete_query| {
                    for doc_matching_delete_query in
                        docs_matching_delete_query.iter().cloned()
                    {
                        new_bitset.remove(doc_matching_delete_query);
                        might_have_changed = true;
                    }
                },
            )?;

            if !might_have_changed {
                continue;
            }

            if let Some(existing_alive_bitset) = segment_reader.alive_bitset() {
                new_bitset.intersect_update(existing_alive_bitset.bitset());
            }

            let mut writer = Cursor::new(Vec::with_capacity(12));
            new_bitset.serialize(&mut writer)?;

            let data = writer.into_inner();
            delete_bitsets.push((segment_id, Bytes::from(data)));
        }

        Ok(delete_bitsets)
    }
}

impl Deref for IndexSearcher {
    type Target = Searcher;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[instrument(name = "index-reader", skip_all)]
/// The background reader refresh task.
async fn refresh_task(
    index_id: String,
    kill_switch: KillSwitchWatcher,
    update_signal: Arc<AtomicU64>,
    reader: IndexReader,
) {
    let mut last_update_point = update_signal.load(Ordering::Relaxed);
    let mut interval = tokio::time::interval(REFRESH_INTERVAL);

    loop {
        interval.tick().await;

        if kill_switch.is_killed() {
            return;
        }

        let point = update_signal.load(Ordering::Relaxed);
        if point <= last_update_point {
            continue;
        }

        if let Err(e) = reader.reload() {
            error!(index_id = index_id, error = ?e, "Failed to refresh index reader due to error");
            continue;
        }

        last_update_point = point;
    }
}
