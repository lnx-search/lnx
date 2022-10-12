use std::path::{Path, PathBuf};
use std::time::Duration;

use datacake_crdt::HLCTimestamp;
use lnx_segments::{Exporter, DELETES_FILE};
use once_cell::sync::OnceCell;
use tantivy::IndexWriter;
use tokio::sync::broadcast;

use crate::indexer::Indexer;
use crate::WriterError;

static FINALIZER: OnceCell<Finalizer> = OnceCell::new();

#[derive(Debug, thiserror::Error)]
#[error("The finalizer actor has died unexpectedly.")]
pub struct DeadActor;

/// Initialises the finalizer background task.
///
/// This task handles all commits and exporting of new segments.
pub(crate) async fn init(base_path: &Path) {
    if FINALIZER.get().is_some() {
        return;
    }

    let (changes_tx, changes_rx) = broadcast::channel(500);
    let (tx, rx) = flume::bounded(2);
    let actor = FinalizerActor {
        base_export_path: base_path.to_path_buf(),
        segments: rx,
        changes_notifier: changes_tx,
    };

    tokio::spawn(actor.run_actor());

    FINALIZER.get_or_init(|| Finalizer {
        segments: tx,
        changes_notifier: changes_rx,
    });
}

/// Submits an indexer to the finalizer queue.
///
/// If the queue is currently full this will block until their is
/// an available slot.
pub(crate) fn submit_indexer(indexer: Indexer) -> Result<(), DeadActor> {
    let finalizer = FINALIZER.get().ok_or(DeadActor)?;

    finalizer.segments.send(indexer).map_err(|_| DeadActor)?;

    Ok(())
}

/// Subscribes to the changes broadcast channel.
///
/// This will broadcast any updates to the given indexer states, i.e. Success or Fail.
pub fn subscribe(
) -> Result<broadcast::Receiver<(HLCTimestamp, SegmentStatus)>, DeadActor> {
    let finalizer = FINALIZER.get().ok_or(DeadActor)?;

    let rx = finalizer.changes_notifier.resubscribe();

    Ok(rx)
}

pub(crate) struct Finalizer {
    segments: flume::Sender<Indexer>,
    changes_notifier: broadcast::Receiver<(HLCTimestamp, SegmentStatus)>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SegmentStatus {
    Success,
    Failure(String),
}

pub(crate) struct FinalizerActor {
    base_export_path: PathBuf,
    segments: flume::Receiver<Indexer>,
    changes_notifier: broadcast::Sender<(HLCTimestamp, SegmentStatus)>,
}

impl FinalizerActor {
    async fn run_actor(self) {
        while let Ok(indexer) = self.segments.recv_async().await {
            let segment_id = indexer.segment_id;

            info!(
                segment_id = %segment_id,
                num_docs = indexer.num_documents,
                "Beginning finalization of segment."
            );

            let path = self
                .base_export_path
                .join(segment_id.to_string())
                .with_extension("segment");

            let start = std::time::Instant::now();
            let result = finalize_segment(&path, indexer).await;
            let elapsed = start.elapsed();

            self.acknowledge_segment(segment_id, elapsed, result);
        }

        info!("Segment finalizer shutting down!");
    }

    #[instrument(name = "segment-finalizer", skip(self, result))]
    fn acknowledge_segment(
        &self,
        segment_id: HLCTimestamp,
        elapsed: Duration,
        result: Result<(), WriterError>,
    ) {
        let status = if let Err(e) = result {
            error!(
                error = ?e,
                "Failed to finalize segment due to error.",
            );

            SegmentStatus::Failure(e.to_string())
        } else {
            info!("Successfully finalised segment.");

            SegmentStatus::Success
        };

        let _ = self.changes_notifier.send((segment_id, status));
    }
}

#[instrument(
    name = "segment-finalizer",
    skip(indexer),
    fields(segment_id = %indexer.segment_id)
)]
async fn finalize_segment(
    export_path: &Path,
    indexer: Indexer,
) -> Result<(), WriterError> {
    let size_hint = indexer.estimated_disk_usage();
    let files_location = indexer.dir.exported_files().to_path_buf();

    let writer = indexer.writer;

    let files = tokio::task::spawn_blocking(move || {
        process_blocking_ops(writer, files_location)
    })
    .await
    .expect("Spawn background thread")?;

    let mut exporter = Exporter::create(
        export_path,
        size_hint,
        indexer.index_name,
        indexer.segment_id,
    )
    .await?;

    let deletes_raw = indexer.deletes.to_compressed_bytes().await?;
    exporter
        .write_raw(Path::new(DELETES_FILE), deletes_raw)
        .await?;

    let total = files.len();
    for (i, fp) in files.into_iter().enumerate() {
        exporter.write_file(&fp).await?;
        info!(target_file = ?fp, "Exported file [{}/{}]", i + 1, total);
    }

    exporter.finalise().await?;

    Ok(())
}

fn process_blocking_ops(
    mut writer: IndexWriter,
    export_path: PathBuf,
) -> Result<Vec<PathBuf>, WriterError> {
    writer.commit()?;
    writer.wait_merging_threads()?;

    let mut files = vec![];

    for entry in std::fs::read_dir(export_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            continue;
        }

        files.push(path);
    }

    Ok(files)
}
