use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datacake_crdt::HLCTimestamp;
use lnx_common::schema::WriterSettings;
use lnx_common::{AppContext, IndexContext};
use lnx_segments::Delete;
use tantivy::Document;
use tokio::sync::oneshot;

use crate::indexer::{IndexerFactory, IndexerPipeline};
use crate::{WriterError, WriterStatistics};

type Responder = oneshot::Sender<Result<HLCTimestamp, WriterError>>;

/// A index writer.
///
/// The writer will produce segments as and when additions or deletes come through.
///
/// After a period of inactivity the writer will shutdown the background thread
/// which is performing the actual writes, this is done in an effort to save
/// system resources once indexing is completed.
pub struct Writer {
    app_ctx: AppContext,
    index_ctx: IndexContext,
    settings: WriterSettings,
    duration: Arc<AtomicU64>,
    ops_tx: flume::Sender<Op>,
    stats: WriterStatistics,
}

impl Writer {
    /// Creates a new index writer.
    pub async fn create(
        app_ctx: AppContext,
        index_ctx: IndexContext,
        settings: WriterSettings,
    ) -> Result<Self, WriterError> {
        let duration = Arc::new(AtomicU64::new(settings.auto_commit_duration));
        let (ops_tx, ops_rx) = flume::bounded(5);

        let factory = IndexerFactory::new(
            index_ctx.name.clone(),
            index_ctx.tantivy_schema(),
            app_ctx.tmp_path.clone(),
            settings,
        );

        let stats = WriterStatistics::default();
        let pipeline = {
            let stats = stats.clone();
            tokio::task::spawn_blocking(move || IndexerPipeline::create(factory, stats))
                .await
                .expect("Spawn background thread")?
        };

        tokio::spawn(auto_commit_timer(duration.clone(), ops_tx.clone()));
        std::thread::Builder::new()
            .name(format!("indexing-actor-{}", index_ctx.name))
            .spawn(move || run_writer_actor(pipeline, ops_rx))
            .expect("Spawn background thread.");

        Ok(Self {
            app_ctx,
            index_ctx,
            settings,
            duration,
            ops_tx,
            stats,
        })
    }

    #[inline]
    /// The statistics associated with this writer.
    pub fn stats(&self) -> WriterStatistics {
        self.stats.clone()
    }

    fn get_new_factory(&self) -> IndexerFactory {
        let schema = self.index_ctx.tantivy_schema();

        IndexerFactory::new(
            self.index_ctx.name.clone(),
            schema,
            self.app_ctx.tmp_path.clone(),
            self.settings,
        )
    }

    /// Attempts to create a new writer as the current writer task has shutdown.
    async fn recreate(&mut self) -> Result<(), WriterError> {
        let (ops_tx, ops_rx) = flume::bounded(5);

        let factory = self.get_new_factory();
        let pipeline = IndexerPipeline::create(factory, self.stats.clone())?;

        tokio::spawn(auto_commit_timer(self.duration.clone(), ops_tx.clone()));
        std::thread::Builder::new()
            .name(format!("indexing-actor-{}", self.index_ctx.name))
            .spawn(move || run_writer_actor(pipeline, ops_rx))
            .expect("Spawn background thread.");

        self.ops_tx = ops_tx;

        Ok(())
    }

    async fn ensure_alive_and_send(&mut self, op: Op) -> Result<(), WriterError> {
        let res = self.ops_tx.send_async(op).await;

        if let Err(e) = res {
            self.recreate().await?;
            self.ops_tx
                .send_async(e.0)
                .await
                .expect("Writer immediately died after recreation. This is a bug.");
        }

        Ok(())
    }

    /// Update the writer's current settings.
    ///
    /// Settings may not be immediately applied, instead it will be applied when the
    /// next segment is written and when the next auto commit period has elapsed.
    pub async fn update_settings(
        &mut self,
        settings: WriterSettings,
    ) -> Result<(), WriterError> {
        self.settings = settings;
        self.duration
            .store(settings.auto_commit_duration, Ordering::Relaxed);

        let factory = self.get_new_factory();
        self.ensure_alive_and_send(Op::SetFactory { factory })
            .await?;

        Ok(())
    }

    /// Add documents to the index.
    ///
    /// A commit id/timestamp is returned allowing the user to determine what segment
    /// a set of documents located at. This is also useful for failure recovery
    pub async fn add_documents(
        &mut self,
        docs: Vec<Document>,
    ) -> Result<HLCTimestamp, WriterError> {
        let (tx, rx) = oneshot::channel();

        self.ensure_alive_and_send(Op::AddDocuments { docs, tx })
            .await?;

        rx.await.map_err(|_| {
            WriterError::Other(
                "Writer actor died unexpectedly while handling operation".to_string(),
            )
        })?
    }

    /// Delete a set of documents from the index.
    ///
    /// These are not immediately applied and are instead rolled out across
    /// the live segments after a flush.
    pub async fn delete_documents(
        &mut self,
        deletes: Vec<Delete>,
    ) -> Result<HLCTimestamp, WriterError> {
        let (tx, rx) = oneshot::channel();

        self.ensure_alive_and_send(Op::DeleteDocuments { deletes, tx })
            .await?;

        rx.await.map_err(|_| {
            WriterError::Other(
                "Writer actor died unexpectedly while handling operation".to_string(),
            )
        })?
    }
}

async fn auto_commit_timer(duration: Arc<AtomicU64>, tx: flume::Sender<Op>) {
    loop {
        let seconds = duration.load(Ordering::Relaxed);
        let max_duration = Duration::from_secs(seconds);

        tokio::time::sleep(max_duration).await;

        if tx
            .send_async(Op::CheckAutoCommit { max_duration })
            .await
            .is_err()
        {
            break;
        }
    }
}

#[instrument(name = "index-writer", skip_all, fields(index = %pipeline.index_name()))]
fn run_writer_actor(mut pipeline: IndexerPipeline, ops_rx: flume::Receiver<Op>) {
    let mut has_commit = false;
    let mut last_commit = Instant::now();
    while let Ok(op) = ops_rx.recv() {
        match op {
            Op::AddDocuments { docs, tx } => {
                has_commit = false;

                let res = pipeline.add_documents(docs);
                let abort = rollback_if_err(&mut pipeline, &res);
                let _ = tx.send(res);

                if abort {
                    break;
                }
            },
            Op::DeleteDocuments { deletes, tx } => {
                has_commit = false;

                let res = pipeline.add_deletes(deletes);
                let abort = rollback_if_err(&mut pipeline, &res);
                let _ = tx.send(res);

                if abort {
                    break;
                }
            },
            Op::SetFactory { factory } => {
                pipeline.set_factory(factory);
            },
            Op::CheckAutoCommit { max_duration } => {
                let time_since_last_commit = last_commit.elapsed();

                if !has_commit && (time_since_last_commit >= max_duration) {
                    let res = pipeline.flush_segment().map_err(|e| {
                        error!(error = ?e, "Failed to flush segment due to error.");
                        e
                    });

                    let abort = rollback_if_err(&mut pipeline, &res);

                    if abort {
                        break;
                    }

                    last_commit = Instant::now();
                    has_commit = true;

                    continue;
                }

                let shutdown_timeout = max_duration * 5;
                if has_commit && (time_since_last_commit >= shutdown_timeout) {
                    info!(
                        "Writer has had no activity within {:?}. Shutting writer.",
                        shutdown_timeout
                    );
                    break;
                }
            },
        }
    }
}

fn rollback_if_err<T, E>(pipeline: &mut IndexerPipeline, res: &Result<T, E>) -> bool {
    if res.is_err() {
        if let Err(e) = pipeline.rollback() {
            error!(error = ?e, "Failed to rollback pipeline due to error. The pipeline will now abort.");

            return true;
        }
    }

    false
}

enum Op {
    AddDocuments { docs: Vec<Document>, tx: Responder },
    DeleteDocuments { deletes: Vec<Delete>, tx: Responder },
    SetFactory { factory: IndexerFactory },
    CheckAutoCommit { max_duration: Duration },
}
