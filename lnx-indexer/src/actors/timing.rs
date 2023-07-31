use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;
use tracing::warn;

use super::indexer::IndexingOp;
use crate::clock::CommitClock;

/// An actor that triggers events every `N` seconds according to the [CommitClock].
pub struct TimingActor {
    clock: CommitClock,
    events: flume::Sender<IndexingOp>,
    stop: Arc<AtomicBool>,
}

impl TimingActor {
    /// Spawns a new timing actor.
    pub fn spawn(
        clock: CommitClock,
        events: flume::Sender<IndexingOp>,
    ) -> Arc<AtomicBool> {
        let stop = Arc::new(AtomicBool::default());

        let actor = Self {
            clock,
            events,
            stop: stop.clone(),
        };

        tokio::spawn(actor.start());

        stop
    }

    async fn start(mut self) {
        loop {
            let delay = self.clock.next();
            tokio::time::sleep(delay).await;

            if self.stop.load(Ordering::Relaxed) {
                return;
            }

            let (tx, rx) = oneshot::channel();
            let res = self.events.send_async(IndexingOp::Commit(tx)).await;

            // The actor died, normally because the commit failed
            if rx.await.is_err() {
                warn!("Timing actor is shutting down as indexing actor has died");
                return
            }

            match res {
                // Our indexing actor has died.
                Err(_) => return,
                Ok(()) => {
                    if self.clock.has_missed_next_window() {
                        warn!(
                            "The timing actor failed to commit within the timing interval of {:?}",
                            self.clock.interval(),
                        );
                    }
                }
            }
        }
    }
}
