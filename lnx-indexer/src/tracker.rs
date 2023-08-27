use tokio::sync::oneshot;

/// Creates a new ingestion tracker pair.
pub fn tracker() -> (IngestionCompleter, IngestionTracker) {
    let (tx, rx) = oneshot::channel();

    (
        IngestionCompleter { completion: tx },
        IngestionTracker { completion: rx },
    )
}

/// A struct that marks an ingestion task as completed.
pub struct IngestionCompleter {
    completion: oneshot::Sender<()>,
}

impl IngestionCompleter {
    /// Mark the ingestion task as completed.
    pub fn complete(self) {
        let _ = self.completion.send(());
    }
}

/// A struct that lets a request wait until the
/// result has been ingested giving some backpressure handling.
pub struct IngestionTracker {
    completion: oneshot::Receiver<()>,
}

impl IngestionTracker {
    /// Waits for the ingestion task to be completed.
    pub async fn wait(self) {
        let _ = self.completion.await;
    }
}
