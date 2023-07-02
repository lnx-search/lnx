use std::borrow::Cow;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use exponential_backoff::Backoff;
use tracing::{instrument, warn};

type TaskFuture = Pin<Box<dyn Future<Output = Result<(), ()>> + Send>>;

/// Spawns a supervising task ensuring the future does not end.
pub fn supervise<F, T, E, S>(fut: F, s: S)
where
    S: SupervisedState + Send + 'static,
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: Debug + Send + 'static,
{
    let fut: TaskFuture = Box::pin(wrap_future(fut));

    let task = Task {
        future: fut,
        state: Box::new(s),
    };

    supervise_task(task);
}

/// Some state which can create new futures in order to replace
/// previous future if they die unexpectedly.
pub trait SupervisedState: Send + Sync + 'static {
    /// Returns the name of the actor/state.
    fn name(&self) -> Cow<'static, str>;

    /// Called to re-create the future from a given state.
    fn recreate(&self, watcher: RecreateCallback) -> anyhow::Result<()>;

    /// The minimum backoff duration for the restarting of actors.
    fn min_backoff_duration(&self) -> Duration {
        Duration::from_millis(250)
    }

    /// The maximum backoff duration for the restarting of actors.
    fn max_backoff_duration(&self) -> Duration {
        Duration::from_secs(5)
    }
}

/// A callback used to submit a new future created by the state.
pub struct RecreateCallback<'a> {
    future: &'a mut Option<TaskFuture>,
}

impl<'a> RecreateCallback<'a> {
    /// Submit a new future to replace the old, dead future.
    pub fn submit<F, T, E>(self, fut: F)
    where
        F: Future<Output = Result<T, E>> + Unpin + Send + 'static,
        T: Send + 'static,
        E: Debug + Send + 'static,
    {
        let fut: TaskFuture = Box::pin(wrap_future(fut));

        (*self.future) = Some(fut);
    }
}

struct Task {
    /// The future to be observed.
    future: TaskFuture,
    /// The state to re-create the future in the even it errors
    /// unexpectedly.
    state: Box<dyn SupervisedState>,
}

async fn wrap_future<F, T, E>(fut: F) -> Result<(), ()>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    E: Debug,
{
    if let Err(e) = fut.await {
        tracing::error!(error = ?e, "Supervised task failed unexpectedly with error");
        Err(())
    } else {
        Ok(())
    }
}

fn supervise_task(mut task: Task) {
    tokio::spawn(async move {
        let backoff = Backoff::new(
            u32::MAX,
            task.state.min_backoff_duration(),
            task.state.max_backoff_duration(),
        );

        loop {
            let res = task.future.await;

            if res.is_ok() {
                return;
            }

            if let Some(future) =
                attempt_recovery(backoff.clone(), task.state.as_ref()).await
            {
                task.future = future;
            } else {
                return;
            }
        }
    });
}

#[instrument(name = "supervisor", skip_all, fields(actor_name = %state.name()))]
async fn attempt_recovery(
    backoff: Backoff,
    state: &dyn SupervisedState,
) -> Option<TaskFuture> {
    for (attempt, duration) in backoff.into_iter().enumerate() {
        tokio::time::sleep(duration).await;
        tracing::info!(
            attempt = attempt,
            "Attempting to restart actor after unexpected exit."
        );

        let mut future = None;
        let callback = RecreateCallback {
            future: &mut future,
        };

        if let Err(e) = state.recreate(callback) {
            warn!(attempt = attempt, error = ?e, "Failed to recreate actor state due to error");
            continue;
        }

        return if let Some(future) = future {
            tracing::info!(attempt = attempt, "Supervisor restarted actor successfully");
            Some(future)
        } else {
            None
        };
    }

    None
}
