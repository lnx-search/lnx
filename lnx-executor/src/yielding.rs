use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::sync::Recorder;
use hdrhistogram::{Histogram, SyncHistogram};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::schedule_default;

const MAX_RECORD_LATENCY: Duration = Duration::from_secs(5);

/// Execute IO on the executor system.
pub fn schedule_yielding_io<IO>(ctx: &YieldContext, io: IO)
where
    IO: YieldingIo,
{
    let ctx = ctx.clone();
    ctx.inc_task();
    let limit = ctx.get_limit();

    let op = move || run_io(ctx, io, Duration::default(), limit);
    schedule_default(op)
}

fn run_io<IO>(mut ctx: YieldContext, mut io: IO, elapsed: Duration, limit: Duration)
where
    IO: YieldingIo,
{
    ctx.inc_step();
    let start = Instant::now();
    let step = io.step(limit);
    ctx.record_latency(start.elapsed());

    match step {
        Step::Completed(took) => ctx.record_latency(took + elapsed),
        Step::WouldBlock(took) => {
            // Re-Schedule the task with the new elapsed duration.
            let op = move || run_io(ctx, io, elapsed + took, limit);
            schedule_default(op);
        },
        Step::Aborted => {},
    }
}

#[derive(Debug, Clone)]
/// The runtime context for a given yielding task.
///
/// This can be used for grouping multiple
/// IO tasks as part of the same collection in order
/// to balance metrics and runtime allowance.
pub struct YieldContext {
    num_tasks: Arc<AtomicU64>,
    num_steps: Arc<AtomicU64>,
    epoch: Arc<AtomicU64>,

    latency_reader: Arc<Mutex<SyncHistogram<u64>>>,
    latency_recording: Recorder<u64>,

    limit: Arc<AtomicU64>,
}

impl Default for YieldContext {
    fn default() -> Self {
        static DEFAULT_YIELD_CONTEXT: OnceCell<YieldContext> = OnceCell::new();
        DEFAULT_YIELD_CONTEXT.get_or_init(Self::new).clone()
    }
}

impl YieldContext {
    /// Create a new unique yield context.
    pub fn new() -> Self {
        let recorder =
            Histogram::new_with_max(MAX_RECORD_LATENCY.as_micros() as u64, 5).unwrap();
        let sync = recorder.into_sync();
        let recorder = sync.recorder();

        Self {
            num_tasks: Arc::new(AtomicU64::new(0)),
            num_steps: Arc::new(AtomicU64::new(0)),
            epoch: Arc::new(AtomicU64::new(0)),
            latency_reader: Arc::new(Mutex::new(sync)),
            latency_recording: recorder,
            limit: Arc::new(
                AtomicU64::new(Duration::from_millis(10).as_micros() as u64),
            ),
        }
    }

    /// The number of tasks executed within this context.
    pub fn num_tasks(&self) -> u64 {
        self.num_tasks.load(Ordering::Relaxed)
    }

    /// The average P99 execution time of each task.
    pub fn p99_execution_time(&self) -> Duration {
        let mut lock = self.latency_reader.lock();
        lock.refresh_timeout(Duration::default());
        let micros = lock.value_at_quantile(0.99);
        Duration::from_micros(micros)
    }

    /// Set/override the execution limit.
    pub fn set_limit(&self, dur: Duration) {
        self.limit.store(dur.as_micros() as u64, Ordering::Relaxed);
    }

    /// Get the given task limit.
    fn get_limit(&self) -> Duration {
        let epoch = self.epoch.load(Ordering::Relaxed);

        // Re-consider limit every 500 steps.
        if (epoch % 500) == 0 {
            if let Some(mut lock) = self.latency_reader.try_lock() {
                lock.refresh_timeout(Duration::default());

                let micros = lock.value_at_quantile(0.99);

                if micros == 0 {
                    return Duration::from_micros(self.limit.load(Ordering::Relaxed));
                }

                let millis = micros / 1_000;
                let limit_micros =
                    micros / (millis.ilog10() as f64 * 1.2).round() as u64;

                self.limit.store(limit_micros, Ordering::Relaxed);
                self.epoch.fetch_add(1, Ordering::Relaxed);
            };
        }

        let limit_micros = self.limit.load(Ordering::Relaxed);
        Duration::from_micros(limit_micros)
    }

    /// Record the total duration of a given yielding task for this ctx.
    fn record_latency(&mut self, dur: Duration) {
        let _ = self.latency_recording.record(dur.as_micros() as u64);
    }

    /// Increment the task counter for this ctx.
    fn inc_task(&self) {
        self.num_tasks.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the step counter for this ctx.
    fn inc_step(&self) {
        self.epoch.fetch_add(1, Ordering::Relaxed);
        self.num_steps.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub enum Step {
    Completed(Duration),
    WouldBlock(Duration),
    Aborted,
}

pub trait YieldingIo: Send + 'static {
    /// Does some execution of IO upto the given time limit.
    ///
    /// If th execution has gone beyond the time limit
    /// the implementation should return `Step::WouldBlock`.
    fn step(&mut self, limit: Duration) -> Step;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_yielding() {
        let ctx = YieldContext::new();

        for _ in 0..5 {
            let mut ctx = ctx.clone();

            ctx.inc_task();
            ctx.inc_step();
            ctx.record_latency(Duration::from_millis(43));
            ctx.record_latency(Duration::from_millis(60));
            ctx.record_latency(Duration::from_millis(45));
            ctx.record_latency(Duration::from_millis(12));
            ctx.record_latency(Duration::from_millis(140));
        }

        {
            let mut reader = ctx.latency_reader.lock();
            reader.refresh_timeout(Duration::default());
            assert_eq!(25, reader.len());
        }

        ctx.epoch.store(500, Ordering::Relaxed);
        let limit = ctx.get_limit();
        assert_eq!(limit, Duration::from_millis(70));
    }
}
