use anyhow::{Result, Error};

use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};


pub(crate) type ChannelMessage = Result<SampleData>;


/// The data sampled from the benchmark
pub(crate) struct SampleData {
    /// All request latencies.
    latencies: Vec<Duration>,

    /// How long the system took to run though.
    ran_for: Duration,
}


pub(crate) struct SamplerHandle {
    sample: SampleData,
    start: Instant,
    submit: oneshot::Sender<ChannelMessage>
}

impl SamplerHandle {
    pub(crate) fn finish(mut self) {
        self.sample.ran_for = self.start.elapsed();
        let _ = self.submit.send(Ok(self.sample));
    }

    pub(crate) fn abort(self, error: Error) {
        let _ = self.submit.send(Err(error));
    }

    pub(crate) fn new() -> (Self, oneshot::Receiver<ChannelMessage>) {
        let sample = SampleData {
            latencies: vec![],
            ran_for: Duration::default(),
        };

        let (tx, rx) = oneshot::channel();

        let inst = Self {
            sample,
            start: Instant::now(),
            submit: tx,
        };

        (inst, rx)
    }

    pub(crate) fn add_latency(&mut self, dur: Duration) {
        self.sample.latencies.push(dur);
    }

    pub(crate) fn start_timing(&mut self) {
        self.start = Instant::now();
    }
}


pub(crate) struct Sampler {
    sample_handles: Vec<oneshot::Receiver<ChannelMessage>>,
}

impl Sampler {
    pub(crate) fn new() -> Self {
        Self {
            sample_handles: vec![],
        }
    }

    pub(crate) fn get_handle(&mut self) -> SamplerHandle {
        let (handler, rx) = SamplerHandle::new();

        self.sample_handles.push(rx);

        handler
    }

    pub(crate) async fn wait_and_sample(mut self) {

    }
}