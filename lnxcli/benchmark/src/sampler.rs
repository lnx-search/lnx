use anyhow::{Result, Error};
use itertools::Itertools;
use plotters::prelude::*;

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
    output: String,
    sample_handles: Vec<oneshot::Receiver<ChannelMessage>>,
}

impl Sampler {
    pub(crate) fn new(output: String) -> Self {
        Self {
            output,
            sample_handles: vec![],
        }
    }

    pub(crate) fn get_handle(&mut self) -> SamplerHandle {
        let (handler, rx) = SamplerHandle::new();

        self.sample_handles.push(rx);

        handler
    }

    pub(crate) async fn wait_and_sample(mut self) -> anyhow::Result<()> {
        let mut duration_times = vec![];
        let mut all_results: Vec<Duration> = vec![];
        for sample in self.sample_handles {
            let mut res = sample.await??;

            duration_times.push(res.ran_for);
            all_results.append(&mut res.latencies);
        }

        let total_latency: Duration = all_results
            .iter()
            .sum();

        let average = total_latency / all_results.len() as u32;
        let max = all_results
            .iter()
            .max()
            .ok_or(Error::msg("no results to collect"))?;
        let min = all_results
            .iter()
            .min()
            .ok_or(Error::msg("no results to collect"))?;

        let avg_dur: Duration = duration_times.iter().sum::<Duration>() / duration_times.len() as u32;
        let requests_a_sec = all_results.len() as f64 / avg_dur.as_secs_f64();

        info!("General benchmark results:");
        info!("     Total Requests Sent: {}", all_results.len());
        info!("     Total Requests Sent: {:.2}", requests_a_sec);
        info!("     Average Latency: {:?}", average);
        info!("     Max Latency: {:?}", max);
        info!("     Min Latency: {:?}", min);

        let path = format!("{}/out.png", &self.output);
        let root = BitMapBackend::new(&path, (640, 480)).into_drawing_area();
        root.fill(&WHITE)?;
        let mut chart = ChartBuilder::on(&root)
            .caption("Benchmark Results", ("sans-serif", 50).into_font())
            .margin(5)
            .x_label_area_size(30)
            .y_label_area_size(30)
            .build_cartesian_2d(-1f32..1f32, -0.1f32..1f32)?;

        chart.configure_mesh().draw()?;

        let chunk_size = all_results.len() / 100;
        let mut chunks = vec![];
        for chunk in &all_results.drain(..).into_iter().chunks(chunk_size) {
            chunks.push(chunk.collect::<Vec<Duration>>())
        };

        chart
            .draw_series(LineSeries::new(
                chunks.iter()
                    .enumerate()
                    .map(|(x, y)| (x as f32, (y.iter().sum::<Duration>() / y.len() as u32).as_secs_f32() * 1000f32)),
                &RED,
            ))?
            .label("latency in ms")
            .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED));

        chart
            .configure_series_labels()
            .background_style(&WHITE.mix(0.8))
            .border_style(&BLACK)
            .draw()?;

        Ok(())
    }
}