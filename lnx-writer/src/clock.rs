use std::time::Duration;

use datacake_crdt::{get_unix_timestamp_ms, HLCTimestamp};
use tokio::sync::oneshot;
use once_cell::sync::OnceCell;

static CLOCK: OnceCell<Clock> = OnceCell::new();

pub(crate) async fn init(node_id: u32) {
    let clock = Clock::new(node_id);
    CLOCK.get_or_init(|| clock);
}

pub fn clock() -> &'static Clock {
    CLOCK
        .get()
        .expect("Clock not initialised, unable to proceed.")
}

#[derive(Clone)]
pub struct Clock {
    node_id: u32,
    tx: flume::Sender<Event>,
}

impl Clock {
    pub fn new(node_id: u32) -> Self {
        let ts = HLCTimestamp::new(get_unix_timestamp_ms(), 0, node_id);
        let (tx, rx) = flume::bounded(1000);

        tokio::spawn(run_clock(ts, rx));

        Self { node_id, tx }
    }

    pub async fn register_ts(&self, ts: HLCTimestamp) {
        if ts.node() == self.node_id {
            return;
        }

        self.tx
            .send_async(Event::Register(ts))
            .await
            .expect("Clock actor should never die");
    }

    pub async fn get_time(&self) -> HLCTimestamp {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Event::Get(tx))
            .await
            .expect("Clock actor should never die");

        rx.await.expect("Responder should not be dropped")
    }

    pub fn get_time_blocking(&self) -> HLCTimestamp {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(Event::Get(tx))
            .expect("Clock actor should never die");

        rx.blocking_recv().expect("Responder should not be dropped")
    }
}

pub enum Event {
    Get(oneshot::Sender<HLCTimestamp>),
    Register(HLCTimestamp),
}

async fn run_clock(mut clock: HLCTimestamp, reqs: flume::Receiver<Event>) {
    while let Ok(event) = reqs.recv_async().await {
        match event {
            Event::Get(tx) => {
                let ts = clock.send().expect("Clock counter should not overflow");

                if clock.counter() >= u16::MAX - 10 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }

                let _ = tx.send(ts);
            },
            Event::Register(remote_ts) => {
                clock
                    .recv(&remote_ts)
                    .expect("Clock counter should not overflow");

                if clock.counter() >= u16::MAX - 10 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            },
        }
    }
}
