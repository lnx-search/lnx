use super::scheduler::{IoScheduler, RingFile};

mod ring_file;
mod stream_reader;

async fn make_tmp_ring_rile() -> RingFile {
    let scheduler = IoScheduler::create().expect("create scheduler failed");
    let file = tempfile::tempfile().unwrap();
    scheduler
        .make_ring_file(0, file)
        .await
        .expect("make ring file failed")
}
