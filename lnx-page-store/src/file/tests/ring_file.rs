use std::io::{ErrorKind, Write};
use std::{io, ptr};

use crate::file::scheduler::IoScheduler;
use crate::file::tests::make_tmp_ring_rile;

#[tokio::test]
async fn test_ring_file_create() {
    let ring_file = make_tmp_ring_rile().await;
    assert_eq!(ring_file.id(), 0);

    drop(ring_file);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_ring_file_graceful_close() {
    let mut ring_file = make_tmp_ring_rile().await;
    assert!(!ring_file.is_closed());

    ring_file.close().await.expect("close failed");
    assert!(ring_file.is_closed());

    let result = ring_file.ensure_safe_state();
    assert!(matches!(result, Err(err) if err.kind() == ErrorKind::BrokenPipe));
}

#[tokio::test]
async fn test_ring_file_write() {
    let mut ring_file = make_tmp_ring_rile().await;

    let data = vec![1u8; 1024];
    let reply = unsafe {
        ring_file
            .submit_write(data.as_ptr(), data.len(), 0, None)
            .await
            .expect("submit write failed")
    };

    let result = reply.await.expect("write reply failed");
    assert_eq!(result, 1024);

    ring_file.fdatasync().await.expect("fdatasync failed");
}

#[tokio::test]
async fn test_ring_file_fsync_lockout() {
    let scenario = fail::FailScenario::setup();
    fail::cfg("ringfile::fdatasync", "return").unwrap();

    let mut ring_file = make_tmp_ring_rile().await;

    let error = ring_file
        .fdatasync()
        .await
        .expect_err("fdatasync should fail");
    assert_eq!(error.kind(), ErrorKind::Other);

    assert!(ring_file.is_locked_out());

    // I pinky promise this pointer isn't touched.
    let error = unsafe {
        ring_file
            .submit_write(ptr::null(), 0, 0, None)
            .await
            .expect_err("submit write should fail")
    };
    assert_eq!(error.kind(), ErrorKind::ReadOnlyFilesystem);

    scenario.teardown();
}

#[tokio::test]
async fn test_ring_file_fsync_lockout_on_i2o2_error() {
    let mut ring_file = make_tmp_ring_rile().await;

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::poll_reply_future", "return(-12)").unwrap();

    let error = ring_file
        .fdatasync()
        .await
        .expect_err("fdatasync should fail");
    assert_eq!(error.kind(), ErrorKind::OutOfMemory);
    assert!(ring_file.is_locked_out());

    scenario.teardown();
}

#[tokio::test]
async fn test_ring_file_handle_i2o2_cancel() {
    let mut ring_file = make_tmp_ring_rile().await;

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::poll_reply_future", "return(cancelled)").unwrap();

    let reply = unsafe {
        ring_file
            .submit_write(b"".as_ptr(), 0, 0, None)
            .await
            .expect("submit write should succeed")
    };
    let error = crate::file::scheduler::get_reply_or_err(reply)
        .await
        .expect_err("write should return cancelled err");
    assert_eq!(error.kind(), ErrorKind::Interrupted);

    let error = ring_file
        .write_buffer(Vec::new(), 0)
        .await
        .expect_err("write should fail");
    assert_eq!(error.kind(), ErrorKind::Interrupted);

    let mut read_buffer = Vec::with_capacity(5);
    let reply = unsafe {
        ring_file
            .submit_read(read_buffer.as_mut_ptr(), 5, 0, None)
            .await
            .expect("submit read should succeed")
    };
    let error = crate::file::scheduler::get_reply_or_err(reply)
        .await
        .expect_err("write should return cancelled err");
    assert_eq!(error.kind(), ErrorKind::Interrupted);

    let error = ring_file
        .fdatasync()
        .await
        .expect_err("fdatasync should fail");
    assert_eq!(error.kind(), ErrorKind::Interrupted);
    assert!(ring_file.is_locked_out());

    scenario.teardown();
}

#[tokio::test]
async fn test_write_buffer_out_of_storage_error() {
    let ring_file = make_tmp_ring_rile().await;

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::poll_reply_future", "return(12)").unwrap();

    let buffer = b"Hello, World!".to_vec();
    let error = ring_file
        .write_buffer(buffer, 0)
        .await
        .expect_err("write should fail");
    assert_eq!(error.kind(), ErrorKind::StorageFull);

    scenario.teardown();
}

#[rstest::rstest]
#[case::empty_file(0)]
#[case::filled_file(200)]
#[tokio::test]
async fn test_ring_file_get_len(#[case] expected_len: u64) {
    let scheduler = IoScheduler::for_test();
    let tmp_file = tempfile::tempfile().unwrap();
    tmp_file.set_len(expected_len).unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let len = file.get_len().await.expect("get len failed");
    assert_eq!(len, expected_len);
}

#[rstest::rstest]
#[case::no_offset_write_zero(0, 0)]
#[case::no_offset_write_unaligned(200, 0)]
#[case::no_offset_write_aligned(4096, 0)]
#[case::offset_write_unaligned(200, 5)]
#[case::offset_write_aligned(4096, 5)]
#[tokio::test]
async fn test_ring_file_buffer_write(#[case] write_size: usize, #[case] offset: u64) {
    use std::os::unix::fs::FileExt;

    let ring_file = make_tmp_ring_rile().await;

    let data_buffer = vec![1; write_size];
    ring_file
        .write_buffer(data_buffer.clone(), offset)
        .await
        .expect("write buffer failed");

    let file = ring_file.inner_file();
    let mut data_written = vec![0; write_size + offset as usize];
    let n_read = file.read_at(&mut data_written, 0).unwrap();
    assert_eq!(n_read, data_written.len());

    assert_eq!(&data_written[..offset as usize], vec![0; offset as usize]);
    assert_eq!(&data_written[offset as usize..], data_buffer);
}

#[rstest::rstest]
#[case::no_offset_read_zero(0, 0)]
#[case::no_offset_read_small(200, 0)]
#[case::no_offset_read_large(128 << 10, 0)]
#[case::offset_read_zero(0, 2)]
#[case::offset_read_small(200, 2)]
#[case::offset_read_large(128 << 10, 2)]
#[tokio::test]
async fn test_ring_file_read(#[case] read_len: usize, #[case] offset: u64) {
    let scheduler = IoScheduler::for_test();
    let mut tmp_file = tempfile::tempfile().unwrap();

    let mut data_buffer = vec![1; 256 << 10];
    data_buffer[0] = 4;
    data_buffer[1] = 3;
    data_buffer[2] = 2;
    tmp_file.write_all(&data_buffer).unwrap();
    tmp_file.sync_all().unwrap();

    let file = scheduler
        .make_ring_file(1, tmp_file)
        .await
        .expect("Failed to make ring file");

    let mut read_buffer = vec![0; read_len];

    // SAFETY: For testing purposes we don't really care about the cancellation.
    let reply = unsafe {
        file.submit_read(read_buffer.as_mut_ptr(), read_len, offset, None)
            .await
            .expect("submit read failed")
    };
    let reply = reply.await.expect("scheduler cancelled IOP");
    if reply < 0 {
        panic!("read errored: {}", io::Error::from_raw_os_error(-reply));
    }

    assert_eq!(reply as usize, read_len);

    assert_eq!(
        &read_buffer,
        &data_buffer[offset as usize..][..read_len],
        "read buffer does not match",
    );
}
