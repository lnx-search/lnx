use std::io::ErrorKind;
use std::sync::Arc;

use crate::file::ctx;
use crate::file::stream_reader::StreamReaderBuilder;
use crate::file::tests::make_tmp_ring_rile;

#[rstest::rstest]
#[case(32 << 10, 0)]
#[should_panic(expected = "buffer size must not be zero")]
#[case(0, 0)]
#[should_panic(expected = "buffer size must a multiple of DISK_ALIGN")]
#[case(100, 0)]
#[should_panic(expected = "offset must be a multiple of DISK_ALIGN")]
#[case(32 << 10, 15)]
#[tokio::test]
async fn test_stream_reader_builder_construction(
    #[case] buffer_size: usize,
    #[case] offset: u64,
) {
    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let ring_file = make_tmp_ring_rile().await;

    let _reader = StreamReaderBuilder::new(ctx.clone(), ring_file)
        .with_buffer_size(buffer_size)
        .with_offset(offset)
        .build();
}

#[rstest::rstest]
#[tokio::test]
async fn test_reader_single_read(
    #[values(0, 4096)] offset: u64,
    #[values(20, 512, 4096, 64 << 10)] read_size: usize,
) {
    let sample_buffer = (0..(1 << 20))
        .map(|value| (value % 256) as u8)
        .collect::<Vec<u8>>();

    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let ring_file = make_tmp_ring_rile().await;
    ring_file
        .write_buffer(sample_buffer.clone(), offset)
        .await
        .expect("write to file");

    let mut reader = StreamReaderBuilder::new(ctx.clone(), ring_file)
        .with_offset(offset)
        .build();

    let mut buffer = vec![0; read_size];
    reader
        .read_exact(&mut buffer)
        .await
        .expect("read to buffer");
    assert_eq!(&buffer, &sample_buffer[..read_size]);
}

#[tokio::test]
async fn test_reader_short_read_error() {
    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let ring_file = make_tmp_ring_rile().await;
    ring_file
        .write_buffer(vec![1; 4 << 10], 0)
        .await
        .expect("write to file");

    let mut reader = StreamReaderBuilder::new(ctx.clone(), ring_file).build();

    let scenario = fail::FailScenario::setup();
    fail::cfg("i2o2::fail::poll_reply_future", "return(12)").unwrap();

    // We use the fail point injection to simulate this, not an actual kernel call.
    let mut buffer = vec![0; 512];
    let err = reader
        .read_exact(&mut buffer)
        .await
        .expect_err("read exact should return error due to short read");
    assert_eq!(err.kind(), ErrorKind::BrokenPipe);
    assert_eq!(
        err.to_string(),
        "kernel read returned small buffer after retries"
    );

    scenario.teardown();
}

#[tokio::test]
async fn test_reader_unexpected_eof() {
    let ctx = Arc::new(ctx::FileContext::for_test(false));
    let ring_file = make_tmp_ring_rile().await;
    ring_file
        .write_buffer(vec![1; 4 << 10], 0)
        .await
        .expect("write to file");

    let mut reader = StreamReaderBuilder::new(ctx.clone(), ring_file).build();

    let mut buffer = vec![0; 8 << 10];
    let err = reader
        .read_exact(&mut buffer)
        .await
        .expect_err("read exact should return error due to unexpected EOF");
    assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    assert_eq!(err.to_string(), "could not fill buffer completely");
}
