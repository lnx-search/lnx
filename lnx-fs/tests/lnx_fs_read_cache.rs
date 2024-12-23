use bytes::Bytes;
use lnx_fs::{Body, RuntimeOptions, VirtualFileSystem};

#[tokio::test]
async fn test_read_cache_used() {
    let rt_options = RuntimeOptions::builder().num_threads(1).build();
    let dir = tempfile::TempDir::new().unwrap();
    let vfs = VirtualFileSystem::mount(dir.path().to_path_buf(), rt_options)
        .await
        .unwrap();

    let bucket = vfs
        .create_bucket("test")
        .await
        .expect("Create bucket")
        .enable_statistics_return();

    let result = bucket
        .write(
            "example.txt",
            Body::complete(Bytes::from_static(b"Hello, world!")),
        )
        .await
        .expect("Write file");
    assert_eq!(result.stats.cache_evictions, 0);
    assert_eq!(result.stats.io_bytes, 72);
    assert_eq!(result.stats.evicted_bytes, 0);

    let first_read = bucket.read("example.txt").await.expect("Read file");
    let data = first_read.inner.collect().await.unwrap();
    assert_eq!(data, Bytes::from_static(b"Hello, world!"));
    assert_eq!(first_read.stats.cached_bytes, 0);
    assert_eq!(first_read.stats.cache_hits, 0);
    assert_eq!(first_read.stats.cache_misses, 1);
    assert_eq!(first_read.stats.io_bytes, 8192);

    let second_read = bucket.read("example.txt").await.expect("Read file");
    let data = second_read.inner.collect().await.unwrap();
    assert_eq!(data, Bytes::from_static(b"Hello, world!"));
    assert_eq!(second_read.stats.cached_bytes, 13);
    assert_eq!(second_read.stats.cache_hits, 1);
    assert_eq!(second_read.stats.cache_misses, 0);
    assert_eq!(second_read.stats.io_bytes, 0);
}
