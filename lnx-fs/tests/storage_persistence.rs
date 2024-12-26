use bytes::Bytes;
use lnx_fs::{Body, RuntimeOptions, VirtualFileSystem};

#[tokio::test]
async fn test_storage_is_persisted() {
    let dir = tempfile::TempDir::new().unwrap();

    let rt_options = RuntimeOptions::builder().num_threads(1).build();
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

    drop(bucket);
    drop(vfs);

    let rt_options = RuntimeOptions::builder().num_threads(1).build();
    let vfs = VirtualFileSystem::mount(dir.path().to_path_buf(), rt_options)
        .await
        .unwrap();

    let bucket = vfs
        .bucket("test")
        .expect("Get existing bucket")
        .enable_statistics_return();

    let body = bucket
        .read("example.txt")
        .await
        .expect("Read existing data");
    let data = body.inner.collect().await.unwrap();
    assert_eq!(data, Bytes::from_static(b"Hello, world!"));
    assert_eq!(body.stats.io_bytes, 13);
}
