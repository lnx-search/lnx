use std::fs::File;
use std::hint::black_box;
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;
use std::time::Instant;

use anyhow::Result;
use futures_util::AsyncReadExt;
use glommio::io::DmaStreamReaderBuilder;
use humansize::DECIMAL;
use lnx_fs::{Body, BucketConfig, Bytes, RuntimeOptions, VirtualFileSystem};
use tracing::info;

const SIZES: &[usize] = &[
    1 << 10,   // 1KB
    10 << 10,  // 10KB
    100 << 10, // 100KB
    1 << 20,   // 1MB
    10 << 20,  // 10MB
    100 << 20, // 100MB
];

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    tracing_subscriber::fmt::init();

    info!("Benchmarking READ blocking IO");
    benchmark_reader_full_scan_blocking_io().await?;

    info!("Benchmarking READ glommio IO");
    benchmark_reader_full_scan_glommio_io().await?;
    
    info!("Benchmarking READ VFS IO");
    benchmark_reader_full_scan_vfs_io().await?;

    info!("Benchmarking WRITE blocking IO");
    benchmark_writer_blocking_io().await?;

    info!("Benchmarking WRITE VFS IO");
    benchmark_writer_vfs_io().await?;

    Ok(())
}

async fn benchmark_writer_blocking_io() -> Result<()> {
    let tmp_dir = tempfile::TempDir::new_in("./scratch_space/blocking/")?;

    for &size in SIZES {
        let mut buffer = vec![0; size];
        fastrand::fill(&mut buffer);

        let path = tmp_dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let now = Instant::now();
            for _ in 0..25 {
                let file = tempfile::NamedTempFile::new_in(&path)?;
                let mut writer = BufWriter::new(file);
                writer.write_all(&buffer)?;
                writer.flush()?;
                writer.get_mut().as_file_mut().sync_data()?;
            }

            let elapsed = now.elapsed() / 25;
            let secs = elapsed.as_secs_f32();
            let rate = size as f32 / secs;
            let formatted_size = humansize::format_size(size, DECIMAL);
            let formatted_rate = humansize::format_size(rate as u64, DECIMAL);

            info!("Blocking IO run: {formatted_size} {elapsed:?} {formatted_rate}/s");

            Ok::<_, anyhow::Error>(())
        })
        .await??;
    }

    Ok(())
}

async fn benchmark_reader_full_scan_blocking_io() -> Result<()> {
    let tmp_dir = tempfile::TempDir::new_in("./scratch_space/blocking/")?;

    const READ_SIZE: usize = 40 << 30;
    const RUNS: u32 = 5;

    let path = tmp_dir.path().join("size_scan_test");
    fille_file(&path, READ_SIZE)?;

    let mut reader = BufReader::with_capacity(32 << 10, File::open(path)?);

    tokio::task::spawn_blocking(move || {
        let now = Instant::now();
        for _ in 0..RUNS {
            reader.seek(SeekFrom::Start(0))?;
            let mut temp_buf = vec![0; 512 << 10];
            let mut bytes_read = 0;
            while bytes_read < READ_SIZE {
                let n = reader.read(&mut temp_buf[..])?;
                bytes_read += n;
            }
            black_box(bytes_read);
        }

        let elapsed = now.elapsed() / RUNS;
        let secs = elapsed.as_secs_f32();
        let rate = READ_SIZE as f32 / secs;
        let formatted_size = humansize::format_size(READ_SIZE, DECIMAL);
        let formatted_rate = humansize::format_size(rate as u64, DECIMAL);

        info!("Blocking IO run: {formatted_size} {elapsed:?} {formatted_rate}/s");

        Ok::<_, anyhow::Error>(())
    }).await??;

    Ok(())
}

async fn benchmark_writer_vfs_io() -> Result<()> {
    let tmp_dir = tempfile::TempDir::new_in("./scratch_space/vfs/")?;
    let rt_options = RuntimeOptions::builder().num_threads(1).build();

    let vfs = VirtualFileSystem::mount(tmp_dir.path().to_path_buf(), rt_options).await?;
    let bucket = vfs.create_bucket("benches").await?;

    let config_update = BucketConfig::builder().flush_delay_millis(50).build();
    bucket.update_config(config_update).await?;
    drop(bucket);
    let bucket = vfs.reload_bucket("benches").await?;

    for &size in SIZES {
        let mut buffer = vec![0; size];
        fastrand::fill(&mut buffer);

        let body = Bytes::copy_from_slice(&buffer);

        let now = Instant::now();
        for _ in 0..25 {
            bucket
                .write("example.txt", Body::complete(body.clone()))
                .await?;
        }

        let elapsed = now.elapsed() / 25;
        let secs = elapsed.as_secs_f32();
        let rate = size as f32 / secs;
        let formatted_size = humansize::format_size(size, DECIMAL);
        let formatted_rate = humansize::format_size(rate as u64, DECIMAL);

        info!("VFS IO run: {formatted_size} {elapsed:?} {formatted_rate}/s");
    }

    drop(vfs);

    Ok(())
}

async fn benchmark_reader_full_scan_vfs_io() -> Result<()> {
    let tmp_dir = tempfile::TempDir::new_in("./scratch_space/vfs/")?;
    let rt_options = RuntimeOptions::builder().num_threads(1).build();

    let vfs = VirtualFileSystem::mount(tmp_dir.path().to_path_buf(), rt_options).await?;
    let bucket = vfs.create_bucket("benches").await?;

    const READ_SIZE: usize = 40 << 30;
    const RUNS: u32 = 5;
    
    let(tx, body) = Body::channel();
    tokio::spawn(async move {
        let mut buffer = vec![0; 1<<20];
        fastrand::fill(&mut buffer);
        let buffer = Bytes::from(buffer);
        
        let mut bytes_written = 0;
        while bytes_written < READ_SIZE {
            tx.send(buffer.clone()).await;
            bytes_written += buffer.len();
        }
        
        tx.finish().await;
    });
    
    bucket.write("size_scan_test", body).await?;
    
    let now = Instant::now();
    for _ in 0..RUNS {
        let body = bucket.read("size_scan_test").await?;
        
        let mut bytes_read = 0;
        while let Some(chunk) = body.next().await? {
            bytes_read += chunk.len();
        }
        assert!(bytes_read >= READ_SIZE);
    }

    let elapsed = now.elapsed() / RUNS;
    let secs = elapsed.as_secs_f32();
    let rate = READ_SIZE as f32 / secs;
    let formatted_size = humansize::format_size(READ_SIZE, DECIMAL);
    let formatted_rate = humansize::format_size(rate as u64, DECIMAL);

    info!("VFS IO run: {formatted_size} {elapsed:?} {formatted_rate}/s");

    Ok(())
}

async fn benchmark_reader_full_scan_glommio_io() -> Result<()> {
    let tmp_dir = tempfile::TempDir::new_in("./scratch_space/vfs/")?;
    
    const READ_SIZE: usize = 40 << 30;
    const RUNS: u32 = 5;
    
    let path = tmp_dir.path().join("size_scan_test");
    fille_file(&path, READ_SIZE)?;
    
    tokio::task::spawn_blocking(move || {
        let executor = glommio::LocalExecutor::default();
        executor.run(async move {
            let file = Rc::new(glommio::io::DmaFile::open(path).await?);
            
            let now = Instant::now();
            for _ in 0..RUNS {
                let mut reader = DmaStreamReaderBuilder::from_rc(file.clone())
                    .with_read_ahead(10)
                    .with_buffer_size(512 << 10)
                    .build();

                let mut temp_buf = vec![0; 512 << 10];
                let mut bytes_read = 0;
                while bytes_read < READ_SIZE {
                    let n = reader.read(&mut temp_buf[..]).await?;
                    bytes_read += n;                    
                }
                black_box(bytes_read);
            }

            let elapsed = now.elapsed() / RUNS;
            let secs = elapsed.as_secs_f32();
            let rate = READ_SIZE as f32 / secs;
            let formatted_size = humansize::format_size(READ_SIZE, DECIMAL);
            let formatted_rate = humansize::format_size(rate as u64, DECIMAL);

            info!("Glommio IO run: {formatted_size} {elapsed:?} {formatted_rate}/s");
            
            Ok::<_, io::Error>(())
        })
    }).await??;

    Ok(())
}

fn fille_file(path: &Path, size: usize) -> Result<()> {
    let mut buffer = vec![0; 1<<20];
    fastrand::fill(&mut buffer);
    let mut file = File::options()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)?;

    let mut bytes_written = 0;
    while bytes_written < size {
        file.write_all(&buffer)?;
        bytes_written += buffer.len();
    }
    file.sync_all()?;
    
    Ok(())
}