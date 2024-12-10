use std::io::{BufWriter, Write};
use std::time::Instant;
use anyhow::Result;
use humansize::DECIMAL;
use tracing::info;
use lnx_fs::{Body, Bytes, RuntimeOptions, VirtualFileSystem};

const SIZES: &[usize] = &[
    1 << 10,    // 1KB
    10 << 10,   // 10KB
    100 << 10,  // 100KB
    1 << 20,    // 1MB
    10 << 20,   // 10MB
    100 << 20   // 100MB
];

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    
    tracing_subscriber::fmt::init();
    
    info!("Benchmarking blocking IO");
    benchmark_blocking_io().await?;
    
    info!("Benchmarking VFS IO");
    benchmark_vfs_io().await?;
    
    Ok(())
}

async fn benchmark_blocking_io() -> Result<()> { 
    let tmp_dir = tempfile::TempDir::new()?;
    
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
        }).await??;
    }
    
    Ok(())
}


async fn benchmark_vfs_io() -> Result<()> {
    let tmp_dir = tempfile::TempDir::new()?;
    let rt_options = RuntimeOptions::builder()
        .num_threads(1)
        .build();
    
    let vfs = VirtualFileSystem::mount(tmp_dir.path().to_path_buf(), rt_options).await?;
    let bucket = vfs.create_bucket("benches").await?;
    
    for &size in SIZES {
        let mut buffer = vec![0; size];
        fastrand::fill(&mut buffer);
        
        let body = Bytes::copy_from_slice(&buffer);
        
        let now = Instant::now();
        for _ in 0..25 {
            bucket.write(
                "example.txt", 
                Body::complete(body.clone())
            ).await?;
        }

        let elapsed = now.elapsed() / 25;
        let secs = elapsed.as_secs_f32();
        let rate = size as f32 / secs;
        let formatted_size = humansize::format_size(size, DECIMAL);
        let formatted_rate = humansize::format_size(rate as u64, DECIMAL);

        info!("VFS IO run: {formatted_size} {elapsed:?} {formatted_rate}/s");
    }

    Ok(())
}