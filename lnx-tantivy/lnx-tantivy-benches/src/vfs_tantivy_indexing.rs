use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use lnx_fs::{Bucket, RuntimeOptions, VirtualFileSystem};
use tantivy::schema::{Schema, FAST, INDEXED, STORED, TEXT};
use tracing::{info, instrument};

use crate::config::{COMMIT_EVERY_N_DOCS, DATASET_REPEATS, NUM_THREADS};

#[instrument("vfs-tantivy-indexing")]
pub async fn main() -> Result<()> {
    info!("setting up run");

    tokio::fs::create_dir_all("./scratch_space/vfs").await?;

    let temp_dir = tempfile::TempDir::new_in("./scratch_space/vfs")?;

    let runtime_options = RuntimeOptions::builder().num_threads(1).build();

    let vfs = VirtualFileSystem::mount(temp_dir.path().to_path_buf(), runtime_options)
        .await
        .context("Create virtual file system")?;

    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
    schema_builder.add_text_field("title", TEXT | STORED | FAST);
    schema_builder.add_text_field("overview", TEXT | STORED | FAST);
    schema_builder.add_text_field("genres", TEXT | STORED | FAST);
    schema_builder.add_text_field("poster", TEXT | STORED | FAST);
    schema_builder.add_i64_field("release_date", INDEXED | STORED | FAST);
    let schema = schema_builder.build();

    let movies = crate::models::load_movies().context("Load movies dataset")?;
    let docs = crate::models::convert_movies_to_tantivy_doc(movies, schema.clone())
        .context("Convert movies to tantivy doc")?;

    for &commit_every_n in COMMIT_EVERY_N_DOCS {
        info!(commit_every_n = commit_every_n, "starting run");

        let bucket_name = format!("benches_commit_{commit_every_n}");
        let bucket = vfs
            .create_bucket(&bucket_name)
            .await
            .context("Create bucket")?;

        let (tx, rx) = flume::bounded(128);

        let mut handles = Vec::new();
        for _ in 0..NUM_THREADS {
            let handle = tokio::task::spawn(indexer_task(
                rx.clone(),
                bucket.clone(),
                schema.clone(),
                commit_every_n,
            ));

            handles.push(handle);
        }

        let start = Instant::now();
        for _ in 0..DATASET_REPEATS {
            for doc in docs.iter() {
                tx.send_async(doc.clone()).await?;
            }
        }
        drop(tx);

        for handle in handles {
            handle.await??;
        }
        let elapsed = start.elapsed();
        info!(elapsed = ?elapsed, "Completed run");

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    drop(vfs);

    tokio::time::sleep(Duration::from_secs(2)).await;

    Ok(())
}

async fn indexer_task(
    incoming: flume::Receiver<tantivy::TantivyDocument>,
    bucket: Bucket,
    schema: Schema,
    commit_every_n: usize,
) -> Result<()> {
    while !incoming.is_disconnected() {
        let schema = schema.clone();
        let incoming = incoming.clone();
        let segment = tokio::task::spawn_blocking(move || {
            let mut indexer = lnx_tantivy::indexer::SingleSegmentIndexer::new(schema);

            let mut doc_count = 0;
            while let Ok(doc) = incoming.recv() {
                doc_count += 1;
                indexer.add_document(doc)?;

                if doc_count >= commit_every_n {
                    break;
                }
            }

            let memory = indexer.finish()?;
            Ok::<_, anyhow::Error>(memory)
        })
        .await??;

        segment.write_to(&bucket).await?;
    }

    Ok(())
}
