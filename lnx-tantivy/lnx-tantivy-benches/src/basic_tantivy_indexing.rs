use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tantivy::directory::MmapDirectory;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::{FAST, INDEXED, STORED, TEXT};
use tantivy::{Index, IndexSettings};
use tracing::{info, instrument};

use crate::config::{
    COMMIT_EVERY_N_DOCS,
    DATASET_REPEATS,
    INDEXER_MEMORY_ALLOWANCE,
    NUM_THREADS,
};

#[instrument("basic-tantivy-indexing")]
pub async fn main() -> Result<()> {
    info!("setting up run");

    tokio::fs::create_dir_all("./scratch_space/blocking").await?;

    let mut schema_builder = tantivy::schema::Schema::builder();
    schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
    schema_builder.add_text_field("title", TEXT | STORED | FAST);
    schema_builder.add_text_field("overview", TEXT | STORED | FAST);
    schema_builder.add_text_field("genres", TEXT | STORED | FAST);
    schema_builder.add_text_field("poster", TEXT | STORED | FAST);
    schema_builder.add_i64_field("release_date", INDEXED | STORED | FAST);
    let schema = schema_builder.build();

    let movies = crate::models::load_movies().context("Load movies dataset")?;
    let docs = crate::models::convert_movies_to_tantivy_doc(movies, schema.clone())
        .context("Convert movies to tantivy doc")
        .map(Arc::new)?;

    for &commit_every_n in COMMIT_EVERY_N_DOCS {
        info!(commit_every_n = commit_every_n, "starting run");

        let path = format!("./scratch_space/blocking/run_commit_every_{commit_every_n}");
        let _ = tokio::fs::remove_dir_all(&path).await;
        tokio::fs::create_dir_all(&path)
            .await
            .context("Create dir")?;

        let schema = schema.clone();
        let docs = docs.clone();

        let directory = MmapDirectory::open(&path)?;
        let index = Index::create(directory, schema, IndexSettings::default())
            .context("Create index")?;

        let mut writer: tantivy::IndexWriter =
            index.writer_with_num_threads(NUM_THREADS, INDEXER_MEMORY_ALLOWANCE)?;
        //writer.set_merge_policy(Box::new(NoMergePolicy));

        let start = Instant::now();
        tokio::task::spawn_blocking(move || {
            let mut num_docs = 0;
            for _ in 0..DATASET_REPEATS {
                for doc in docs.iter() {
                    num_docs += 1;
                    writer.add_document(doc.clone())?;

                    if num_docs % commit_every_n == 0 {
                        writer.commit()?;
                    }
                }
            }
            writer.commit()?;
            writer.wait_merging_threads()?;

            Ok::<_, anyhow::Error>(())
        })
        .await??;
        let elapsed = start.elapsed();
        info!(elapsed = ?elapsed, "Completed run");

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
