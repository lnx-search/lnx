use std::path::{Path, PathBuf};
use chrono::Utc;
use anyhow::Result;
use walkdir::WalkDir;

use crate::cr32_hash;

static IGNORE_FILES: [&str; 1] = [
    ".tantivy-writer.lock",
];


pub async fn create_snapshot(output_path: &Path, index_base_dir: &Path, index_name: &str) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_path).await?;

    let index_id = cr32_hash(index_name);
    let snapshot_name = format!("{}-{}-{}", index_id, Utc::now(), env!("CARGO_PKG_VERSION"));
    let output_path = output_path.join(snapshot_name);

    tokio::task::spawn_blocking(move || {
        collect_snapshot(
            output_path.clone(),
            index_base_dir.into_path_buf(),
            index_id,
        )
    }).await??;

    Ok(output_path)
}

fn collect_snapshot(output_file: PathBuf, index_base_dir: PathBuf, index_id: u64) -> Result<()> {
    let file = std::fs::File::create(output_file)?;

    let index_location = index_base_dir.join(index_id);

    let walker = WalkDir::new(index_location)
        .into_iter()
        .filter_map(|e| e.ok());

    for entry in walker {
        println!("{}", entry.path().display());
    }

    todo!()
}
