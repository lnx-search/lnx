use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use chrono::Utc;
use anyhow::Result;
use walkdir::{WalkDir, DirEntry};
use zip::CompressionMethod;
use zip::write::FileOptions;

use crate::cr32_hash;

static IGNORE_FILES: [&str; 1] = [
    ".tantivy-writer.lock",
];


#[instrument(name = "snapshot-writer")]
pub async fn create_snapshot(
    output_path: &Path,
    index_base_dir: &Path,
    index_name: &str,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_path).await?;

    let index_root = index_base_dir.to_path_buf();
    let index_id = cr32_hash(index_name);
    let snapshot_name = format!(
        "snapshot-{}-{}-{}-lnx-v{}",
        index_id,
        index_name,
        Utc::now().timestamp(),
        std::env::var("CARGO_PKG_VERSION")
            .unwrap_or_else(|_| "unknown".to_string()),
    );
    info!("producing snapshot {}", &snapshot_name);

    let output_path = output_path.join(snapshot_name);
    let snapshot_file = output_path.clone();

    let instant = std::time::Instant::now();
    tokio::task::spawn_blocking(move || {
        collect_snapshot(
            output_path,
            index_root,
            index_id,
        )
    }).await??;

    info!("snapshot took {:?} to build and compress", instant.elapsed());

    Ok(snapshot_file)
}

#[instrument(name = "writer-worker", skip_all)]
fn collect_snapshot(output_file: PathBuf, index_root: PathBuf, index_id: u64) -> Result<()> {
    let file = std::fs::File::create(output_file)?;

    let index_location = index_root.join(index_id.to_string());

    let mut walker = WalkDir::new(&index_location)
        .into_iter()
        .filter_map(|e| e.ok());

    zip_dir(&mut walker, &index_location, file)?;

    Ok(())
}


fn zip_dir<T>(
    it: &mut dyn Iterator<Item = DirEntry>,
    prefix: &Path,
    writer: T,
) -> zip::result::ZipResult<()>
where
    T: Write + Seek,
{
    let mut zip = zip::ZipWriter::new(writer);
    let options = FileOptions::default()
        .compression_method(CompressionMethod::Deflated);

    let mut buffer = Vec::new();
    for entry in it {
        let path = entry.path();
        let name = path.strip_prefix(prefix).unwrap();

        if path.is_file() {
            let should_ignore = path.file_name()
                .map(|v| IGNORE_FILES.contains(&v.to_str().unwrap_or("")))
                .unwrap_or(false);

            if should_ignore {
                continue;
            }

            info!("adding file {:?} as {:?} ...", path, name);
            zip.start_file(path_to_string(&path), options)?;
            let mut f = File::open(path)?;

            f.read_to_end(&mut buffer)?;
            zip.write_all(&*buffer)?;
            buffer.clear();
        } else if name.as_os_str().len() != 0 {
            info!("adding dir {:?} as {:?} ...", path, name);
            zip.add_directory(path_to_string(&path), options)?;
        }
    }
    zip.finish()?;
    Result::Ok(())
}

fn path_to_string(path: &std::path::Path) -> String {
    let mut path_str = String::new();
    for component in path.components() {
        if let std::path::Component::Normal(os_str) = component {
            if !path_str.is_empty() {
                path_str.push('/');
            }
            path_str.push_str(&*os_str.to_string_lossy());
        }
    }
    path_str
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_zipping() -> anyhow::Result<()> {
        let index_name = "zip-test";
        let index_id = cr32_hash(index_name);

        std::fs::create_dir_all(format!("./tmp/{}/inner", index_id))?;

        std::fs::write(format!("./tmp/{}/inner/hello.txt", index_id), "Hello World!")?;
        std::fs::write(format!("./tmp/{}/hello.txt", index_id), "Hello World!")?;
        std::fs::write(format!("./tmp/{}/test-file", index_id), "Hello World!")?;

        let res = create_snapshot(
            Path::new("./tmp/snapshots"),
            Path::new("./tmp"),
            index_name,
        ).await;

        assert!(res.is_ok());

        let out_file = res.unwrap();
        assert!(out_file.exists());

        Ok(())
    }
}