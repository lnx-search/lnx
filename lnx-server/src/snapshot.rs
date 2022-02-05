use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use chrono::Utc;
use engine::structures::ROOT_PATH;
use walkdir::{DirEntry, WalkDir};
use zip::read::ZipArchive;
use zip::write::FileOptions;
use zip::CompressionMethod;

static IGNORE_FILES: [&str; 1] = [".tantivy-writer.lock"];

/// Wraps the current storage into a zip file.
///
/// The output file name is in the format of:<br/>
/// `snapshot-<timestamp>-lnx-v<lnx_version>`.
#[instrument(name = "snapshot-writer")]
pub async fn create_snapshot(output_path: &Path) -> Result<()> {
    tokio::fs::create_dir_all(output_path).await?;

    let snapshot_name = format!(
        "snapshot-{}-lnx-v{}",
        Utc::now().timestamp(),
        env!("CARGO_PKG_VERSION"),
    );
    info!("producing snapshot {}", &snapshot_name);

    let output_path = output_path.join(snapshot_name);

    let instant = std::time::Instant::now();
    tokio::task::spawn_blocking(move || collect_into_snapshot(output_path)).await??;

    info!(
        "snapshot took {:?} to build and compress",
        instant.elapsed()
    );

    Ok(())
}

/// Load snapshot file.
///
/// The output file name is in the format of:<br/>
/// `snapshot-<UTC_timestamp>-lnx-v<lnx_version>`.
#[instrument(name = "snapshot-extractor", skip_all)]
pub fn load_snapshot(snapshot_file: &Path) -> Result<()> {
    if !snapshot_file.exists() {
        return Err(anyhow!("file {:?} does not exist", &snapshot_file));
    } else if !snapshot_file.is_file() {
        return Err(anyhow!("file {:?} is a directory", &snapshot_file));
    }

    let file_name = snapshot_file.file_name().unwrap().to_string_lossy();

    info!("extracting snapshot {}", file_name);

    let snapshot_file = snapshot_file.to_path_buf();
    let instant = std::time::Instant::now();
    extract_snapshot(snapshot_file)?;
    info!("snapshot took {:?} to extract", instant.elapsed());

    Ok(())
}

#[instrument(name = "wrapper-worker", skip_all)]
fn collect_into_snapshot(output_file: PathBuf) -> Result<()> {
    let file = std::fs::File::create(output_file)?;

    let mut walker = WalkDir::new(Path::new(ROOT_PATH))
        .into_iter()
        .filter_map(|e| e.ok());

    zip_dir(&mut walker, Path::new(ROOT_PATH), file)?;

    Ok(())
}

#[instrument(name = "extract-worker", skip_all)]
fn extract_snapshot(snapshot: PathBuf) -> Result<()> {
    if Path::new(ROOT_PATH).exists() {
        return Err(anyhow!(
            "{} already exists, the root path must not exist before loading a \
            snapshot to prevent accidental corruption.",
            ROOT_PATH
        ));
    }

    let reader = std::fs::File::open(&snapshot)?;
    let mut zip = ZipArchive::new(reader)?;

    for i in 0..zip.len() {
        let mut file = zip.by_index(i)?;

        if file.is_dir() {
            if let Some(to_create) = file.enclosed_name() {
                info!("expanding directory {}", file.name());
                std::fs::create_dir_all(to_create)?;
            }

            continue;
        }

        if let Some(to_create) = file.enclosed_name() {
            info!("extracting file {}", file.name());
            let mut writer = std::fs::File::create(to_create)?;
            std::io::copy(&mut file, &mut writer)?;

            #[cfg(unix)]
            {
                let target_change = to_create.to_path_buf();
                use std::os::unix::fs::PermissionsExt;

                if let Some(mode) = file.unix_mode() {
                    std::fs::set_permissions(
                        target_change,
                        std::fs::Permissions::from_mode(mode),
                    )?;
                }
            }
        }
    }

    Ok(())
}

fn zip_dir(
    it: &mut impl Iterator<Item = DirEntry>,
    prefix: &Path,
    writer: File,
) -> zip::result::ZipResult<()> {
    let options;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = writer.metadata()?.permissions().mode();
        options = FileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .unix_permissions(perms);
    }

    #[cfg(not(unix))]
    {
        options = FileOptions::default().compression_method(CompressionMethod::Deflated);
    }

    let mut zip = zip::ZipWriter::new(writer);

    let mut buffer = Vec::new();
    for entry in it {
        let path = entry.path();
        let name = path.strip_prefix(prefix).unwrap();

        if path.is_file() {
            let should_ignore = path
                .file_name()
                .map(|v| IGNORE_FILES.contains(&v.to_str().unwrap_or("")))
                .unwrap_or(false);

            if should_ignore {
                continue;
            }

            info!("adding file {:?} as {:?} ...", path, name);
            zip.start_file(path_to_string(path), options)?;
            let mut f = File::open(path)?;

            f.read_to_end(&mut buffer)?;
            zip.write_all(&*buffer)?;
            buffer.clear();
        } else if !name.as_os_str().is_empty() {
            info!("adding dir {:?} as {:?} ...", path, name);
            zip.add_directory(path_to_string(path), options)?;
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
