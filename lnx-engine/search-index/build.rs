use std::io::Write;
use std::{fs, path};

use anyhow::Result;
use flate2::write::GzEncoder;
use flate2::Compression;

fn main() -> Result<()> {
    // Tell Cargo that if the given file changes, to rerun this build script.
    println!("cargo:rerun-if-changed=./datasets");

    let _ = fs::remove_dir_all("./_dist");
    fs::create_dir_all("./_dist")?;

    compress_stop_words()?;

    Ok(())
}

fn compress_stop_words() -> Result<()> {
    if !path::Path::new("./datasets/stop_words").exists() {
        fs::write("./_dist/stop_words", b"")?;
        return Ok(());
    }

    let mut data = vec![];
    for entry in fs::read_dir("./datasets/stop_words")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            continue;
        }

        let mut file = fs::read(path)?;
        data.append(&mut file);
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
    encoder.write_all(&data)?;
    let data = encoder.finish()?;

    fs::write("./_dist/stop_words", &data)?;

    Ok(())
}
