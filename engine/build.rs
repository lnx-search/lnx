use std::fs;
use std::path;

use anyhow::Result;
use lzzzz::lz4;

fn main() -> Result<()> {
    // Tell Cargo that if the given file changes, to rerun this build script.
    println!("cargo:rerun-if-changed=./datasets");

    compress_frequency_dicts()?;
    compress_stop_words()?;

    Ok(())
}

fn compress_stop_words() -> Result<()> {
    fs::create_dir_all("./_dist/dictionaries")?;

    if !path::Path::new("./datasets/dictionaries").exists() {
        return Ok(())
    }

    for entry in fs::read_dir("./datasets/dictionaries")? {
        let entry = entry?;
        let name = entry.file_name();
        let (name, _) = name.to_str().unwrap().split_at(name.len() - 4);
        let path = entry.path();
        if path.is_dir() {
            continue
        }

        let data = fs::read(path)?;
        let mut buffer = Vec::with_capacity(data.len());
        let len = lz4::compress(&data, &mut buffer, lz4::ACC_LEVEL_DEFAULT)?;
        let compressed = &buffer[0..len];
        fs::write(&format!("./_dist/dictionaries/{}", name), compressed)?;
    }
    Ok(())
}

fn compress_frequency_dicts() -> Result<()> {
    fs::create_dir_all("./_dist/stop_words")?;

    if !path::Path::new("./datasets/stop_words").exists() {
        return Ok(())
    }

    for entry in fs::read_dir("./datasets/stop_words")? {
        let entry = entry?;
        let name = entry.file_name();
        let (name, _) = name.to_str().unwrap().split_at(name.len() - 4);
        let path = entry.path();
        if path.is_dir() {
            continue
        }

        let data = fs::read(path)?;
        let mut buffer = Vec::with_capacity(data.len());
        let len = lz4::compress(&data, &mut buffer, lz4::ACC_LEVEL_DEFAULT)?;
        let compressed = &buffer[0..len];
        fs::write(&format!("./_dist/stop_words/{}", name), compressed)?;
    }

    Ok(())
}