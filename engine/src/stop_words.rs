use std::io::Write;

use anyhow::{Error, Result};
use flate2::write::GzDecoder;
use hashbrown::HashSet;
use once_cell::sync::OnceCell;

static STOP_WORDS: OnceCell<Vec<String>> = OnceCell::new();
static STOP_WORDS_HASHSET: OnceCell<HashSet<String>> = OnceCell::new();

pub(crate) fn init_stop_words() -> Result<()> {
    let buffer: &[u8] = include_bytes!("../_dist/stop_words");
    let mut data = GzDecoder::new(vec![]);
    data.write_all(buffer)?;
    let data = data.finish()?;

    let words = String::from_utf8(data)
        .map_err(|_| Error::msg("failed to parse stop words from linked data."))?;

    let mut hashed_data = HashSet::new();
    let mut data = vec![];
    for word in words.to_lowercase().split("\n") {
        if let Some(word) = word.strip_suffix("\r") {
            data.push(word.to_string());
            hashed_data.insert(word.to_string());
        }
    }

    let _ = STOP_WORDS.set(data);
    let _ = STOP_WORDS_HASHSET.set(hashed_data);

    Ok(())
}

pub(crate) fn get_stop_words() -> Result<Vec<String>> {
    if let Some(words) = STOP_WORDS.get() {
        Ok(words.clone())
    } else {
        Err(Error::msg(
            "stop words was not initialised at time of calling.",
        ))
    }
}

pub(crate) fn get_hashset_words<'a>() -> Result<&'a HashSet<String>> {
    if let Some(words) = STOP_WORDS_HASHSET.get() {
        Ok(words)
    } else {
        Err(Error::msg(
            "stop words was not initialised at time of calling.",
        ))
    }
}
