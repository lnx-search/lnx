use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::sync::Arc;

use anyhow::{Error, Result};
use arc_swap::ArcSwap;
use bincode::Options;
use flate2::write::GzDecoder;
use once_cell::sync::OnceCell;

use crate::storage::StorageBackend;

static DEFAULT_WORDS: OnceCell<Vec<String>> = OnceCell::new();

/// Ensures the default words are initialised.
///
/// If the words are already set then this is a no op.
///
/// The words are extracted from the bundled compressed stop_words binary
/// and split on a line by line basis.
fn init_default_words() -> Result<()> {
    if DEFAULT_WORDS.get().is_some() {
        return Ok(());
    }

    let mut default_words = Vec::new();
    let buffer: &[u8] = include_bytes!("../_dist/stop_words");
    if !buffer.is_empty() {
        let mut data = GzDecoder::new(vec![]);
        data.write_all(buffer)?;
        let data = data.finish()?;

        let words = String::from_utf8(data)
            .map_err(|_| Error::msg("failed to parse stop words from linked data."))?;

        for word in words.to_lowercase().split('\n') {
            if let Some(word) = word.strip_suffix('\r') {
                default_words.push(word.to_string());
            } else {
                default_words.push(word.to_string());
            }
        }
    }

    let _ = DEFAULT_WORDS.set(default_words);

    Ok(())
}

/// The structure in charge of controlling the stop words.
#[derive(Clone)]
pub struct StopWordManager {
    index_stop_words: Arc<ArcSwap<Vec<String>>>,
}

impl StopWordManager {
    /// Creates a new `StopWordManager`.
    pub(crate) fn init() -> Result<Self> {
        // Ensure the default words are set.
        init_default_words()?;

        Ok(Self {
            index_stop_words: Arc::new(ArcSwap::from_pointee(vec![])),
        })
    }

    /// Checks if the given word is in the list of stop words.
    #[inline]
    pub fn is_stop_word(&self, word: &str) -> bool {
        self.index_stop_words.load().iter().any(|v| v == word)
    }

    /// Gets all the stop words for the given index.
    ///
    /// If the index has no specific custom stop words
    /// the default list of stop words are returned.
    pub fn get_stop_words(&self) -> Vec<String> {
        let words = self.index_stop_words.load();
        if words.len() == 0 {
            DEFAULT_WORDS.get().expect("get defaults").to_vec()
        } else {
            words.as_ref().to_vec()
        }
    }

    /// Adds a list of stop words to the given index's sector.
    ///
    /// If the index already has specific words added, the words are appended.
    /// If the index doesnt already have specific stop words, the words are added
    /// to a new index slot.
    pub fn add_stop_words(&self, mut words: Vec<String>) {
        words = words.drain(..).map(|v| v.to_lowercase()).collect();

        {
            let guard = self.index_stop_words.load();
            words.extend_from_slice(guard.as_ref());
        }

        self.index_stop_words.store(Arc::new(words))
    }

    /// Removes a set of stop words from the index's specific set if it exists.
    pub fn remove_stop_words(&self, mut words: Vec<String>) {
        words = words.into_iter().map(|v| v.to_lowercase()).collect();

        let new_words: Vec<String> = {
            let guard = self.index_stop_words.load();
            guard
                .as_ref()
                .iter()
                .filter(|item| !words.contains(item))
                .map(|v| v.to_string())
                .collect()
        };

        self.index_stop_words.store(Arc::new(new_words));
    }

    /// Clears all custom stop words from all indexes.
    pub fn clear_stop_words(&self) {
        self.index_stop_words.store(Arc::new(vec![]))
    }
}

impl Debug for StopWordManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "StopWordManager(words={})",
            self.index_stop_words.load().len()
        ))
    }
}

pub(crate) struct PersistentStopWordManager {
    conn: StorageBackend,
    manager: StopWordManager,
}

impl PersistentStopWordManager {
    const KEYSPACE: &'static str = "stop_words";

    /// Creates a new `PersistentStopWordManager`.
    #[instrument(name = "stop-words", skip_all)]
    pub(crate) fn new(conn: StorageBackend, manager: StopWordManager) -> Result<Self> {
        debug!("loading stop words from persistent store");

        let raw_structure = conn.load_structure(Self::KEYSPACE)?;
        let words: Vec<String> = if let Some(buff) = raw_structure {
            bincode::options().with_big_endian().deserialize(&buff)?
        } else {
            vec![]
        };

        let count = words.len();
        manager.add_stop_words(words);
        debug!("{} new words successfully loaded", count);

        Ok(Self { conn, manager })
    }

    /// Adds a list of stop words to the given index's sector.
    ///
    /// If the index already has specific words added, the words are appended.
    /// If the index doesnt already have specific stop words, the words are added
    /// to a new index slot.
    pub fn add_stop_words(&self, words: Vec<String>) {
        self.manager.add_stop_words(words)
    }

    /// Removes a set of stop words from the index's specific set if it exists.
    pub fn remove_stop_words(&self, words: Vec<String>) {
        self.manager.remove_stop_words(words)
    }

    /// Clears all custom stop words from all indexes.
    pub fn clear_stop_words(&self) {
        self.manager.clear_stop_words()
    }

    /// Saves any changes to the stop words to the persistent disk.
    #[instrument(name = "stop-words", skip_all)]
    pub fn commit(&self) -> Result<()> {
        let words = self.manager.get_stop_words();
        self.conn.store_structure(Self::KEYSPACE, &words)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_stop_word_addition() -> Result<()> {
        let manager = StopWordManager::init()?;

        manager.add_stop_words(vec!["for".into(), "oF".into(), "THe".into()]);

        assert!(manager.is_stop_word(&String::from("for")));
        assert!(manager.is_stop_word(&String::from("of")));
        assert!(manager.is_stop_word(&String::from("the")));
        assert!(!manager.is_stop_word(&String::from("Ahhhhh")));

        Ok(())
    }

    #[test]
    fn test_basic_stop_word_removal() -> Result<()> {
        let manager = StopWordManager::init()?;

        manager.add_stop_words(vec!["for".into(), "oF".into(), "THe".into()]);

        manager.remove_stop_words(vec!["of".into()]);

        assert!(manager.is_stop_word(&String::from("for")));
        assert!(manager.is_stop_word(&String::from("the")));

        assert!(!manager.is_stop_word(&String::from("of")));
        assert!(!manager.is_stop_word(&String::from("Ahhhhh")));

        Ok(())
    }

    #[test]
    fn test_basic_stop_word_clear() -> Result<()> {
        let manager = StopWordManager::init()?;

        manager.add_stop_words(vec!["for".into(), "oF".into(), "THe".into()]);

        manager.remove_stop_words(vec!["of".into()]);

        assert!(manager.is_stop_word(&String::from("for")));
        assert!(manager.is_stop_word(&String::from("the")));

        assert!(!manager.is_stop_word(&String::from("of")));
        assert!(!manager.is_stop_word(&String::from("Ahhhhh")));

        manager.clear_stop_words();

        assert!(!manager.is_stop_word(&String::from("for")));
        assert!(!manager.is_stop_word(&String::from("the")));
        assert!(!manager.is_stop_word(&String::from("of")));
        assert!(!manager.is_stop_word(&String::from("Ahhhhh")));

        Ok(())
    }

    #[test]
    fn test_concurrent_stop_word_changes() -> Result<()> {
        let manager = StopWordManager::init()?;

        let duplicate = manager.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        let thread = std::thread::spawn(move || {
            let _ = rx.recv();

            if !duplicate.is_stop_word(&String::from("for")) {
                return Err(Error::msg("invalid thread test"));
            }

            if duplicate.is_stop_word(&String::from("Ahhhhh")) {
                return Err(Error::msg("invalid thread test"));
            }

            Ok(())
        });

        manager.add_stop_words(vec!["for".into(), "oF".into(), "THe".into()]);

        manager.remove_stop_words(vec!["of".into()]);

        let _ = tx.send(());

        assert!(manager.is_stop_word(&String::from("for")));
        assert!(manager.is_stop_word(&String::from("the")));

        assert!(!manager.is_stop_word(&String::from("of")));
        assert!(!manager.is_stop_word(&String::from("Ahhhhh")));

        assert!(thread.join().is_ok());

        Ok(())
    }
}
