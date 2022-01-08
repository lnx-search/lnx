use std::cmp::Reverse;
use std::hash::{Hash, Hasher};

use anyhow::Result;
use bincode::Options;
use hashbrown::HashMap;
use tantivy::schema::Schema;
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};
use tantivy::DateTime;

use crate::storage::StorageBackend;

pub(crate) trait Validate {
    fn validate(&self) -> Result<()> {
        Ok(())
    }

    fn validate_with_schema(&self, _schema: &Schema) -> Result<()> {
        Ok(())
    }
}

pub(crate) trait AsScore {
    fn as_score(&self) -> Option<f32> {
        None
    }
}

impl AsScore for f64 {}
impl AsScore for u64 {}
impl AsScore for i64 {}
impl AsScore for DateTime {}
impl AsScore for Reverse<f64> {}
impl AsScore for Reverse<u64> {}
impl AsScore for Reverse<i64> {}
impl AsScore for Reverse<DateTime> {}

impl AsScore for f32 {
    fn as_score(&self) -> Option<f32> {
        Some(*self)
    }
}

pub(crate) trait FrequencyCounter {
    fn process_sentence(&mut self, sentence: &str);

    fn register(&mut self, k: String);

    fn clear_frequencies(&mut self);

    fn get_count(&self, k: &str) -> u32;

    fn counts(&self) -> &HashMap<String, u32>;
}

#[derive(Clone)]
pub(crate) struct FrequencySet {
    tokenizer: TextAnalyzer,
    inner: HashMap<String, u32>,
}

impl Default for FrequencySet {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl FrequencySet {
    pub(crate) fn new() -> Self {
        Self::with_capacity(0)
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let tokenizer = TextAnalyzer::from(SimpleTokenizer).filter(LowerCaser);

        Self {
            tokenizer,
            inner: HashMap::with_capacity(capacity),
        }
    }
}

impl FrequencyCounter for FrequencySet {
    fn process_sentence(&mut self, sentence: &str) {
        let mut tokens = self.tokenizer.token_stream(sentence);

        while let Some(token) = tokens.next() {
            self.register(token.text.clone())
        }
    }

    fn register(&mut self, k: String) {
        let v = {
            let exists = self.inner.get(&k);

            if let Some(v) = exists {
                *v + 1
            } else {
                1
            }
        };

        self.inner.insert(k, v);
    }

    fn clear_frequencies(&mut self) {
        self.inner = HashMap::new();
    }

    fn get_count(&self, k: &str) -> u32 {
        self.inner.get(k).copied().unwrap_or(0u32)
    }

    fn counts(&self) -> &HashMap<String, u32> {
        &self.inner
    }
}

pub(crate) struct PersistentFrequencySet {
    tree: sled::Tree,
    clean_set: FrequencySet,
    dirty_set: FrequencySet,
}

impl PersistentFrequencySet {
    pub(crate) fn new(conn: StorageBackend) -> Result<Self> {
        let tree = conn.conn().open_tree("global_frequencies")?;
        let mut inst = Self {
            tree,
            clean_set: FrequencySet::new(),
            dirty_set: FrequencySet::new(),
        };

        inst.load_frequencies_from_store()?;

        Ok(inst)
    }

    #[instrument(name = "frequency-counter", skip_all)]
    fn load_frequencies_from_store(&mut self) -> Result<()> {
        info!("loading frequencies from persistent backend.");

        let mut walker = self.tree.iter();
        while let Some(Ok((key, count))) = walker.next() {
            let count = bincode::options().with_big_endian().deserialize(&count)?;
            self.clean_set
                .inner
                .insert(String::from_utf8_lossy(key.as_ref()).to_string(), count);
        }

        info!(
            "loaded frequencies new item count: {}",
            self.clean_set.inner.len()
        );

        Ok(())
    }

    #[inline]
    #[instrument(name = "frequency-counter", skip_all)]
    pub(crate) fn remove_frequencies(&mut self, set: HashMap<String, u32>) {
        for (key, count) in set {
            self.dirty_set.inner.entry(key)
                .and_replace_entry_with(|k, v| {
                    let new_count = v.saturating_sub(count);

                    if new_count == 0 {
                        None
                    } else {
                        Some(new_count)
                    }
                })
        }
    }

    #[instrument(name = "frequency-counter", skip_all)]
    pub(crate) fn commit(&mut self) -> Result<()> {
        info!("storing frequencies in persistent backend.");

        let mut batch = sled::Batch::default();
        for (key, count) in self.dirty_set.inner.drain() {
            let count = bincode::options().with_big_endian().serialize(&count)?;
            batch.insert(key.as_bytes(), count);
        }

        self.tree.apply_batch(batch)?;

        self.clean_set = self.dirty_set.clone();

        Ok(())
    }

    #[instrument(name = "frequency-counter", skip_all)]
    pub(crate) fn rollback(&mut self) {
        info!("forgetting frequency changes in persistent backend.");

        self.dirty_set = self.clean_set.clone();
    }
}

impl FrequencyCounter for PersistentFrequencySet {
    fn process_sentence(&mut self, sentence: &str) {
        self.dirty_set.process_sentence(sentence)
    }

    fn register(&mut self, k: String) {
        self.dirty_set.register(k)
    }

    fn clear_frequencies(&mut self) {
        self.dirty_set.clear_frequencies();
    }

    fn get_count(&self, k: &str) -> u32 {
        self.clean_set.get_count(k)
    }

    fn counts(&self) -> &HashMap<String, u32> {
        self.clean_set.counts()
    }
}

pub fn cr32_hash(v: impl Hash) -> u64 {
    let mut hasher = crc32fast::Hasher::default();

    v.hash(&mut hasher);

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{OpenType, SledBackedDirectory};

    static TEST_FILE: &str = "./test";

    #[test]
    fn test_text_processing() {
        let sentence = "The quick brown fox, jumped over the quick brown dogg.";

        let mut freq_dict = FrequencySet::new();
        freq_dict.process_sentence(sentence);

        assert_eq!(freq_dict.get_count("the"), 2);
        assert_eq!(freq_dict.get_count("quick"), 2);
        assert_eq!(freq_dict.get_count("brown"), 2);
        assert_eq!(freq_dict.get_count("fox"), 1);
        assert_eq!(freq_dict.get_count("jumped"), 1);
        assert_eq!(freq_dict.get_count("over"), 1);
        assert_eq!(freq_dict.get_count("dogg"), 1);
    }

    #[test]
    fn test_storage_mem_backed_processing() -> Result<()> {
        let sentence = "The quick brown fox, jumped over the quick brown dogg.";

        let dir = SledBackedDirectory::new_with_root(&OpenType::TempFile)?;
        let storage = StorageBackend::using_conn(dir);
        let mut freq_dict = PersistentFrequencySet::new(storage)?;
        freq_dict.process_sentence(sentence);
        freq_dict.commit()?;

        assert_eq!(freq_dict.get_count("the"), 2);
        assert_eq!(freq_dict.get_count("quick"), 2);
        assert_eq!(freq_dict.get_count("brown"), 2);
        assert_eq!(freq_dict.get_count("fox"), 1);
        assert_eq!(freq_dict.get_count("jumped"), 1);
        assert_eq!(freq_dict.get_count("over"), 1);
        assert_eq!(freq_dict.get_count("dogg"), 1);

        Ok(())
    }

    #[test]
    fn test_storage_file_backed_processing() -> Result<()> {
        // In-case something went wrong
        let _ = std::fs::remove_file(TEST_FILE);

        let sentence = "The quick brown fox, jumped over the quick brown dogg.";

        let dir = SledBackedDirectory::new_with_root(&OpenType::TempFile)?;
        let storage = StorageBackend::using_conn(dir);

        {
            let mut freq_dict = PersistentFrequencySet::new(storage.clone())?;
            freq_dict.process_sentence(sentence);
            freq_dict.commit()?;

            assert_eq!(freq_dict.get_count("the"), 2);
            assert_eq!(freq_dict.get_count("quick"), 2);
            assert_eq!(freq_dict.get_count("brown"), 2);
            assert_eq!(freq_dict.get_count("fox"), 1);
            assert_eq!(freq_dict.get_count("jumped"), 1);
            assert_eq!(freq_dict.get_count("over"), 1);
            assert_eq!(freq_dict.get_count("dogg"), 1);
        }

        {
            let freq_dict = PersistentFrequencySet::new(storage)?;

            assert_eq!(freq_dict.get_count("the"), 2);
            assert_eq!(freq_dict.get_count("quick"), 2);
            assert_eq!(freq_dict.get_count("brown"), 2);
            assert_eq!(freq_dict.get_count("fox"), 1);
            assert_eq!(freq_dict.get_count("jumped"), 1);
            assert_eq!(freq_dict.get_count("over"), 1);
            assert_eq!(freq_dict.get_count("dogg"), 1);
        }

        let _ = std::fs::remove_file(TEST_FILE);

        Ok(())
    }
}
