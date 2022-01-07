use std::cmp::Reverse;
use std::hash::{Hash, Hasher};

use anyhow::Result;
use bincode::deserialize;
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
    conn: StorageBackend,
    set: FrequencySet,
}

impl PersistentFrequencySet {
    const KEYSPACE: &'static str = "frequencies";

    pub(crate) fn new(conn: StorageBackend) -> Result<Self> {
        let mut inst = Self {
            conn,
            set: FrequencySet::new(),
        };

        inst.load_frequencies_from_store()?;

        Ok(inst)
    }

    fn load_frequencies_from_store(&mut self) -> Result<()> {
        info!("[ FREQUENCY-COUNTER ] loading frequencies from persistent backend.");

        let raw_structure = self.conn.load_structure(Self::KEYSPACE)?;
        let frequencies: HashMap<String, u32> = if let Some(buff) = raw_structure {
            deserialize(&buff)?
        } else {
            HashMap::new()
        };

        for (word, count) in frequencies {
            self.set.inner.insert(word, count);
        }

        info!(
            "[ FREQUENCY-COUNTER ] loaded frequencies new item count: {}",
            self.set.inner.len()
        );

        Ok(())
    }

    pub(crate) fn commit(&self) -> Result<()> {
        info!("[ FREQUENCY-COUNTER ] storing frequencies in persistent backend.");

        let frequencies = self.set.counts();
        self.conn.store_structure(Self::KEYSPACE, frequencies)?;
        Ok(())
    }
}

impl FrequencyCounter for PersistentFrequencySet {
    fn process_sentence(&mut self, sentence: &str) {
        self.set.process_sentence(sentence)
    }

    fn register(&mut self, k: String) {
        self.set.register(k)
    }

    fn clear_frequencies(&mut self) {
        self.set.clear_frequencies();
    }

    fn get_count(&self, k: &str) -> u32 {
        self.set.get_count(k)
    }

    fn counts(&self) -> &HashMap<String, u32> {
        self.set.counts()
    }
}

pub fn hash(v: impl Hash) -> u64 {
    let mut hasher = ahash::AHasher::default();

    v.hash(&mut hasher);

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let storage = StorageBackend::connect(None)?;
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

        let storage = StorageBackend::connect(None)?;

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
