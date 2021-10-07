use std::cmp::Reverse;

use anyhow::Result;
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

    fn get_count(&self, k: &str) -> u32 {
        self.inner.get(k).map(|v| *v).unwrap_or(0u32)
    }

    fn counts(&self) -> &HashMap<String, u32> {
        &self.inner
    }
}

#[derive(Debug)]
struct FrequencyRow {
    word: String,
    count: u32,
}

pub(crate) struct PersistentFrequencySet {
    conn: StorageBackend,
    set: FrequencySet,
}

impl PersistentFrequencySet {
    pub(crate) fn new(conn: StorageBackend) -> Result<Self> {
        let mut inst = Self {
            conn,
            set: FrequencySet::new(),
        };

        inst.ensure_table()?;
        inst.load_frequencies_from_store()?;

        Ok(inst)
    }

    fn ensure_table(&mut self) -> Result<()> {
        debug!("[ FREQUENCY-COUNTER ] ensuring table existence.");
        self.conn
            .create_table("frequencies", vec![("word", "TEXT"), ("count", "INTEGER")])?;

        Ok(())
    }

    fn load_frequencies_from_store(&mut self) -> Result<()> {
        info!("[ FREQUENCY-COUNTER ] loading frequencies from persistent backend.");
        let mut results = self.conn.prepare("SELECT word, count FROM frequencies")?;

        let person_iter = results.query_map([], |row| {
            Ok(FrequencyRow {
                word: row.get(0)?,
                count: row.get(1)?,
            })
        })?;

        for row in person_iter {
            let row = row.unwrap();

            self.set.inner.insert(row.word, row.count);
        }

        info!(
            "[ FREQUENCY-COUNTER ] loaded frequencies new item count: {}",
            self.set.inner.len()
        );

        Ok(())
    }

    pub(crate) fn commit(&self) -> Result<()> {
        info!("[ FREQUENCY-COUNTER ] storing frequencies in persistent backend.");

        let mut stmt = self
            .conn
            .prepare_cached("INSERT INTO frequencies (word, count) VALUES(?, ?)")?;

        for (word, count) in self.set.counts() {
            stmt.execute(rusqlite::params![word, count])?;
        }

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

    fn get_count(&self, k: &str) -> u32 {
        self.set.get_count(k)
    }

    fn counts(&self) -> &HashMap<String, u32> {
        self.set.counts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_FILE: &str = "./test.db";

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
        let sentence = "The quick brown fox, jumped over the quick brown dogg.";

        let storage = StorageBackend::connect(Some(TEST_FILE.into()))?;

        {
            let mut freq_dict = PersistentFrequencySet::new(storage.duplicate_conn()?)?;
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

        std::fs::remove_file(TEST_FILE)?;

        Ok(())
    }
}
