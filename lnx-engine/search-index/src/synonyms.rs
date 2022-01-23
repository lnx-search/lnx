use std::iter::FromIterator;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arc_swap::ArcSwap;
use bincode::Options;
use hashbrown::{HashMap, HashSet};
use crate::StorageBackend;

#[derive(Debug, Clone)]
pub struct SynonymsManager {
    synonyms: Arc<ArcSwap<HashMap<String, Box<[String]>>>>,
}

impl SynonymsManager {
    pub fn init() -> Self {
        Self {
            synonyms: Arc::new(ArcSwap::from_pointee(HashMap::default())),
        }
    }

    #[inline]
    pub fn get_synonyms(&self, word: &str) -> Option<Box<[String]>> {
        self.synonyms.load().get(word).cloned()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.synonyms.load().as_ref().len()
    }

    /// Removes a set of words and their synonyms.
    pub fn remove_many_word_synonyms(&self, words: &[String]) -> HashMap<String, Box<[String]>> {
        let guard = self.synonyms.load();
        let mut current_mapping = guard.as_ref().clone();

        for word in words {
            current_mapping.remove(word);
        }

        current_mapping
    }

    /// Parses a given synonym relation string and modifies the inner dictionary to reflect
    /// the changes.
    ///
    /// Parses foo,bar,baz:foo,bar,baz into forming a synonym for each item minus itself.
    pub fn parse_many_synonyms(&self, relations: &[String]) -> Result<HashMap<String, Box<[String]>>> {
        let guard = self.synonyms.load();
        let current_mapping = guard.as_ref();
        let mut new_mapping: HashMap<String, HashSet<String>> = HashMap::with_capacity(current_mapping.len());
        for (key, relations) in current_mapping {
            new_mapping.insert(
                key.to_string(),
                HashSet::from_iter(relations.iter().map(String::to_string)),
            );
        }

        for relation in relations {
            let (left, right) = relation.split_once(":")
                .ok_or_else(|| anyhow!(
                    "invalid synonym relation defined. synonyms must follow the \
                    `word(s):related_item(s)` format, Got {:?}",
                    relation,
                ))?;

            let left = left.to_lowercase();
            let right = right.to_lowercase();

            let relation_stream: Vec<&str> = right.split(',').collect();

            if relation_stream.is_empty() {
                return Err(anyhow!("at least one relation must be defined, got none"));
            }

            for word in left.split(',') {
                new_mapping.entry(word.trim().to_string())
                    .and_modify(|v| {
                        // There is probably a faster way of doing this. However,
                        // This is by far the simplest strategy and
                        for relation in relation_stream.iter() {
                            // Skip ourself
                            if *relation == word {
                                continue;
                            }

                            v.insert(relation.to_string());
                        }
                    })
                    .or_insert_with(|| {
                        let mut new_set = HashSet::new();

                        // There is probably a faster way of doing this. However,
                        // This is by far the simplest strategy and
                        for relation in relation_stream.iter() {
                            // Skip ourself
                            if *relation == word {
                                continue;
                            }

                            new_set.insert(relation.to_string());
                        }

                        new_set
                    });
            }
        }

        Ok(HashMap::from_iter(
            new_mapping
                .into_iter()
                .map(|(k, v)| {
                    let v = Vec::from_iter(v.into_iter())
                        .into_boxed_slice();

                    (k, v)
                })
        ))
    }
}


pub(crate) struct PersistentSynonymsManager {
    conn: StorageBackend,
    manager: SynonymsManager,
}

impl PersistentSynonymsManager {
    const KEYSPACE: &'static str = "synonyms";

    /// Creates a new `PersistentSynonymsManager`.
    #[instrument(name = "synonyms", skip_all)]
    pub(crate) fn new(conn: StorageBackend, manager: SynonymsManager) -> Result<Self> {
        debug!("loading synonyms from persistent store");

        let raw_structure = conn.load_structure(Self::KEYSPACE)?;
        let words: HashMap<String, Box<[String]>> = if let Some(buff) = raw_structure {
            bincode::options().with_big_endian().deserialize(&buff)?
        } else {
            HashMap::default()
        };

        let count = words.len();
        manager.synonyms.store(Arc::new(words));
        debug!("{} new synonyms relations successfully loaded", count);

        Ok(Self { conn, manager })
    }

    /// Parses a given synonym relation string and modifies the inner dictionary to reflect
    /// the changes.
    ///
    /// Parses foo,bar,baz:foo,bar,baz into forming a synonym for each item minus itself.
    #[instrument(name = "synonyms", skip(self))]
    pub fn parse_many_synonyms(&self, relation: &[String]) -> Result<()> {
        let old_len = self.manager.len();

        let new_synonyms = self.manager.parse_many_synonyms(relation)?;
        self.conn.store_structure(Self::KEYSPACE, &new_synonyms)?;

        let new_len = new_synonyms.len();
        self.manager.synonyms.store(Arc::new(new_synonyms));

        info!("{} new synonyms added to the dictionary", new_len - old_len);

        Ok(())
    }

    /// Remove a given word's synonyms
    #[instrument(name = "synonyms", skip(self))]
    pub fn remove_many_word_synonyms(&self, words: &[String]) -> Result<()> {
        let old_len = self.manager.len();

        let new_synonyms = self.manager.remove_many_word_synonyms(words);
        self.conn.store_structure(Self::KEYSPACE, &new_synonyms)?;

        let new_len = new_synonyms.len();
        self.manager.synonyms.store(Arc::new(new_synonyms));

        info!("removed {} synonyms from the dictionary", old_len - new_len);

        Ok(())
    }

    /// Remove all synonyms
    #[instrument(name = "synonyms", skip(self))]
    pub fn clear_all(&self) -> Result<()> {
        let new = HashMap::default();
        self.conn.store_structure(Self::KEYSPACE, &new)?;

        self.manager.synonyms.store(Arc::new(new));

        info!("removed all synonyms from the dictionary");

        Ok(())
    }
}