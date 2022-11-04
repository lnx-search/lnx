use rkyv::{Archive, Serialize, Deserialize};
use hashbrown::HashSet;
use bytecheck::CheckBytes;

use crate::CorruptedData;

#[derive(Archive, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes))]
/// A set of stop words that should be ignored in the presence of a query.
pub struct StopWords {
    words: HashSet<Box<str>>,
}

impl StopWords {
    /// Deserializes the stop words from a given buffer.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CorruptedData> {
        rkyv::from_bytes(bytes).map_err(|_| CorruptedData)
    }

    /// Serializes the set of stop words to a buffer.
    pub fn to_bytes(&self) -> Result<Vec<u8>, CorruptedData> {
        rkyv::to_bytes::<_, 2048>(self)
            .map(|v| v.into_vec())
            .map_err(|_| CorruptedData)
    }

    #[inline]
    /// Merges one set of stop words with the current set.
    pub fn merge(&mut self, other: StopWords) {
        self.words.extend(other.words);
    }

    #[inline]
    /// Checks if the given word is a set stop word.
    pub fn is_stop_word(&self, term: &str) -> bool {
        self.words.contains(term)
    }
}