use std::ops::Deref;
use std::sync::Arc;
use compose::{SymSpell, Verbosity};
use rkyv::{Archive, Serialize, Deserialize};
use bytecheck::CheckBytes;

use crate::CorruptedData;

#[derive(Clone)]
/// The fast fuzzy correction handler.
///
/// This can potentially be a very heavy operation to load
/// and unload from memory, hence this is ran in a background
/// thread when loading.
pub struct FastFuzzyCorrector(Arc<FastFuzzyCorrectorInner>);

impl Deref for FastFuzzyCorrector {
    type Target = FastFuzzyCorrectorInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FastFuzzyCorrector {
    /// Deserializes the fuzzy corrector from a given buffer.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CorruptedData> {
        let inner: FastFuzzyCorrectorInner = rkyv::from_bytes(bytes)
            .map_err(|_| CorruptedData)?;

        Ok(Self(Arc::new(inner)))
    }

    /// Serializes the fuzzy corrector to a buffer.
    pub fn to_bytes(&self) -> Result<Vec<u8>, CorruptedData> {
        rkyv::to_bytes::<_, 2048>(self.0.as_ref())
            .map(|v| v.into_vec())
            .map_err(|_| CorruptedData)
    }

    /// Builds a new corrector from a set of term frequencies.
    pub fn using_dictionary_frequencies(terms: impl Iterator<Item = (String, i64)>) {
        let mut symspell = SymSpell::default();
        symspell.using_dictionary_frequencies(terms, false)
    }
}

#[derive(Archive, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes))]
pub struct FastFuzzyCorrectorInner {
    symspell: SymSpell,
}

impl FastFuzzyCorrectorInner {
    #[inline]
    /// Checks for any corrections for the given term.
    pub fn correct(&self, term: &str, edit_distance: u8) -> impl Iterator<Item = (String, u8)> {
        self.symspell
            .lookup(term, Verbosity::Closest, edit_distance as i64)
            .into_iter()
            .map(|v| (v.term, v.distance as u8))
    }
}