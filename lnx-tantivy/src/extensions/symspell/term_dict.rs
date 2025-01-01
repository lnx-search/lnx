use std::collections::{BTreeMap, HashMap};
use crate::extensions::symspell::ustr::UBytes;

/// A symspell delete term dictionary for performing lookups 
/// of terms to their ordinal value.
pub trait TermDictionary {
    /// Returns the `u32` ordinal of the term.
    fn get(&self, term: UBytes) -> Option<u32>;
    
    /// Creates a new term dictionary from the set of terms.
    fn from_terms(terms: BTreeMap<UBytes, u32>) -> Self;
}


/// A term dictionary backed by a FST.
pub struct FstDictionary {
    inner: fst::Map<Vec<u8>>,
}

impl TermDictionary for FstDictionary {
    #[inline]
    fn get(&self, term: UBytes) -> Option<u32> {
        self.inner
            .get(term)
            .map(|v| v as u32)
    }

    fn from_terms(terms: BTreeMap<UBytes, u32>) -> Self {        
        let mut builder = fst::MapBuilder::new(Vec::new()).unwrap();
        for (term, ord) in terms {
            builder.insert(term, ord as u64).unwrap();            
        }
        
        Self {
            inner: builder.into_map(),
        }
    }
}

/// A term dictionary backed by a hashmap
pub struct MapDictionary {
    inner: fnv::FnvHashMap<UBytes, u32>,
}

impl TermDictionary for MapDictionary {
    #[inline]
    fn get(&self, term: UBytes) -> Option<u32> {
        self.inner.get(&term).copied()
    }

    fn from_terms(terms: BTreeMap<UBytes, u32>) -> Self {
        let mut map = HashMap::with_capacity_and_hasher(terms.len(), fnv::FnvBuildHasher::default());
        
        for (term, ord) in terms {
            map.insert(term, ord).unwrap();
        }
        
        Self {
            inner: map,
        }        
    }
}