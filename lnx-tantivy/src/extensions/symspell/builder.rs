use std::borrow::Cow;
use std::collections::VecDeque;
use std::{cmp, io};
use crate::extensions::symspell::suggestion::Suggestion;
use crate::extensions::symspell::term_dict::TermDictionary;
use crate::extensions::symspell::ustr::UBytes;
//use super::utils::CowBytes;

const PREFIX_LENGTH: usize = 16;

#[derive(Debug, Copy, Clone, bon::Builder)]
/// The configuration options for building the symspell index.
pub struct SymspellIndexOptions {
    #[builder(default = 9)]
    edit_distance_2_word_len_threshold: usize,
    #[builder(default = 5)]
    edit_distance_1_word_len_threshold: usize,
}

impl SymspellIndexOptions {
    #[inline]
    pub fn edit_distance_for_term(&self, term: &[u8]) -> usize {
        if term.len() >= self.edit_distance_2_word_len_threshold {
            2
        } else if term.len() >= self.edit_distance_1_word_len_threshold {
            1
        } else {
            0
        }
    }
}

pub struct SymspellIndexBuilder {
    term_dict: tantivy::termdict::TermDictionary
}

impl SymspellIndexBuilder {
    fn build_deletes(&mut self) -> io::Result<()> {
        let mut streamer = self.term_dict.range().into_stream()?;
        while let Some((key, value)) = streamer.next() {
            
        }
        
        Ok(())
    }
    
}

#[derive(Eq, PartialEq, Debug)]
pub enum Verbosity {
    Top,
    Closest,
    All,
}

pub struct SymSpellIndex {
    /// A mapping of the edited terms to the parent terms that they could
    /// have come from.
    deletes: fnv::FnvHashMap<UBytes, Box<[Box<[u8]>]>>,
    options: SymspellIndexOptions,
}

impl SymSpellIndex
{
    pub fn lookup(&self, term_bytes: &[u8], verbosity: Verbosity) -> Vec<Suggestion> {
        let mut suggestions = Vec::with_capacity(1);
        
        let max_edit_distance = self.options.edit_distance_for_term(term_bytes);
        
        let term = UBytes::from_slice_prefix(term_bytes);
        
        if self.deletes.contains_key(&term) {
            // We can't be sure the exact match exists or not so opt on the side
            // of caution.
            if term_bytes.len() > PREFIX_LENGTH {
                suggestions.push(Suggestion::new(term_bytes, 0, 0));
            }
            suggestions.push(Suggestion::new(term_bytes, 0, 0));
            
            if verbosity != Verbosity::All {
                return suggestions;
            }
        }        
        
        if max_edit_distance == 0 {
            return suggestions;
        }
        
        let mut candidates = VecDeque::new();
        candidates.push_back(term);
        
        while let Some(candidate) = candidates.pop_front() {
            let length_diff = term.len() - candidate.len();
            
            if length_diff > max_edit_distance {
                if verbosity == Verbosity::All {
                    continue;
                } else {
                    break;
                }
            }
            
            let Some(candidate_parents) = self.deletes.get(&candidate) else { continue };
            
            for delete_parent in candidate_parents {
                // TODO: maybe this isn't needed?
                // if delete_parent.as_ref() == term_bytes {
                //     continue;
                // }

                
                
            }            
        }       
        
        
        suggestions
    }
}

fn edits_prefix(
    options: &SymspellIndexOptions,
    term: &[u8]
) -> fnv::FnvHashSet<UBytes> {
    let mut hash_set = fnv::FnvHashSet::default();

    let max_edit_distance = options.edit_distance_for_term(term);

    if max_edit_distance == 0 {
        return hash_set;
    }
    
    let term = UBytes::from_slice_prefix(term);    
    hash_set.insert(term);
    edits(max_edit_distance, term, 0, &mut hash_set);

    hash_set
}

fn edits(
    max_edit_distance: usize,
    term: UBytes,
    edit_distance: usize,
    delete_words: &mut fnv::FnvHashSet<UBytes>,
) {
    let edit_distance = edit_distance + 1;

    for i in 0..term.len() {
        let delete = term.remove(i);    
        let did_insert = delete_words.insert(delete);

        if did_insert && edit_distance < max_edit_distance {
            edits(
                max_edit_distance,
                delete,
                edit_distance,
                delete_words,
            );
        }
    }
}


fn diff(len1: usize, len2: usize) -> usize {
    if len1 > len2 {
        len1 - len2
    } else {
        len2 - len1
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_edits() {
        let mut words = fnv::FnvHashSet::default();
        let term = UBytes::from_slice_prefix(b"hello");
        edits(
            2,
            term,
            0,
            &mut words,
        );
        
        let mut words = words.into_iter().collect::<Vec<_>>();
        words.sort();
        assert_eq!(
            words,
            &[
                UBytes::from_slice_prefix(b"ell"),
                UBytes::from_slice_prefix(b"ello"),
                UBytes::from_slice_prefix(b"elo"),
                UBytes::from_slice_prefix(b"hel"),
                UBytes::from_slice_prefix(b"hell"),
                UBytes::from_slice_prefix(b"helo"),
                UBytes::from_slice_prefix(b"heo"),
                UBytes::from_slice_prefix(b"hll"),
                UBytes::from_slice_prefix(b"hllo"),
                UBytes::from_slice_prefix(b"hlo"),
                UBytes::from_slice_prefix(b"llo"),
            ]
        );    
    }

    #[test]
    fn test_edits_long_term() {
        // This just checks the system doesn't panic
        let mut words = fnv::FnvHashSet::default();
        let term = UBytes::from_slice_prefix(b"insomniacilification");
        dbg!(term.len());
        edits(
            3,
            term,
            0,
            &mut words,
        );
    }
    
    #[test]
    fn test_edits_prefix() {
        let options = SymspellIndexOptions::builder()
            .build();
        
        let words = edits_prefix(
            &options,
            b"hello",
        );

        let mut words = words.into_iter().collect::<Vec<_>>();
        words.sort();
        assert_eq!(
            words,
            &[
                UBytes::from_slice_prefix(b"ello"),
                UBytes::from_slice_prefix(b"hell"),
                UBytes::from_slice_prefix(b"hello"),
                UBytes::from_slice_prefix(b"helo"),
                UBytes::from_slice_prefix(b"hllo"),
            ]
        );
    }
}