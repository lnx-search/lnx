use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arc_swap::ArcSwap;
use symspell::{AsciiStringStrategy, SymSpell};

use crate::helpers::FrequencyCounter;

pub(crate) type SymSpellCorrectionManager = Arc<SymSpellManager>;

/// The manager around the sym spell fuzzy searching system.
pub(crate) struct SymSpellManager {
    sym: Arc<ArcSwap<SymSpell<AsciiStringStrategy>>>,
}

impl SymSpellManager {
    pub(crate) fn new() -> Self {
        let sym = SymSpell::default();
        let sym = Arc::new(ArcSwap::from_pointee(sym));
        Self { sym }
    }

    /// Corrects the sentence with an edit distance of 1.
    ///
    /// If the index does not have a set of frequencies this returns the original string.
    pub(crate) fn correct(&self, sentence: &str) -> String {
        self.sym.load().lookup_compound(sentence, 2)
    }

    /// Sets a custom symspell handler for the given index.
    ///
    /// This means when something is next set to be corrected for the index, the
    /// custom frequencies will be used instead of the default.
    pub(crate) fn adjust_index_frequencies(&self, frequencies: &impl FrequencyCounter) {
        let mut symspell: SymSpell<AsciiStringStrategy> = SymSpell::default();
        symspell.using_dictionary_frequencies(
            frequencies
                .counts()
                .into_iter()
                .map(|(k, v)| (k.clone(), *v as i64))
                .collect(),
        );

        self.sym.store(Arc::from(symspell))
    }
}

impl Debug for SymSpellManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("SymSpellManager")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::FrequencySet;

    #[test]
    fn test_text_correction() {
        let sentence = "The quick brown fox, jumped over the quick brown dogg.";

        let mut freq_dict = FrequencySet::new();
        freq_dict.process_sentence(sentence);

        let manager = SymSpellManager::new();
        manager.adjust_index_frequencies(&freq_dict);

        let test_sentence = "teh quick broown fox jumpedd ove the quic bruwn dog";
        let corrected_sentence = manager.correct(test_sentence);

        assert_eq!(
            corrected_sentence,
            "the quick brown fox jumped over the quick brown dogg"
        );
    }
}
