use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arc_swap::ArcSwap;
use hashbrown::HashMap;
use symspell::{AsciiStringStrategy, Suggestion, SymSpell, Verbosity};

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
    
    pub(crate) fn terms(&self, term: &str, dist: i64) -> Vec<Suggestion> {
        self.sym.load().lookup(term, Verbosity::Top, dist)
    }

    /// Sets a custom symspell handler for the given index.
    ///
    /// This means when something is next set to be corrected for the index, the
    /// custom frequencies will be used instead of the default.
    #[instrument(name = "fast-fuzzy", skip_all, fields(unique_words = frequencies.len()))]
    pub(crate) fn adjust_index_frequencies(&self, frequencies: &HashMap<String, u32>) {
        info!("adjusting spell correction system to new frequency count, this may take a while...");

        let mut symspell: SymSpell<AsciiStringStrategy> = SymSpell::default();
        symspell.using_dictionary_frequencies(
            frequencies
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
