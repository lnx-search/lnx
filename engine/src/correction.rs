use once_cell::sync::OnceCell;
use std::fs;
use symspell::{SymSpell, SymSpellBuilder, AsciiStringStrategy};
use anyhow::Error;
use std::sync::atomic::{AtomicI64, Ordering};


static DATA_DIR: &str = "datasets/dictionaries";
static SYMSPELL: OnceCell<SymSpell<AsciiStringStrategy>> = OnceCell::new();
static MAX_EDIT_DISTANCE: AtomicI64 = AtomicI64::new(2);

pub(crate) fn load_dictionaries(max_edit_distance: i64) -> anyhow::Result<()> {
    let mut symspell: SymSpell<AsciiStringStrategy> = SymSpellBuilder::default()
        .max_dictionary_edit_distance(max_edit_distance)
        .build()
        .unwrap();

    MAX_EDIT_DISTANCE.store(max_edit_distance, Ordering::Relaxed);

    for entry in fs::read_dir(DATA_DIR)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            return Err(Error::msg("directories are not expected"))
        }

        symspell.load_dictionary(
            path.as_os_str().to_str().unwrap(),
            0,
            1,
            " ",
        );
    }

    let _ = SYMSPELL.set(symspell);

    Ok(())
}

pub(crate) fn get_suggested_sentence(query: &str) -> anyhow::Result<String> {
    let sym = SYMSPELL.get()
        .ok_or_else(|| Error::msg("symspell is not initialised"))?;

    let med = MAX_EDIT_DISTANCE.load(Ordering::Relaxed);
    let mut results = sym.lookup_compound(query, med);
    if results.len() == 0 {
        return Ok(query.into())
    }

    Ok(results.remove(0).term)
}