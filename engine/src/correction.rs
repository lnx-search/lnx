use once_cell::sync::OnceCell;
use std::fs;
use symspell::{SymSpell, AsciiStringStrategy};
use anyhow::Error;


static DATA_DIR: &str = "datasets/dictionaries";
static SYMSPELL: OnceCell<SymSpell<AsciiStringStrategy>> = OnceCell::new();


pub(crate) fn load_dictionaries() -> anyhow::Result<()> {
    let mut symspell: SymSpell<AsciiStringStrategy> = SymSpell::default();

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

    let mut results = sym.lookup_compound(query, 2);
    if results.len() == 0 {
        return Ok(query.into())
    }

    Ok(results.remove(0).term)
}