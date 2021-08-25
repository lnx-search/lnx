use once_cell::sync::OnceCell;
use std::fs;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

use symspell::{AsciiStringStrategy, SymSpell};

use flate2::write::GzDecoder;

static SYMSPELL: OnceCell<SymSpell<AsciiStringStrategy>> = OnceCell::new();
static ENABLED: AtomicBool = AtomicBool::new(false);

pub(crate) fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

pub(crate) fn enable_load_dictionaries() -> anyhow::Result<()> {
    ENABLED.store(true, Ordering::Relaxed);

    let mut symspell: SymSpell<AsciiStringStrategy> = SymSpell::default();

    let buffer: &[u8] = include_bytes!("../_dist/dictionary");
    let mut data = GzDecoder::new(vec![]);
    data.write_all(buffer)?;
    let data = data.finish()?;
    fs::write("./_temp.txt", &data)?;

    symspell.load_dictionary("./_temp.txt", 0, 1, " ");

    fs::remove_file("./_temp.txt")?;

    let _ = SYMSPELL.set(symspell);

    Ok(())
}

pub(crate) fn correct_sentence(query: &str) -> String {
    let sym = SYMSPELL.get().expect("get symspell");

    let mut suggestions = sym.lookup_compound(query, 2);

    if suggestions.len() == 0 {
        return query.into();
    }

    return suggestions.remove(0).term;
}
