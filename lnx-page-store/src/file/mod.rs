mod allocation_table;
mod integrity;
mod log;
#[cfg(all(test, not(feature = "test-miri")))]
mod tests;

/// The total number of pages each page file holds onto.
pub const PAGES_PER_FILE: usize = 1_000_000;
