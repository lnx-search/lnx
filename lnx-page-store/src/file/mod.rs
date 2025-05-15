mod allocated_table;
mod log;

/// The total number of pages each page file holds onto.
pub const PAGES_PER_FILE: usize = 1_000_000;
