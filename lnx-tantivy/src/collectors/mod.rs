use std::collections::HashSet;

use crate::utils::NoOpRandomState;

mod distinct;
mod ext;
mod top_docs;

pub use self::distinct::Distinct;
pub use self::top_docs::{CustomSortTopDocs, SortableKey, TopDocs};

pub type SeenHashIds = HashSet<u64, NoOpRandomState>;
