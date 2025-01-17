use std::collections::HashSet;

use crate::utils::NoOpRandomState;

mod ext;
mod top_docs;
mod distinct;

pub use self::top_docs::{TopDocs, CustomSortTopDocs};
pub use self::distinct::Distinct;

pub type SeenHashIds = HashSet<u64, NoOpRandomState>;
