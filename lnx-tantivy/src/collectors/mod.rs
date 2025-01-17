use std::collections::HashSet;

use crate::utils::NoOpRandomState;

mod ext;
mod top_docs;

pub use self::top_docs::TopDocs;

pub type SeenHashIds = HashSet<u64, NoOpRandomState>;
