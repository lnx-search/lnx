use std::collections::HashSet;

use crate::utils::NoOpRandomState;

mod ext;
mod top_docs;

pub type SeenHashIds = HashSet<u64, NoOpRandomState>;
