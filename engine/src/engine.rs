use std::sync::Arc;
use hashbrown::HashMap;
use parking_lot::RwLock;
use evmap::{WriteHandle, ReadHandle};

use tokio::sync;

use crate::structures::LoadedIndex;


#[derive(Clone)]
pub struct SearchEngine {
    max_concurrency: u32,
    indexes: Arc<RwLock<HashMap<String, LoadedIndex>>>,
}

impl SearchEngine {
    pub fn create(max_concurrency: u32) -> Self {
        Self {
            max_concurrency,
            indexes: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    pub fn add_index(&mut self, name: impl Into<String>, index: LoadedIndex) {

    }
}