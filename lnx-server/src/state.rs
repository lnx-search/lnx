use engine::{Engine, StorageBackend};

use crate::auth::AuthManager;

#[derive(Clone)]
pub struct State {
    pub log_search: bool,
    pub engine: Engine,
    pub auth: AuthManager,
    pub storage: StorageBackend,
}

impl State {
    pub fn new(
        engine: Engine,
        storage: StorageBackend,
        auth: AuthManager,
        log_search: bool,
    ) -> Self {
        Self {
            log_search,
            engine,
            storage,
            auth,
        }
    }
}
