use engine::Engine;

use crate::auth::AuthManager;

#[derive(Clone)]
pub struct State {
    pub log_search: bool,
    pub engine: Engine,
    pub auth: AuthManager,
    pub storage: sled::Db,
}

impl State {
    pub fn new(
        engine: Engine,
        storage: sled::Db,
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
