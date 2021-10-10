use std::collections::HashMap;

use bytes::Bytes;
use engine::{Engine, StorageBackend};
use serde::Serialize;

use crate::auth::AuthManager;

#[derive(Clone)]
pub struct State {
    pub engine: Engine,
    pub auth: AuthManager,
    pub storage: StorageBackend,
}

impl State {
    pub fn new(
        engine: Engine,
        storage: StorageBackend,
        auth: AuthManager,
    ) -> Self {
        Self { engine, storage, auth }
    }
}
