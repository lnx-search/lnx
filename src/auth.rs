use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use parking_lot::Mutex;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};

pub mod permissions {
    pub const MODIFY_ENGINE: usize = 1 << 0;
    pub const SEARCH_INDEX: usize = 1 << 1;
    pub const MODIFY_DOCUMENTS: usize = 1 << 2;
    pub const MODIFY_STOP_WORDS: usize = 1 << 3;
    pub const MODIFY_AUTH: usize = 1 << 4;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UserData {
    token: String,
    allowed_indexes: Option<Vec<String>>,
    permissions: usize,
    created: DateTime<Utc>,
    user: Option<String>,
    description: Option<String>,
}

impl UserData {
    pub fn has_permissions(&self, flags: usize) -> bool {
        self.permissions & flags != 0
    }
}

#[derive(Clone)]
pub struct AuthManager {
    keys: Arc<ArcSwap<HashMap<String, Arc<UserData>>>>
}

impl AuthManager {
    pub fn new() -> Self {
        Self {
            keys: Arc::new(ArcSwap::from_pointee(HashMap::new()))
        }
    }

    pub fn create_key(
        &self,
        permissions: usize,
        user: Option<String>,
        description: Option<String>,
        allowed_indexes: Option<Vec<String>>,
    ) -> Arc<UserData> {
        let created = Utc::now();
        let token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(64)
            .map(char::from)
            .collect();

        let data = Arc::new(UserData {
            token,
            allowed_indexes,
            permissions,
            created,
            user,
            description,
        });

        let mut new;
        {
            let existing = self.keys.load();
            new = existing.as_ref().clone();
            new.insert(data.token.clone(), data.clone());
        }
        self.keys.store(Arc::new(new));

        data
    }


    pub fn get_all_tokens(&self) -> Vec<Arc<UserData>> {
        self.keys
            .load()
            .iter()
            .map(|(_, v)| v.clone())
            .collect()
    }

    pub fn get_token_data(&self, token: &str) -> Option<Arc<UserData>> {
        let guard = self.keys.load();
        guard.get(token).map(|v| v.clone())
    }

    pub fn revoke_token(&self, token: &str) {
        let mut new;
        {
            let existing = self.keys.load();
            new = existing.as_ref().clone();
            new.remove(token);
        }
        self.keys.store(Arc::new(new));
    }

    pub fn revoke_all_tokens(&self) {
        self.keys.store(Arc::new(HashMap::new()));
    }
}
