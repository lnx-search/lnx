use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use engine::StorageBackend;
use hashbrown::HashMap;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};

pub mod permissions {
    pub const MODIFY_ENGINE: usize = 1 << 0;
    pub const SEARCH_INDEX: usize = 1 << 1;
    pub const MODIFY_DOCUMENTS: usize = 1 << 2;
    pub const MODIFY_STOP_WORDS: usize = 1 << 3;
    pub const MODIFY_AUTH: usize = 1 << 4;
    pub const SUPER_USER: usize = MODIFY_ENGINE
        | SEARCH_INDEX
        | MODIFY_DOCUMENTS
        | MODIFY_STOP_WORDS
        | MODIFY_AUTH;
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
    auth_enabled: bool,
    keys: Arc<ArcSwap<HashMap<String, Arc<UserData>>>>
}

impl AuthManager {
    pub fn new(enabled: bool, super_user_key: String) -> Self {
        let super_user_data = UserData {
            token: super_user_key.clone(),
            allowed_indexes: None,
            permissions: permissions::SUPER_USER,
            created: Utc::now(),
            user: Some(String::from("CONFIG SUPER USER")),
            description: Some(String::from("The super use as defined in the cli commands.")),
        };

        let mut map = HashMap::new();
        map.insert(super_user_key, Arc::new(super_user_data));

        Self {
            auth_enabled: enabled,
            keys: Arc::new(ArcSwap::from_pointee(map))
        }
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        self.auth_enabled
    }

    pub fn create_token(
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

    pub async fn commit(&self, storage: StorageBackend) -> Result<()> {
        let tokens = self.get_all_tokens();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let ref_tokens: Vec<&UserData> = tokens
                .iter()
                .map(|v| v.as_ref())
                .collect();

            storage.store_structure("index_tokens", &ref_tokens)
        }).await?
    }
}
