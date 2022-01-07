use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwap;
use bincode::Options;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::helpers::atomic_store;

static KEYSPACE: &str = "index_tokens";

pub mod permissions {
    /// Users with this permission can create and delete indexes.
    ///
    /// This is arguably the most dangerous permission aside from super
    /// user.
    pub const MODIFY_ENGINE: usize = 1 << 0;

    /// Users with this permission can search their allowed indexes.
    pub const SEARCH_INDEX: usize = 1 << 1;

    /// Users with this permission can add and remove documents from their
    /// allowed indexes.
    pub const MODIFY_DOCUMENTS: usize = 1 << 2;

    /// Users with this permission can add and remove stop words from their
    /// allowed indexes.
    pub const MODIFY_STOP_WORDS: usize = 1 << 3;

    /// Users with this permission can create, revoke and modify access tokens.
    pub const MODIFY_AUTH: usize = 1 << 4;

    /// Users with this permission have all of the other permissions.
    pub const SUPER_USER: usize = MODIFY_ENGINE
        | SEARCH_INDEX
        | MODIFY_DOCUMENTS
        | MODIFY_STOP_WORDS
        | MODIFY_AUTH;
}

/// A give set of metadata associated with the access token.
#[derive(Clone, Serialize, Deserialize)]
pub struct TokenData {
    /// The access token itself.
    token: String,

    /// A set of indexes the token is allowed to access.
    ///
    /// If None the token can access all indexes.
    allowed_indexes: Option<Vec<String>>,

    /// The permissions the given token has.
    permissions: usize,

    /// The UTC datetime of when the token was created.
    ///
    /// This does not change with updates.
    created: DateTime<Utc>,

    /// A optional given identifier to tie to a user or other entity.
    user: Option<String>,

    /// A optional description for this token.
    description: Option<String>,
}

impl TokenData {
    /// Checks if the token has the given permissions.
    pub fn has_permissions(&self, flags: usize) -> bool {
        self.permissions & flags != 0
    }

    pub fn has_access_to_index(&self, index: &str) -> bool {
        if let Some(ref indexes) = self.allowed_indexes {
            indexes.iter().any(|v| v == index)
        } else {
            true
        }
    }
}

/// A controller that manages all of the system access tokens.
///
/// If disabled this does nothing.
#[derive(Clone)]
pub struct AuthManager {
    auth_enabled: bool,
    keys: Arc<ArcSwap<HashMap<String, Arc<TokenData>>>>,
}

impl AuthManager {
    /// Creates a new manager being enabled/disabled with a given super user key.
    ///
    /// If disabled the super use key can be an empty string.
    ///
    /// The super user key is simply used as a default key to allow people to
    /// access the control endpoints for the first time.
    /// The super user key can be revoked.
    pub fn new(
        enabled: bool,
        super_user_key: String,
        storage: &sled::Db,
    ) -> Result<Self> {
        let super_user_data = TokenData {
            token: super_user_key.clone(),
            allowed_indexes: None,
            permissions: permissions::SUPER_USER,
            created: Utc::now(),
            user: Some(String::from("CONFIG SUPER USER")),
            description: Some(String::from(
                "The super use as defined in the cli commands.",
            )),
        };

        let mut map = HashMap::new();
        if let Some(data) = storage.get(KEYSPACE)? {
            let tokens: Vec<TokenData> =
                bincode::options().with_big_endian().deserialize(&data)?;

            for token in tokens {
                map.insert(token.token.to_string(), Arc::new(token));
            }
        }

        map.insert(super_user_key, Arc::new(super_user_data));

        Ok(Self {
            auth_enabled: enabled,
            keys: Arc::new(ArcSwap::from_pointee(map)),
        })
    }

    /// Is authorization enabled or disabled.
    #[inline]
    pub fn enabled(&self) -> bool {
        self.auth_enabled
    }

    /// Creates a new access token.
    ///
    /// This generates a 64 character long random token which has the given
    /// set of metadata associated with it.
    pub fn create_token(
        &self,
        permissions: usize,
        user: Option<String>,
        description: Option<String>,
        allowed_indexes: Option<Vec<String>>,
    ) -> Arc<TokenData> {
        let created = Utc::now();
        let token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(64)
            .map(char::from)
            .collect();

        let data = Arc::new(TokenData {
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

    /// Updates a given token with new metadata.
    ///
    /// Similar to create_token, update token updates an existing token's
    /// metadata with a new set of metadata.
    pub fn update_token(
        &self,
        token: &str,
        permissions: usize,
        user: Option<String>,
        description: Option<String>,
        allowed_indexes: Option<Vec<String>>,
    ) -> Option<Arc<TokenData>> {
        let mut new;
        {
            let existing = self.keys.load();
            new = existing.as_ref().clone();
        }

        let existing = new.get(token)?;

        let data = Arc::new(TokenData {
            token: token.to_string(),
            allowed_indexes,
            permissions,
            created: existing.created,
            user,
            description,
        });

        new.insert(data.token.clone(), data.clone());
        self.keys.store(Arc::new(new));

        Some(data)
    }

    /// Gets all access token metadata.
    pub fn get_all_tokens(&self) -> Vec<Arc<TokenData>> {
        self.keys.load().iter().map(|(_, v)| v.clone()).collect()
    }

    /// Gets a specific access token's metadata.
    pub fn get_token_data(&self, token: &str) -> Option<Arc<TokenData>> {
        let guard = self.keys.load();
        guard.get(token).cloned()
    }

    /// Revoke a given access token.
    pub fn revoke_token(&self, token: &str) {
        let mut new;
        {
            let existing = self.keys.load();
            new = existing.as_ref().clone();
            new.remove(token);
        }
        self.keys.store(Arc::new(new));
    }

    /// Revoke all access tokens.
    ///
    /// # WARNING:
    ///     This is absolutely only designed for use in an emergency.
    ///     Running this will revoke all tokens including the super user key,
    ///     run this at your own risk
    pub fn revoke_all_tokens(&self) {
        self.keys.store(Arc::new(HashMap::new()));
    }

    /// Saves and changes to the token state to persistent storage.
    pub async fn commit(&self, storage: sled::Db) -> Result<()> {
        let tokens = self.get_all_tokens();
        let ref_tokens: Vec<TokenData> =
            tokens.into_iter().map(|v| v.as_ref().clone()).collect();

        atomic_store(storage, KEYSPACE, ref_tokens).await?;

        Ok(())
    }
}
