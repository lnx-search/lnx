use std::hash::{Hash, Hasher};

use std::sync::Arc;
use chrono::{Utc, DateTime};
use evmap::{ReadGuard};
use parking_lot::Mutex;
use rand::distributions::Alphanumeric;
use rand::{Rng};
use serde::{Serialize, Deserialize};


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

impl Eq for UserData {}
impl PartialEq<Self> for UserData {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl Hash for UserData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.token.hash(state)
    }
}



#[derive(Clone)]
pub struct AuthManager {
    reader: evmap::ReadHandle<String, Arc<UserData>>,
    writer: Arc<Mutex<evmap::WriteHandle<String, Arc<UserData>>>>
}

impl AuthManager {
    pub fn new() -> Self {
        let (reader, writer) = evmap::new();
        Self {
            reader,
            writer: Arc::new(Mutex::new(writer))
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
            description
        });

        {
            let mut lock = self.writer.lock();
            lock.insert(data.token.clone(), data.clone());
            lock.refresh();
        }

        data
    }

    pub fn get_token_data<'rh>(&'rh self, token: &str) -> Option<ReadGuard<'rh, Arc<UserData>>> {
        self.reader.get_one(token)
    }
}
