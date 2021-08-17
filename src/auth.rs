use anyhow::Result;
use bitflags::bitflags;
use parking_lot::Mutex;
use sqlx::{SqliteConnection, Connection, Row};
use tokio::fs;
use rand::{distributions::Alphanumeric, Rng};

bitflags! {
    struct AuthFlags: u32 {
        const SEARCH = 1 << 0;
        const MODIFY_DOCUMENTS = 1 << 1;
        const MODIFY_INDEXES = 1 << 2;
        const ALL = Self::SEARCH.bits | Self::MODIFY_DOCUMENTS.bits | Self::MODIFY_INDEXES.bits;
    }
}

pub type TokenInfo = (String, u32);

/// A manager around a sqlite database and a hashtable.
///
/// This is used to manage any authorization keys.
/// While this makes the server more secure and is *highly* recommended
/// for production use, it is not the world's most advanced security in
/// the world so it's still not recommend to expose this to the public
/// world even though you have it behind auth.
pub struct AuthManager {
    cached_values: Mutex<evmap::WriteHandle<String, TokenInfo>>,
    storage: tokio::sync::Mutex<SqliteConnection>,
}

impl AuthManager {
    /// Connects to the SQLite database and loads any existing credentials.
    pub async fn connect(&self, dir: &str) -> Result<(Self, evmap::ReadHandle<String, TokenInfo>)> {
        fs::create_dir_all(dir).await?;

        let fp = format!("{}/data.db", dir);

        {
            fs::OpenOptions::new()
                .create(true)
                .open(&fp)
                .await?;
        }

        let (reader, writer) = evmap::new();
        let cached_values = Mutex::new(writer);

        let conn = sqlx::SqliteConnection::connect(&fp).await?;
        let storage = tokio::sync::Mutex::new(conn);

        let inst = Self {
            cached_values,
            storage,
        };

        Ok((inst, reader))
    }

    /// Loads all previously saved data / changes.
    ///
    /// This assumes that the cache is empty / not populated already with
    /// data from the db.
    async fn load_all(&self) -> Result<()> {
        let rows = {
            let mut lock = self.storage.lock().await;
            sqlx::query(
                "SELECT token, username, permissions FROM access_tokens",
            ).fetch_all(&mut *lock).await?
        };

        let mut lock = self.cached_values.lock();

        for row in rows {
            let token: String = row.get("token");
            let username: String = row.get("username");
            let permissions: u32 = row.get("permissions");

            (*lock).insert(token, (username, permissions));
        }

        (*lock).refresh();

        Ok(())
    }

    /// Creates and registers as access token with the given user marking and
    /// permission flags.
    pub async fn create_token(&self, user: String, permissions: u32) -> Result<String> {
        let token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(64)
            .map(char::from)
            .collect();

        {
            let mut lock = self.storage.lock().await;
            sqlx::query("INSERT INTO access_tokens (token, username, permissions) VALUES (?, ?, ?)")
                .bind(token.clone())
                .bind(user.clone())
                .bind(permissions)
                .execute(&mut *lock).await?;
        }

        {
            let mut lock = self.cached_values.lock();
            (*lock).insert(token.clone(), (user, permissions));
            (*lock).refresh();
        }

        Ok(token)
    }

    /// Revokes a created access token.
    pub async fn revoke_token(&self, token: String) -> Result<()> {
        {
            let mut lock = self.storage.lock().await;
            sqlx::query("DELETE FROM access_tokens WHERE token = ?")
                .bind(token.clone())
                .execute(&mut *lock).await?;
        }

        {
            let mut lock = self.cached_values.lock();
            (*lock).clear(token.clone());
            (*lock).refresh();
        }

        Ok(())
    }

}