use anyhow::{Result, Error};
use axum::http::header;
use headers::HeaderMapExt;
use hyper::http::{HeaderValue, Request, Response, StatusCode};
use parking_lot::Mutex;
use rand::{distributions::Alphanumeric, Rng};
use serde::Serialize;
use sqlx::{Connection, Row, SqliteConnection};
use std::sync::Arc;
use tokio::fs;
use tower_http::auth::AuthorizeRequest;
use crate::responders::json_response;
use axum::body::Body;
use axum::extract::Extension;

pub struct AuthFlags;
impl AuthFlags {
    pub const SEARCH: u32 = 1 << 0;
    pub const MODIFY_DOCUMENTS: u32 = 1 << 1;
    pub const MODIFY_INDEXES: u32 = 1 << 2;
    pub const ALL: u32 = Self::SEARCH | Self::MODIFY_DOCUMENTS | Self::MODIFY_INDEXES;
}

#[derive(Debug)]
pub enum Op {
    Set,
    Unset,
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
    pub async fn connect(dir: &str) -> Result<(Self, evmap::ReadHandle<String, TokenInfo>)> {
        fs::create_dir_all(dir).await?;

        let fp = format!("{}/data.db", dir);

        {
            fs::OpenOptions::new().create(true).open(&fp).await?;
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
            sqlx::query("SELECT token, username, permissions FROM access_tokens")
                .fetch_all(&mut *lock)
                .await?
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
            sqlx::query(
                "INSERT INTO access_tokens (token, username, permissions) VALUES (?, ?, ?)",
            )
            .bind(token.clone())
            .bind(user.clone())
            .bind(permissions)
            .execute(&mut *lock)
            .await?;
        }

        {
            let mut lock = self.cached_values.lock();
            (*lock).insert(token.clone(), (user, permissions));
            (*lock).refresh();
        }

        let search = (permissions & AuthFlags::SEARCH) != 0;
        let index = (permissions & AuthFlags::MODIFY_INDEXES) != 0;
        let documents = (permissions & AuthFlags::MODIFY_DOCUMENTS) != 0;

        info!(
            "[ AUTHORIZATION ] created access token with permissions SEARCH={}, MODIFY_INDEXES={}, MODIFY_DOCUMENTS={}",
            search, index, documents,
        );

        Ok(token)
    }

    /// Revokes a created access token.
    pub async fn revoke_token(&self, token: String) -> Result<()> {
        {
            let mut lock = self.storage.lock().await;
            sqlx::query("DELETE FROM access_tokens WHERE token = ?")
                .bind(token.clone())
                .execute(&mut *lock)
                .await?;
        }

        {
            let mut lock = self.cached_values.lock();
            (*lock).clear(token.clone());
            (*lock).refresh();
        }

        info!("[ AUTHORIZATION ] revoked access token");

        Ok(())
    }

    /// Either sets or unsets permissions and updates them both in cache
    /// and on disk.
    pub async fn modify_permissions(&self, token: &str, permissions: u32, op: Op) -> Result<()> {
        let new_permissions = {
            let mut lock = self.cached_values.lock();
            if let Some(results) = (*lock).get_one(token) {
                let (username, existing) = results.as_ref();

                let mut new;
                match op {
                    Op::Set => {
                        new = existing & (!permissions);
                    },
                    Op::Unset => {
                        new = existing | permissions;
                    }
                };

                (*lock).update(token.into(), (username.clone(), new));
                (*lock).refresh();

                new
            } else {
                return Err(Error::msg("this token is not registered"))
            }
        };

        {
            let mut lock = self.storage.lock().await;
            sqlx::query("UPDATE access_tokens SET permissions = ?  WHERE token = ?")
                .bind(new_permissions)
                .bind(token.clone())
                .execute(&mut *lock)
                .await?;
        }

        let search = (permissions & AuthFlags::SEARCH) != 0;
        let index = (permissions & AuthFlags::MODIFY_INDEXES) != 0;
        let documents = (permissions & AuthFlags::MODIFY_DOCUMENTS) != 0;

        info!(
            "[ AUTHORIZATION ] updated access token permissions to SEARCH={}, MODIFY_INDEXES={}, MODIFY_DOCUMENTS={}",
            search, index, documents,
        );

        Ok(())
    }
}

pub type TokenReader = Arc<evmap::ReadHandle<String, TokenInfo>>;

/// A authorization layer which watches a map for token keys.
///
/// If enabled this will reject any requests that dont have the auth
/// or dont have the right permissions flags assigned to them.
#[derive(Debug, Clone)]
pub struct UserAuthIfEnabled {
    enabled: bool,
    tokens: TokenReader,
    reject_msg: bytes::Bytes,
    required_permissions: u32,
}

impl UserAuthIfEnabled {
    pub fn bearer<T: Serialize>(
        tokens: TokenReader,
        required_permissions: u32,
        enabled: bool,
        reject_msg: &T,
    ) -> Result<Self> {
        let msg = serde_json::to_vec(&json!({
            "status": StatusCode::UNAUTHORIZED.as_u16(),
            "data": reject_msg
        }))?;
        let reject_msg = bytes::Bytes::copy_from_slice(&msg);

        Ok(Self {
            enabled,
            tokens,
            reject_msg,
            required_permissions,
        })
    }
}

impl AuthorizeRequest for UserAuthIfEnabled {
    type Output = ();
    type ResponseBody = axum::body::BoxBody;

    fn authorize<B>(&mut self, request: &Request<B>) -> Option<Self::Output> {
        if !self.enabled {
            return Some(());
        };

        let header = match request.headers().get(header::AUTHORIZATION) {
            None => return None,
            Some(header) => header,
        };

        // We turn 'Bearer <token>' into ('Bearer', '<token>')
        let buffer = header.as_bytes();
        let token = String::from_utf8_lossy(&buffer[7..]);

        let retrieved = match self.tokens.get_one(token.as_ref()) {
            None => return None,
            Some(values) => values,
        };

        let (username, permissions) = retrieved.as_ref();

        let path = request.uri().path();
        if (*permissions & self.required_permissions) == 0 {
            warn!("[ AUTHORIZATION ] user '{}' attempted an operation with incorrect permissions! Resource path: {:?}", username, path);
            return None;
        }

        debug!(
            "[ AUTHORIZATION ] user {} succeeded permissions check for resource: {:?}",
            username, path
        );

        None
    }

    fn unauthorized_response<B>(&mut self, _request: &Request<B>) -> Response<Self::ResponseBody> {
        let body = axum::body::box_body(hyper::Body::from(self.reject_msg.clone()));
        let mut res = Response::new(body);
        res.headers_mut().typed_insert(headers::ContentType::json());
        *res.status_mut() = StatusCode::UNAUTHORIZED;
        res
    }
}

/// A authorization layer for the master API key.
///
/// This is used to create / delete authorization keys.
#[derive(Debug, Clone)]
pub struct SuperUserAuthIfEnabled {
    enabled: bool,
    auth: HeaderValue,
    reject_msg: bytes::Bytes,
}

impl SuperUserAuthIfEnabled {
    pub fn bearer<T: Serialize>(token: &str, enabled: bool, reject_msg: &T) -> Result<Self> {
        let msg = serde_json::to_vec(&json!({
            "status": StatusCode::UNAUTHORIZED.as_u16(),
            "data": reject_msg
        }))?;
        let reject_msg = bytes::Bytes::copy_from_slice(&msg);
        let auth = HeaderValue::from_str(token).unwrap();

        Ok(Self {
            enabled,
            auth,
            reject_msg,
        })
    }
}

impl AuthorizeRequest for SuperUserAuthIfEnabled {
    type Output = ();
    type ResponseBody = axum::body::BoxBody;

    fn authorize<B>(&mut self, request: &Request<B>) -> Option<Self::Output> {
        if !self.enabled {
            return Some(());
        };

        if let Some(actual) = request.headers().get(header::AUTHORIZATION) {
            (actual == self.auth).then(|| ())
        } else {
            None
        }
    }

    fn unauthorized_response<B>(&mut self, _request: &Request<B>) -> Response<Self::ResponseBody> {
        let body = axum::body::box_body(hyper::Body::from(self.reject_msg.clone()));
        let mut res = Response::new(body);
        res.headers_mut().typed_insert(headers::ContentType::json());
        *res.status_mut() = StatusCode::UNAUTHORIZED;
        res
    }
}

pub async fn create_token(
    Extension(_auth_manager): Extension<AuthManager>,
) -> Response<Body> {
    json_response(StatusCode::OK, &())
}

pub async fn revoke_token(
    Extension(_auth_manager): Extension<AuthManager>,
) -> Response<Body> {
    json_response(StatusCode::OK, &())
}

pub async fn revoke_all(
    Extension(_auth_manager): Extension<AuthManager>,
) -> Response<Body> {
    json_response(StatusCode::OK, &())
}

pub async fn modify_permissions(
    Extension(_auth_manager): Extension<AuthManager>,
) -> Response<Body> {
    json_response(StatusCode::OK, &())
}
