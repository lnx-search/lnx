use std::time::Duration;

use crate::metastore::MetastoreError;

const POOL_SIZE: u32 = if cfg!(test) { 1 } else { 5 };

#[derive(Clone)]
pub struct MetastoreDB {
    pool: sqlx::SqlitePool,
}

impl MetastoreDB {
    /// Attempts to connect to the SQLite database at the given path.
    pub(crate) async fn connect(path: &str) -> Result<Self, MetastoreError> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .acquire_timeout(Duration::from_secs(10))
            .acquire_slow_threshold(Duration::from_secs(1))
            .max_connections(POOL_SIZE)
            .connect(path)
            .await?;

        let slf = Self { pool };

        slf.setup_tables().await?;

        Ok(slf)
    }

    async fn setup_tables(&self) -> Result<(), MetastoreError> {
        let query = r#"
        CREATE TABLE IF NOT EXISTS lnx__bucket_config (
            key TEXT NOT NULL PRIMARY KEY,
            value TEXT NOT NULL
        );
        "#;

        sqlx::query(query).execute(&self.pool).await?;

        Ok(())
    }

    /// Attempts to retrieve a configuration value with the given key.
    pub async fn get_config_value<V>(
        &self,
        key: &str,
    ) -> Result<Option<V>, MetastoreError>
    where
        V: serde::de::DeserializeOwned,
    {
        let query = r#"
            SELECT value
            FROM lnx__bucket_config
            WHERE key = ?;
        "#;

        let value: Option<String> = sqlx::query_scalar(query)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;

        value
            .map(|v| serde_json::from_str(&v).map_err(MetastoreError::ConfigSerdeError))
            .transpose()
    }

    /// Attempts to set a config value with the given key.
    ///
    /// This is implemented as an UPSERT.
    pub async fn set_config_value<V>(
        &self,
        key: &str,
        value: &V,
    ) -> Result<(), MetastoreError>
    where
        V: serde::Serialize + ?Sized,
    {
        let query = r#"
            INSERT INTO lnx__bucket_config (key, value)
            VALUES (?, ?) 
            ON CONFLICT (key) 
            DO UPDATE SET value = excluded.value;
        "#;

        let value =
            serde_json::to_string(&value).map_err(MetastoreError::ConfigSerdeError)?;
        sqlx::query(query)
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Deletes a config value with a given key.
    pub async fn del_config_value(&self, key: &str) -> Result<(), MetastoreError> {
        let query = r#"
            DELETE FROM lnx__bucket_config WHERE key = ?;
        "#;

        sqlx::query(query).bind(key).execute(&self.pool).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_table_setup() {
        let _metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");
    }

    #[tokio::test]
    async fn test_config_kv() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        metastore
            .set_config_value("name", "demo")
            .await
            .expect("Set config name");
        metastore
            .set_config_value("age", &1234)
            .await
            .expect("Set config name");

        let name: String = metastore
            .get_config_value("name")
            .await
            .expect("Get config value")
            .expect("Config value should exist");
        assert_eq!(name, "demo");

        let age: usize = metastore
            .get_config_value("age")
            .await
            .expect("Get config value")
            .expect("Config value should exist");
        assert_eq!(age, 1234);

        let missing: Option<()> = metastore
            .get_config_value("missing")
            .await
            .expect("Get config value");
        assert!(missing.is_none());

        let invalid = metastore
            .get_config_value::<usize>("name")
            .await
            .expect_err("Serde should raise an error");
        assert!(matches!(invalid, MetastoreError::ConfigSerdeError(_)));
    }
}
