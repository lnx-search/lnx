use std::borrow::Cow;
use serde::{Serialize, Deserialize};

use crate::configuration::INDEX_KEYSPACE_PREFIX;
use crate::index::polling::PollingMode;
use crate::schema::Schema;

#[derive(Clone, Serialize, Deserialize)]
pub struct IndexContext {
    name: Cow<'static, String>,
    schema: Schema,
    polling_mode: PollingMode,
    storage_config: Option<serde_json::Value>,
}

impl IndexContext {
    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    #[inline]
    pub fn id(&self) -> u64 {
        crc32fast::hash(self.name.as_bytes()) as u64
    }

    #[inline]
    pub fn polling_mode(&self) -> PollingMode {
        self.polling_mode
    }

    #[inline]
    pub fn storage_config(&self) -> Option<&serde_json::Value> {
        self.storage_config.as_ref()
    }

    #[inline]
    pub fn keyspace(&self) -> String {
        format!(
            "{prefix}_{index}",
            prefix = INDEX_KEYSPACE_PREFIX,
            index = self.id()
        )
    }
}
