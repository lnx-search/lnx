use std::borrow::Cow;

use crate::configuration::INDEX_KEYSPACE_PREFIX;

#[derive(Clone)]
pub struct IndexContext {
    name: Cow<'static, String>,
}

impl IndexContext {
    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn id(&self) -> u64 {
        crc32fast::hash(self.name.as_bytes()) as u64
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
