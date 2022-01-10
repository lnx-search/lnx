use std::cmp::Reverse;
use std::hash::{Hash, Hasher};

use anyhow::Result;
use tantivy::schema::Schema;
use tantivy::DateTime;


pub(crate) trait Validate {
    fn validate(&self) -> Result<()> {
        Ok(())
    }

    fn validate_with_schema(&self, _schema: &Schema) -> Result<()> {
        Ok(())
    }
}

pub(crate) trait AsScore {
    fn as_score(&self) -> Option<f32> {
        None
    }
}

impl AsScore for f64 {}
impl AsScore for u64 {}
impl AsScore for i64 {}
impl AsScore for DateTime {}
impl AsScore for Reverse<f64> {}
impl AsScore for Reverse<u64> {}
impl AsScore for Reverse<i64> {}
impl AsScore for Reverse<DateTime> {}

impl AsScore for f32 {
    fn as_score(&self) -> Option<f32> {
        Some(*self)
    }
}

pub fn cr32_hash(v: impl Hash) -> u64 {
    let mut hasher = crc32fast::Hasher::default();

    v.hash(&mut hasher);

    hasher.finish()
}

