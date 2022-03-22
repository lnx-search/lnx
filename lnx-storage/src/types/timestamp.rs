use std::fmt::{Display, Formatter};
use std::ops::Deref;

use chrono::Utc;
use lnx_common::types::DateTime;
use scylla::cql_to_rust::{FromCqlVal, FromCqlValError};
use scylla::frame::response::result::CqlValue;
use scylla::frame::value::{self, ValueTooBig};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct Timestamp(pub chrono::Duration);

impl Default for Timestamp {
    fn default() -> Self {
        Self(chrono::Duration::milliseconds(
            Utc::now().timestamp_millis(),
        ))
    }
}

impl Into<DateTime> for Timestamp {
    fn into(self) -> DateTime {
        DateTime::from(self.0.num_seconds() as u64)
    }
}

impl From<chrono::Duration> for Timestamp {
    fn from(v: chrono::Duration) -> Self {
        Self(v)
    }
}

impl From<i64> for Timestamp {
    fn from(v: i64) -> Self {
        Self(chrono::Duration::milliseconds(v))
    }
}
impl Display for Timestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for Timestamp {
    type Target = chrono::Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromCqlVal<CqlValue> for Timestamp {
    fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
        cql_val
            .as_duration()
            .map(Self)
            .ok_or(FromCqlValError::BadCqlType)
    }
}

impl scylla::frame::value::Value for Timestamp {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        value::Timestamp(self.0).serialize(buf)
    }
}
