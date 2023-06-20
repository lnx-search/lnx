use std::borrow::Cow;
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, Ipv6Addr};

use time::formatting::Formattable;
use time::OffsetDateTime;

use crate::UserDiplayType;

pub type TypedMap<'a> = BTreeMap<Cow<'a, str>, Value<'a>>;
pub type TypedMapIter<'a> =
    std::collections::btree_map::IntoIter<Cow<'a, str>, Value<'a>>;

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Str(Cow<'a, str>),
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    DateTime(DateTime),
    IpAddr(Ipv6Addr),
    Bytes(Vec<u8>),
    Array(Vec<Value<'a>>),
    Object(TypedMap<'a>),
}

impl<'a> UserDiplayType for Value<'a> {
    fn type_name(&self) -> Cow<'static, str> {
        match self {
            Value::Null => Cow::Borrowed("null"),
            Value::Str(_) => Cow::Borrowed("string"),
            Value::U64(_) => Cow::Borrowed("u64"),
            Value::I64(_) => Cow::Borrowed("i64"),
            Value::F64(_) => Cow::Borrowed("f64"),
            Value::Bool(_) => Cow::Borrowed("bool"),
            Value::DateTime(_) => Cow::Borrowed("datetime"),
            Value::Bytes(_) => Cow::Borrowed("bytes"),
            Value::Array(_) => Cow::Borrowed("array"),
            Value::Object(_) => Cow::Borrowed("object"),
            Value::IpAddr(_) => Cow::Borrowed("ip"),
        }
    }
}

impl<'a> UserDiplayType for &Value<'a> {
    fn type_name(&self) -> Cow<'static, str> {
        match self {
            Value::Null => Cow::Borrowed("null"),
            Value::Str(_) => Cow::Borrowed("string"),
            Value::U64(_) => Cow::Borrowed("u64"),
            Value::I64(_) => Cow::Borrowed("i64"),
            Value::F64(_) => Cow::Borrowed("f64"),
            Value::Bool(_) => Cow::Borrowed("bool"),
            Value::DateTime(_) => Cow::Borrowed("datetime"),
            Value::Bytes(_) => Cow::Borrowed("bytes"),
            Value::Array(_) => Cow::Borrowed("array"),
            Value::Object(_) => Cow::Borrowed("object"),
            Value::IpAddr(_) => Cow::Borrowed("ip"),
        }
    }
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(value: &'a str) -> Self {
        Self::Str(Cow::Borrowed(value))
    }
}

impl<'a> From<String> for Value<'a> {
    fn from(value: String) -> Self {
        Self::Str(Cow::Owned(value))
    }
}

impl<'a> From<u64> for Value<'a> {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl<'a> From<i64> for Value<'a> {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl<'a> From<f64> for Value<'a> {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl<'a> From<bool> for Value<'a> {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl<'a> From<Ipv4Addr> for Value<'a> {
    fn from(value: Ipv4Addr) -> Self {
        Self::IpAddr(value.to_ipv6_mapped())
    }
}

impl<'a> From<Ipv6Addr> for Value<'a> {
    fn from(value: Ipv6Addr) -> Self {
        Self::IpAddr(value)
    }
}

impl<'a> From<DateTime> for Value<'a> {
    fn from(value: DateTime) -> Self {
        Self::DateTime(value)
    }
}

impl<'a, T: Into<Value<'a>>> From<Vec<T>> for Value<'a> {
    fn from(value: Vec<T>) -> Self {
        Self::Array(value.into_iter().map(|v| v.into()).collect())
    }
}

impl<'a, T: Into<Value<'a>>> From<BTreeMap<Cow<'a, str>, T>> for Value<'a> {
    fn from(value: BTreeMap<Cow<'a, str>, T>) -> Self {
        Self::Object(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Ord, PartialOrd, Eq)]
pub struct DateTime {
    micros: i64,
}

impl DateTime {
    pub const MAX: DateTime = DateTime { micros: i64::MAX };
    pub const MIN: DateTime = DateTime { micros: i64::MIN };

    #[inline]
    pub fn from_secs(v: i64) -> Option<Self> {
        Self::from_millis(i64::checked_mul(v, 1000)?)
    }

    #[inline]
    pub fn from_millis(v: i64) -> Option<Self> {
        Self::from_micros(i64::checked_mul(v, 1000)?)
    }

    #[inline]
    pub fn from_micros(v: i64) -> Option<Self> {
        Some(Self { micros: v })
    }

    #[inline]
    pub fn format(
        &self,
        format: &(impl Formattable + ?Sized),
    ) -> anyhow::Result<String> {
        OffsetDateTime::from_unix_timestamp_nanos(self.micros as i128 * 1000)
            .map_err(|_| anyhow::anyhow!("Cannot format datetime as is beyond what the format supports rendering"))?
            .format(format)
            .map_err(|e| anyhow::anyhow!("Cannot format datetime with the given format: {e}"))
    }
}
