use std::collections::HashMap;
use std::fmt::Formatter;
use std::net::IpAddr;


use serde::de::{Error, MapAccess, SeqAccess};
use serde::de::value::{MapAccessDeserializer, SeqAccessDeserializer};
use serde::{Deserialize, Deserializer};
use crate::field_id::FieldId;

#[derive(
    Debug,
    Clone,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde_derive::Deserialize,
)]
#[serde(transparent)]
/// An arbitrarily nested document with fields mapping to values.
pub struct Document(HashMap<FieldId, Value, ahash::RandomState>);

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(
    bounds(
        __C: rkyv::validation::ArchiveContext,
    )
))]
/// A dynamic JSON-like value.
pub enum Value {
    /// A `null` value.
    Null,
    /// A `bool` value.
    Bool(bool),
    /// An `i64` value.
    I64(i64),
    /// A `u64` value.
    U64(u64),
    /// A `f64` value.
    F64(f64),
    /// A [IpAddr] value.
    Ip(Box<IpAddr>),
    /// A datetime which is set to a UTC timezone.
    DateTime(i64),
    /// A `str` value.
    Str(
        #[rkyv(with = rkyv::with::AsBox)]
        String
    ),
    /// A `bytes` value.
    Bytes(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<u8>,
    ),
    /// An array of [Value]s.
    Array(Array),
    /// An map of [Value]s.
    Map(#[rkyv(omit_bounds)] HashMap<FieldId, Value, ahash::RandomState>),
}

impl<'de> serde::de::Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor)
    }
}

struct ValueVisitor;

impl<'de> serde::de::Visitor<'de> for ValueVisitor {
    type Value = Value;

    #[inline]
    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "A JSON-like object with additional support for bytes or IP addresses.")
    }

    #[inline]
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Bool(v))
    }

    #[inline]
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::I64(v))
    }

    #[inline]
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::U64(v))
    }

    #[inline]
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::F64(v))
    }

    #[inline]
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Str(v.to_string()))
    }

    #[inline]
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Str(v))
    }

    #[inline]
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Bytes(v.to_vec()))
    }

    #[inline]
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Bytes(v))
    }

    #[inline]
    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Null)
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Null)
    }

    #[inline]
    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let seq = SeqAccessDeserializer::new(seq);
        let arr = Array::deserialize(seq)?;
        Ok(Value::Array(arr))
    }

    #[inline]
    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let seq = MapAccessDeserializer::new(map);
        let map = HashMap::<FieldId, Value, ahash::RandomState>::deserialize(seq)?;
        Ok(Value::Map(map))
    }
}

#[derive(
    Debug,
    Clone,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde_derive::Deserialize,
)]
#[serde(untagged)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(
    bounds(
        __C: rkyv::validation::ArchiveContext,
    )
))]
/// An array of one specific supported type.
pub enum Array {
    /// An array of bools
    Bool(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<bool>
    ),
    /// An array of i64s
    I64(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<i64>
    ),
    /// And array of u64s
    U64(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<u64>
    ),
    /// An array of floats.
    F64(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<f64>
    ),
    /// An array of Ips.
    Ip(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<IpAddr>
    ),
    /// An array of datetimes.
    Datetime(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<i64>
    ),
    /// An array of strings.
    Str(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<Box<str>>
    ),
    /// An array of byte arrays.
    Bytes(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<Box<[u8]>>
    ),
    #[allow(clippy::enum_variant_names)]
    /// An array of arrays.
    Array(
        #[rkyv(omit_bounds)]
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<Array>
    ),
    /// An array of objects.
    Object(
        #[rkyv(omit_bounds)]
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<HashMap<FieldId, Value, ahash::RandomState>>
    ),
}
