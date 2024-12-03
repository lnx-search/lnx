use std::collections::HashMap;
use std::net::IpAddr;
use chrono::Utc;

pub type FieldId = u64;

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
/// An arbitrarily nested document with fields mapping to values.
pub struct Document(HashMap<FieldId, Value, ahash::RandomState>);

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
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
    Str(Box<str>),
    /// A `bytes` value.
    Bytes(
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<u8>,
    ),
    /// An array of [Value]s.
    Array(
        #[rkyv(omit_bounds)]
        #[rkyv(with = rkyv::with::AsBox)]
        Vec<Value>
    ),
    /// An map of [Value]s.
    Object(#[rkyv(omit_bounds)] HashMap<FieldId, Value, ahash::RandomState>),
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_value_size() {
        dbg!(size_of::<rkyv::Archived<Value>>());
    }
}