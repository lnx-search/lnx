use std::borrow::Cow;
use std::fmt::Formatter;
use std::marker::PhantomData;

use ahash::HashMapExt;
use serde::de::{Error, MapAccess, SeqAccess};
use serde::de::value::SeqAccessDeserializer;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone)]
/// A document object which tries to use as much borrowed data as possible
/// for initial processing (before type checking and conversion.)
/// 
/// This document allows for partial borrowing of both document keys
/// *and* values if available which can significantly improve the deserializing performance
/// of things like JSON objects.
pub struct BorrowedDocument<'a>(pub ahash::HashMap<Cow<'a, str>, BorrowedValue<'a>>);

impl<'a, 'de: 'a> serde::de::Deserialize<'de> for BorrowedDocument<'a> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(DocumentVisitor(PhantomData))
    }
}

struct DocumentVisitor<'a>(PhantomData<&'a ()>);
impl<'a, 'de: 'a> serde::de::Visitor<'de> for DocumentVisitor<'a> {
    type Value = BorrowedDocument<'a>;

    #[inline]
    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "A map object with JSON-like values and string keys")
    }

    #[inline]
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = ahash::HashMap::with_capacity(map.size_hint().unwrap_or(1));
        while let Some((k, v)) = map.next_entry()? {
            let key: CowStrWrapper = k;
            object.insert(key.0, v);            
        }        
        Ok(BorrowedDocument(object))
    }
}


#[derive(Debug, Clone)]
/// A JSON-like object that can be used for deserializing incoming payloads
/// and convert them to strictly typed [Documents](crate::Document).
pub enum BorrowedValue<'a> {
    /// A null value.
    Null,
    /// A boolean value.
    Bool(bool),
    /// A `u64` value.
    U64(u64),
    /// A `u32` value.
    U32(u32),
    /// A `u16` value.
    U16(u16),
    /// A `u8` value.
    U8(u8),
    /// A `i64` value.
    I64(i64),
    /// A `i32` value.
    I32(i32),
    /// A `i16` value.
    I16(i16),
    /// A `i8` value.
    I8(i8),
    /// A `f64` value.
    F64(f64),
    /// A `f32` value.
    F32(f32),
    /// A string value.
    Str(Cow<'a, str>),
    /// An arbitrary byte array.
    Bytes(Cow<'a, [u8]>),
    /// An array of dynamic objects.
    Array(Vec<BorrowedValue<'a>>),
    /// A nested dynamic object.
    Object(Vec<(Cow<'a, str>, BorrowedValue<'a>)>)
}

impl<'a, 'de: 'a> serde::de::Deserialize<'de> for BorrowedValue<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>
    {
        deserializer.deserialize_any(ValueVisitor(PhantomData))
    }
}

struct ValueVisitor<'a>(PhantomData<&'a ()>);

impl<'a, 'de: 'a> serde::de::Visitor<'de> for ValueVisitor<'a> {
    type Value = BorrowedValue<'a>;

    #[inline]
    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "A JSON-like value or byte array")
    }

    #[inline]
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::Bool(v))
    }

    #[inline]
    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::I8(v))
    }

    #[inline]
    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::I16(v))
    }

    #[inline]
    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::I32(v))
    }

    #[inline]
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::I64(v))
    }

    #[inline]
    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::U8(v))
    }

    #[inline]
    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::U16(v))
    }

    #[inline]
    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::U32(v))
    }

    #[inline]
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::U64(v))
    }


    #[inline]
    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::F32(v))
    }


    #[inline]
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::F64(v))
    }

    #[inline]
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::Str(Cow::Owned(v.into())))
    }

    #[inline]
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::Str(Cow::Borrowed(v)))
    }

    #[inline]
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::Str(Cow::Owned(v.into())))
    }

    #[inline]
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::Bytes(Cow::Owned(v.into())))
    }

    #[inline]
    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::Bytes(Cow::Borrowed(v)))
    }

    #[inline]
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::Bytes(Cow::Owned(v)))
    }

    #[inline]
    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::Null)
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BorrowedValue::Null)
    }

    #[inline]
    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let seq = SeqAccessDeserializer::new(seq);
        let object = Vec::deserialize(seq)?;
        Ok(BorrowedValue::Array(object))
    }

    #[inline]
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = Vec::with_capacity(map.size_hint().unwrap_or(1));
        while let Some((k, v)) = map.next_entry()? {
            object.push((k, v));
        }
        Ok(BorrowedValue::Object(object))
    }
}


struct CowStrWrapper<'a>(Cow<'a, str>);
impl<'a, 'de: 'a> serde::de::Deserialize<'de> for CowStrWrapper<'a> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer
            .deserialize_any(CowStrAccess(PhantomData))
            .map(CowStrWrapper)
    }
}

struct CowStrAccess<'a>(PhantomData<&'a ()>);
impl<'a, 'de: 'a> serde::de::Visitor<'de> for CowStrAccess<'a> {
    type Value = Cow<'a, str>;

    #[inline]
    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "string value")
    }

    #[inline]
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Cow::Owned(v.into()))
    }

    #[inline]
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Cow::Borrowed(v))
    }

    #[inline]
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Cow::Owned(v))
    }
}

struct CowBytesWrapper<'a>(Cow<'a, [u8]>);
impl<'a, 'de: 'a> serde::de::Deserialize<'de> for CowBytesWrapper<'a> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer
            .deserialize_any(CowBytesAccess(PhantomData))
            .map(CowBytesWrapper)
    }
}

struct CowBytesAccess<'a>(PhantomData<&'a ()>);
impl<'a, 'de: 'a> serde::de::Visitor<'de> for CowBytesAccess<'a> {
    type Value = Cow<'a, [u8]>;

    #[inline]
    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "string value")
    }

    #[inline]
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Cow::Owned(v.into()))
    }

    #[inline]
    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Cow::Borrowed(v))
    }

    #[inline]
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Cow::Owned(v))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use super::*;
    
    static RAW_JSON: &str = r#"
    {
        "u64_field": 123,
        "i64_field": -1234,
        "f64_field": 12.345,
        "str_borrowed_field": "foo",
        "str_owned_field": "foo and \"bar\"",
        "bool_field": true,
        "null_field": null,
        "array_field": [
            true,
            "false",
            null
        ],
        "object_field": {
            "more": {
                "nesting": "bar",
                "baz": [1, 2, 3]
            }
        },
        "owned \"keyed\" field": "demo"
    }
    "#;
    
    macro_rules! check_value {
        ($document:ident, $key:expr, $key_borrowed:expr, $expected_value:pat) => {{
            let (key, value) = $document.0.remove_entry(&Cow::Borrowed($key)).expect("Entry should exist");
            
            let is_borrowed = match key {
                Cow::Owned(_) => false,
                Cow::Borrowed(_) => true,
            };
            assert_eq!(is_borrowed, $key_borrowed, "Field name should be borrowed={}", $key_borrowed);
            assert!(matches!(value, $expected_value), "Field ({:?}) value should be {} got {value:?}", $key, stringify!($expected_value));            
        }};
    }
    
    #[test]
    fn test_json_deserialization() {
        let mut document: BorrowedDocument = serde_json::from_str(RAW_JSON).unwrap();
        check_value!(document, "u64_field", true, BorrowedValue::U64(123));
        check_value!(document, "i64_field", true, BorrowedValue::I64(-1234));
        check_value!(document, "f64_field", true, BorrowedValue::F64(12.345));
        check_value!(document, "str_borrowed_field", true, BorrowedValue::Str(Cow::Borrowed("foo")));
        check_value!(document, "str_owned_field", true, BorrowedValue::Str(Cow::Owned(_)));
        check_value!(document, "bool_field", true, BorrowedValue::Bool(true));
        check_value!(document, "null_field", true, BorrowedValue::Null);
        check_value!(document, "array_field", true, BorrowedValue::Array(_));
        check_value!(document, "object_field", true, BorrowedValue::Object(_));
        check_value!(document, "owned \"keyed\" field", false, BorrowedValue::Str(Cow::Borrowed("demo")));
    }
    
    #[derive(serde_derive::Serialize)]
    struct MsgpackSampleStruct {
        u64_field: u64,
        u32_field: u32,
        u16_field: u16,
        u8_field:  u8,
        i64_field: i64,
        i32_field: i32,
        i16_field: i16,
        i8_field:  i8,
        f64_field: f64,
        f32_field: f32,
        bool_field: bool,
        null_field: Option<()>,
        str_field: String,
        array_field: Vec<bool>,
        object_field: serde_json::Value,
        #[serde(rename = "owned \"keyed\" field")]
        borrowed_escaped_field: String,
    }
    
    #[test]
    fn test_msgpack_deserialization() {
        let sample = MsgpackSampleStruct {
            u64_field: 3453454342345,
            u32_field: 124232,
            u16_field: 4234,
            u8_field: 123,
            i64_field: -3453454342345,
            i32_field: -124232,
            i16_field: -4234,
            i8_field: -123,
            f64_field: 12.3,
            f32_field: 12.3,
            bool_field: true,
            null_field: None,
            str_field: "demo \"text\" here".to_string(),
            array_field: vec![false, true],
            object_field: json!({
                "more": {
                    "nesting": "bar",
                    "baz": [1, 2, 3]
                }
            }),
            borrowed_escaped_field: "foo".to_string(),
        };
        
        let data = rmp_serde::to_vec_named(&sample).unwrap();
        let mut document: BorrowedDocument = rmp_serde::from_slice(&data).unwrap();
        check_value!(document, "u64_field", true, BorrowedValue::U64(3453454342345));
        check_value!(document, "u32_field", true, BorrowedValue::U32(124232));
        check_value!(document, "u16_field", true, BorrowedValue::U16(4234));
        check_value!(document, "u8_field", true, BorrowedValue::U8(123));
        check_value!(document, "i64_field", true, BorrowedValue::I64(-3453454342345));
        check_value!(document, "i32_field", true, BorrowedValue::I32(-124232));
        check_value!(document, "i16_field", true, BorrowedValue::I16(-4234));
        check_value!(document, "i8_field", true, BorrowedValue::I8(-123));
        check_value!(document, "f64_field", true, BorrowedValue::F64(12.3));
        check_value!(document, "f32_field", true, BorrowedValue::F32(12.3));
        check_value!(document, "str_field", true, BorrowedValue::Str(Cow::Borrowed("demo \"text\" here")));
        check_value!(document, "bool_field", true, BorrowedValue::Bool(true));
        check_value!(document, "null_field", true, BorrowedValue::Null);
        check_value!(document, "array_field", true, BorrowedValue::Array(_));
        check_value!(document, "object_field", true, BorrowedValue::Object(_));
        check_value!(document, "owned \"keyed\" field", true, BorrowedValue::Str(Cow::Borrowed("foo")));
    }
}