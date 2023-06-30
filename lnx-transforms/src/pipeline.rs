use std::borrow::Cow;
use std::net::Ipv6Addr;

use hashbrown::HashMap;
use lnx_document::{DateTime, KeyValues, UserDisplayType, Value};
use smallvec::SmallVec;

pub struct TransformPipeline {
    stages: HashMap<String, SmallVec<[Box<dyn Transform>; 4]>>,
}

pub trait Transform: Send + 'static {
    /// The error message to display when a type cannot be transformed
    /// by the implementor.
    fn expecting(&self, type_name: &str) -> anyhow::Error;

    #[inline]
    fn transform<'a>(&self, value: Value<'a>) -> anyhow::Result<Value<'a>> {
        match value {
            Value::Null => self.transform_null(),
            Value::Str(value) => self.transform_str(value),
            Value::U64(value) => self.transform_u64(value),
            Value::I64(value) => self.transform_i64(value),
            Value::F64(value) => self.transform_f64(value),
            Value::Bool(value) => self.transform_bool(value),
            Value::DateTime(value) => self.transform_datetime(value),
            Value::IpAddr(value) => self.transform_ip(value),
            Value::Bytes(value) => self.transform_bytes(value),
            Value::Array(elements) => self.transform_array(elements),
            Value::Object(object) => self.transform_object(object),
        }
    }

    #[inline]
    fn transform_null<'a>(&self) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&Value::Null.type_name()))
    }

    #[inline]
    fn transform_str<'a>(&self, value: Cow<'a, str>) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    fn transform_u64<'a>(&self, value: u64) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    fn transform_i64<'a>(&self, value: i64) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    fn transform_f64<'a>(&self, value: f64) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    fn transform_bool<'a>(&self, value: bool) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    fn transform_datetime<'a>(&self, value: DateTime) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    fn transform_bytes<'a>(&self, value: Vec<u8>) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    fn transform_ip<'a>(&self, value: Ipv6Addr) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&value.type_name()))
    }

    #[inline]
    fn transform_array<'a>(
        &self,
        elements: Vec<Value<'a>>,
    ) -> anyhow::Result<Value<'a>> {
        // This implementation should allow the compiler to re-use the existing allocation.
        elements.into_iter().map(self.transform).collect()
    }

    #[inline]
    fn transform_object<'a>(&self, object: KeyValues<'a>) -> anyhow::Result<Value<'a>> {
        Err(self.expecting(&object.type_name()))
    }
}
