use std::borrow::Cow;
use std::fmt::{Display, Formatter};

use lnx_document::{UserDisplayType, Value};
use smallvec::SmallVec;

use crate::pipeline::{Transform, TransformError};

pub struct TypeFilterTransformer {
    mode: FilterMode,
    filter: Filter,
}

impl TypeFilterTransformer {
    /// Creates a new type filter transformer.
    pub fn new(mode: FilterMode, filter: Filter) -> Self {
        Self { mode, filter }
    }
}

impl Transform for TypeFilterTransformer {
    fn expecting(&self, type_name: &str) -> TransformError {
        let message = format!(
            "The type `{type_name}` is not allowed on this field, {} types: [{}]",
            self.mode,
            self.filter.enabled().join(", ")
        );
        TransformError::new(message)
    }

    fn transform<'a>(&self, value: Value<'a>) -> Result<Value<'a>, TransformError> {
        let mut is_valid = self.filter.is_match(&value);

        if let FilterMode::Deny = self.mode {
            is_valid = !is_valid;
        }

        if is_valid {
            Ok(value)
        } else {
            Err(self.expecting(&value.type_name()))
        }
    }
}

#[derive(Debug)]
/// The operation mode of the filter.
pub enum FilterMode {
    /// The filter will only accept types which are enabled.
    Allow,
    /// The filter will deny any types which are enabled.
    Deny,
}

impl Display for FilterMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Allow => write!(f, "allowed"),
            Self::Deny => write!(f, "denied"),
        }
    }
}

#[derive(Default)]
pub struct Filter {
    null: bool,
    string: bool,
    u64: bool,
    i64: bool,
    f64: bool,
    bool: bool,
    datetime: bool,
    ip: bool,
    bytes: bool,
    facet: bool,
    // Arrays are special because you might care about the inner types within the array
    // which aren't as easy to apply transforms to.
    array: Option<Box<Filter>>,
    object: bool,
}

impl Filter {
    /// Returns if the value matches the enabled filter options or not.
    fn is_match(&self, v: &Value) -> bool {
        match v {
            Value::Null => self.null,
            Value::Str(_) => self.string,
            Value::U64(_) => self.u64,
            Value::I64(_) => self.i64,
            Value::F64(_) => self.f64,
            Value::Bool(_) => self.bool,
            Value::Facet(_) => self.facet,
            Value::DateTime(_) => self.datetime,
            Value::IpAddr(_) => self.ip,
            Value::Bytes(_) => self.bytes,
            Value::Array(values) => {
                if let Some(filter) = self.array.as_ref() {
                    values.iter().all(|v| filter.is_match(v))
                } else {
                    false
                }
            },
            Value::Object(_) => self.object,
        }
    }

    fn enabled(&self) -> SmallVec<[Cow<'static, str>; 4]> {
        let mut parts = SmallVec::new();

        if self.string {
            parts.push(Cow::Borrowed("string"));
        }

        if self.u64 {
            parts.push(Cow::Borrowed("u64"));
        }

        if self.i64 {
            parts.push(Cow::Borrowed("i64"));
        }

        if self.f64 {
            parts.push(Cow::Borrowed("f64"));
        }

        if self.bool {
            parts.push(Cow::Borrowed("bool"));
        }

        if self.datetime {
            parts.push(Cow::Borrowed("datetime"));
        }

        if self.ip {
            parts.push(Cow::Borrowed("ip"));
        }

        if self.bytes {
            parts.push(Cow::Borrowed("bytes"));
        }

        if self.facet {
            parts.push(Cow::Borrowed("facet"));
        }

        if self.object {
            parts.push(Cow::Borrowed("facet"));
        }

        if let Some(filter) = self.array.as_ref() {
            for sub_part in filter.enabled() {
                parts.push(Cow::Owned(format!("array<{sub_part}>")));
            }
        }

        parts
    }
}
