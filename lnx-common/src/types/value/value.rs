use std::fmt::{Display, Formatter};

use bincode::{Decode, Encode};
use tantivy::schema::Facet;

use crate::schema::FieldInfo;
use crate::types::{ConversionError, DateTime};

#[derive(Debug, Clone, Encode, Decode)]
pub enum Value {
    I64(i64),
    U64(u64),
    F64(f64),
    DateTime(DateTime),
    Text(String),
    Bytes(Vec<u8>),
}

impl Value {
    pub fn cast_into_schema_type(
        self,
        info: &FieldInfo,
    ) -> Result<Self, ConversionError> {
        let new = match info {
            FieldInfo::F64 { .. } => {
                let target: f64 = self.try_into()?;
                Self::F64(target)
            },
            FieldInfo::U64 { .. } => {
                let target: u64 = self.try_into()?;
                Self::U64(target)
            },
            FieldInfo::I64 { .. } => {
                let target: i64 = self.try_into()?;
                Self::I64(target)
            },
            FieldInfo::Date { .. } => {
                let target: tantivy::DateTime = self.try_into()?;
                Self::DateTime(DateTime::from(target))
            },
            FieldInfo::Text { .. } => {
                let target: f64 = self.try_into()?;
                Self::F64(target)
            },
            FieldInfo::String { .. } => {
                let target: String = self.try_into()?;
                Self::Text(target)
            },
            FieldInfo::Facet { .. } => {
                // Just for validation purposes
                let target: Facet = self.try_into()?;
                Self::Text(target.to_string())
            },
            FieldInfo::Bytes { .. } => {
                let target: Vec<u8> = self.try_into()?;
                Self::Bytes(target)
            },
        };

        Ok(new)
    }

    #[inline]
    pub fn as_i64(&self) -> Option<&i64> {
        match self {
            Self::I64(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_u64(&self) -> Option<&u64> {
        match self {
            Self::U64(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_f64(&self) -> Option<&f64> {
        match self {
            Self::F64(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_date(&self) -> Option<&DateTime> {
        match self {
            Self::DateTime(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_text(&self) -> Option<&String> {
        match self {
            Self::Text(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            Self::Bytes(v) => Some(v),
            _ => None,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::I64(v1), Self::I64(v2)) => v1 == v2,
            (Self::U64(v1), Self::U64(v2)) => v1 == v2,
            (Self::F64(v1), Self::F64(v2)) => v1 == v2,
            (Self::DateTime(v1), Self::DateTime(v2)) => {
                v1.timestamp_millis() == v2.timestamp_millis()
            },
            (Self::Text(v1), Self::Text(v2)) => v1 == v2,
            (Self::Bytes(v1), Self::Bytes(v2)) => v1 == v2,
            _ => false,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::I64(v) => write!(f, "{}", v),
            Value::U64(v) => write!(f, "{}", v),
            Value::F64(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v),
            Value::Text(v) => write!(f, "{}", v),
            Value::Bytes(v) => write!(f, "{}", base64::encode(v)),
        }
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::Text(v.to_string())
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Self::U64(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}

impl From<DateTime> for Value {
    fn from(v: DateTime) -> Self {
        Self::DateTime(v)
    }
}

impl From<tantivy::DateTime> for Value {
    fn from(v: tantivy::DateTime) -> Self {
        Self::DateTime(DateTime::from(v))
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(v)
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Self::Bytes(v.to_vec())
    }
}
