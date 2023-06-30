use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::net::{Ipv4Addr, Ipv6Addr};

use anyhow::{anyhow, bail, Result};
use base64::Engine;
use tantivy::schema::Facet;
use lnx_document::{DateTime, UserDisplayType, Value};
use time::format_description::{well_known, OwnedFormatItem};
use time::OffsetDateTime;

/// The core types values can be casted to.
pub enum TypeCast {
    /// Cast the input value to a `string`.
    String,
    /// Cast the input value to a `u64`.
    U64,
    /// Cast the input value to a `i64`.
    I64,
    /// Cast the input value to a `f64`.
    F64,
    /// Cast the input value to a `bytes`.
    Bytes,
    /// Cast the input value to a `bool`.
    Bool,
    /// Cast the input value to a `datetime`.
    DateTime(DateTimeParser),
    /// Cast the input value to a `ip`.
    IpAddr,
    /// Cast the input value to a `facet`.
    Facet,
}

impl UserDisplayType for TypeCast {
    fn type_name(&self) -> Cow<'static, str> {
        match self {
            TypeCast::String => Cow::Borrowed("string"),
            TypeCast::U64 => Cow::Borrowed("u64"),
            TypeCast::I64 => Cow::Borrowed("i64"),
            TypeCast::F64 => Cow::Borrowed("f64"),
            TypeCast::Bytes => Cow::Borrowed("bytes"),
            TypeCast::Bool => Cow::Borrowed("bool"),
            TypeCast::DateTime(parser) => {
                Cow::Owned(format!("datetime<{}>", parser.supported_formats()))
            },
            TypeCast::IpAddr => Cow::Borrowed("ip"),
            TypeCast::Facet => Cow::Borrowed("facet"),
        }
    }
}

impl TypeCast {
    /// Attempts to cast a JSON value to the cast type.
    pub fn try_cast_value<'a>(
        &self,
        value: Value<'a>,
    ) -> Result<Value<'a>> {
        match value {
            Value::Null => Ok(Value::Null),
            Value::Str(s) => self.try_cast_cow(s),
            Value::U64(v) => self.try_cast_u64(v),
            Value::I64(v) => self.try_cast_i64(v),
            Value::F64(v) => self.try_cast_f64(v),
            Value::Bool(v) => self.try_cast_bool(v),
            Value::Facet(v) => self.try_cast_facet(v),
            Value::DateTime(v) => self.try_cast_datetime(v),
            Value::IpAddr(v) => self.try_cast_ip(v),
            Value::Bytes(v) => self.try_cast_bytes(v),
            Value::Array(elements) => {
                let mut casted = Vec::with_capacity(elements.len());
                for value in elements {
                    if matches!(value, Value::Object(_)) {
                        return Err(self.err_with_detail(value, "due to field containing an array of arrays or array of objects"));
                    }

                    let value = self.try_cast_value(value)?;
                    casted.push(value);
                }
                Ok(Value::Array(casted))
            },
            other => self.bail(other),
        }
    }

    fn bail<T>(&self, value: impl UserDisplayType) -> Result<T> {
        bail!(
            "Cannot cast `{}` to `{}`",
            value.type_name(),
            self.type_name(),
        )
    }

    fn err_invalid_value(
        &self,
        value: impl UserDisplayType + Debug,
    ) -> anyhow::Error {
        anyhow!(
            "Cannot cast `{}` to `{}` due to an invalid value being provided: {:?}",
            value.type_name(),
            self.type_name(),
            value,
        )
    }

    fn err_with_detail(
        &self,
        value: impl UserDisplayType,
        reason: &str
    ) -> anyhow::Error {
        anyhow!(
            "Cannot cast `{}` to `{}` {reason}",
            value.type_name(),
            self.type_name(),
        )
    }

    /// Attempts to cast a string to the cast type.
    pub fn try_cast_str<'a>(
        &self,
        string: &'a str,
    ) -> Result<Value<'a>> {
        self.try_cast_cow(Cow::Borrowed(string))
    }

    /// Attempts to cast a string to the cast type.
    pub fn try_cast_cow<'a>(
        &self,
        string: Cow<'a, str>,
    ) -> Result<Value<'a>> {
        match self {
            Self::String => Ok(Value::Str(string)),
            Self::U64 => string
                .parse::<u64>()
                .map_err(|_| self.err_invalid_value(string))
                .map(Value::U64),
            Self::I64 => string
                .parse::<i64>()
                .map_err(|_| self.err_invalid_value(string))
                .map(Value::I64),
            Self::F64 => string
                .parse::<f64>()
                .map_err(|_| self.err_invalid_value(string))
                .map(Value::F64),
            Self::Bool => string
                .parse::<bool>()
                .map_err(|_| self.err_invalid_value(string))
                .map(Value::Bool),
            Self::DateTime(parser) => parser
                .try_parse_str(string.as_ref())
                .map(Value::DateTime),
            Self::IpAddr => {
                if let Ok(ipv4) = string.parse::<Ipv4Addr>() {
                    return Ok(Value::IpAddr(ipv4.to_ipv6_mapped()));
                }

                if let Ok(ipv6) = string.parse::<Ipv6Addr>() {
                    return Ok(Value::IpAddr(ipv6));
                }

                Err(self.err_invalid_value(string))
            },
            Self::Bytes => {
                let engine = base64::engine::general_purpose::STANDARD;
                if let Ok(bytes) = engine.decode(string.as_ref()) {
                    Ok(Value::Bytes(bytes))
                } else {
                    Err(self.err_invalid_value(string))
                }
            },
            TypeCast::Facet => {
                if let Ok(facet) = Facet::from_text(&string) {
                    Ok(Value::Facet(facet))
                } else {
                    Err(self.err_with_detail(string, "because the value is not in the form of a path"))
                }
            },
        }
    }

    /// Attempts to cast a u64 to the cast type.
    pub fn try_cast_u64<'a>(
        &self,
        v: u64,
    ) -> Result<Value<'a>> {
        match self {
            Self::U64 => Ok(Value::U64(v)),
            Self::I64 => {
                let v: i64 = v.try_into().map_err(|_| {
                    self.err_with_detail(
                        v,
                        "because the input value cannot be safely represented by the target type",
                    )
                })?;
                Ok(Value::I64(v))
            },
            Self::String => {
                let mut buffer = itoa::Buffer::new();
                let s = buffer.format(v);
                Ok(Value::Str(Cow::Owned(s.to_owned())))
            },
            Self::DateTime(parser) => {
                let v: i64 = v.try_into().map_err(|_| {
                    self.err_with_detail(
                        v,
                        "because the input value cannot be safely represented by the target type",
                    )
                })?;
                let dt = parser.try_convert_timestamp(v)?;
                Ok(Value::DateTime(dt))
            },
            _ => self.bail(v),
        }
    }

    /// Attempts to cast a i64 to the cast type.
    pub fn try_cast_i64<'a>(
        &self,
        v: i64,
    ) -> Result<Value<'a>> {
        match self {
            Self::I64 => Ok(Value::I64(v)),
            Self::U64 => {
                let v: u64 = v.try_into().map_err(|_| {
                    self.err_with_detail(
                        v,
                        "because the input value cannot be safely represented by the target type",
                    )
                })?;
                Ok(Value::U64(v))
            },
            Self::String => {
                let mut buffer = itoa::Buffer::new();
                let s = buffer.format(v);
                Ok(Value::Str(Cow::Owned(s.to_owned())))
            },
            Self::DateTime(parser) => {
                let dt = parser.try_convert_timestamp(v)?;
                Ok(Value::DateTime(dt))
            },
            _ => self.bail(v),
        }
    }

    /// Attempts to cast a f64 to the cast type.
    pub fn try_cast_f64<'a>(
        &self,
        v: f64,
    ) -> Result<Value<'a>> {
        match self {
            Self::F64 => Ok(Value::F64(v)),
            Self::String => {
                let mut buffer = ryu::Buffer::new();
                let s = buffer.format(v);
                Ok(Value::Str(Cow::Owned(s.to_owned())))
            },
            _ => self.bail(v),
        }
    }

    /// Attempts to cast a bool to the cast type.
    pub fn try_cast_bool<'a>(
        &self,
        v: bool,
    ) -> Result<Value<'a>> {
        match self {
            Self::Bool => Ok(Value::Bool(v)),
            Self::String => {
                let v = if v { "true" } else { "false" };

                Ok(Value::Str(Cow::Borrowed(v)))
            },
            _ => self.bail(v),
        }
    }

    /// Attempts to cast a datetime to the cast type.
    pub fn try_cast_datetime<'a>(
        &self,
        v: DateTime,
    ) -> Result<Value<'a>> {
        match self {
            Self::DateTime(_) => Ok(Value::DateTime(v)),
            Self::String => match v.format(&well_known::Rfc3339) {
                Err(e) => Err(e),
                Ok(rendered) => Ok(Value::Str(Cow::Owned(rendered))),
            },
            _ => self.bail(v),
        }
    }

    /// Attempts to cast a ip to the cast type.
    pub fn try_cast_ip<'a>(
        &self,
        v: Ipv6Addr,
    ) -> Result<Value<'a>> {
        match self {
            Self::IpAddr => Ok(Value::IpAddr(v)),
            Self::String => {
                let s = if let Some(ipv4) = v.to_ipv4_mapped() {
                    ipv4.to_string()
                } else {
                    v.to_string()
                };
                Ok(Value::Str(Cow::Owned(s)))
            },
            _ => self.bail(v),
        }
    }

    /// Attempts to cast a facet to the cast type.
    pub fn try_cast_facet<'a>(
        &self,
        v: Facet,
    ) -> Result<Value<'a>> {
        match self {
            Self::Facet => Ok(Value::Facet(v)),
            Self::String => Ok(Value::Str(Cow::Owned(v.to_string()))),
            _ => self.bail(v),
        }
    }

    /// Attempts to cast some bytes to the cast type.
    pub fn try_cast_bytes<'a>(
        &self,
        v: Vec<u8>,
    ) -> Result<Value<'a>> {
        match self {
            Self::Bytes => Ok(Value::Bytes(v)),
            _ => self.bail(v),
        }
    }
}

pub enum TimestampResolution {
    Seconds,
    Millis,
    Micros,
}

impl Display for TimestampResolution {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Seconds => write!(f, "unix_seconds"),
            Self::Millis => write!(f, "unix_millis"),
            Self::Micros => write!(f, "unix_micros"),
        }
    }
}

impl TimestampResolution {
    pub fn cast(&self, ts: i64) -> Option<DateTime> {
        match self {
            TimestampResolution::Seconds => DateTime::from_secs(ts),
            TimestampResolution::Millis => DateTime::from_millis(ts),
            TimestampResolution::Micros => DateTime::from_micros(ts),
        }
    }
}

/// A format that can be used to parse a string into a datetime.
pub enum DateTimeFormat {
    /// A RFC 2822 formatted string.
    Rfc2822,
    /// A RFC 3339 formatted string.
    Rfc3339,
    /// A custom, user-defined format.
    Custom {
        format: OwnedFormatItem,
        display: String,
    },
}

impl Display for DateTimeFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rfc2822 => write!(f, "rfc2822"),
            Self::Rfc3339 => write!(f, "rfc3339"),
            Self::Custom { display, .. } => write!(f, "custom<{display:?}>"),
        }
    }
}

impl Debug for DateTimeFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl DateTimeFormat {
    /// Attempts to parse a string to a given format.
    pub fn parse(&self, s: &str) -> Result<DateTime> {
        let dt = match self {
            DateTimeFormat::Rfc2822 => OffsetDateTime::parse(s, &well_known::Rfc2822)?,
            DateTimeFormat::Rfc3339 => OffsetDateTime::parse(s, &well_known::Rfc3339)?,
            DateTimeFormat::Custom { format, .. } => OffsetDateTime::parse(s, format)?,
        };

        Ok(
            DateTime::from_micros((dt.unix_timestamp_nanos() / 1_000) as i64)
                .expect("Casting to native datetime should never fail"),
        )
    }
}

#[derive(Default)]
pub struct DateTimeParser {
    integer_timestamp_resolution: Option<TimestampResolution>,
    string_formats: Vec<DateTimeFormat>,
}

impl DateTimeParser {
    pub fn with_timestamp_resolution(mut self, res: TimestampResolution) -> Self {
        self.integer_timestamp_resolution = Some(res);
        self
    }

    pub fn with_format(mut self, format: DateTimeFormat) -> Self {
        self.string_formats.push(format);
        self
    }

    pub fn add_string(&mut self, format: DateTimeFormat) {
        self.string_formats.push(format);
    }

    pub fn supported_formats(&self) -> String {
        let mut elements = Vec::new();

        if let Some(resolution) = self.integer_timestamp_resolution.as_ref() {
            elements.push(resolution.to_string());
        }

        for format in self.string_formats.iter() {
            elements.push(format.to_string())
        }

        elements.join(",")
    }

    /// Attempts to parse a JSON value into a datetime using the format info.
    pub fn try_parse_json(&self, value: Value) -> Result<DateTime> {
        match value {
            Value::Str(s) => self.try_parse_str(s.as_ref()),
            Value::I64(ts) => self.try_convert_timestamp(ts),
            other => self.bail(other),
        }
    }

    /// Attempts to parse a typed value into a datetime using the format info.
    pub fn try_parse_typed(&self, value: Value) -> Result<DateTime> {
        match value {
            Value::Str(s) => self.try_parse_str(s.as_ref()),
            Value::I64(ts) => self.try_convert_timestamp(ts),
            other => self.bail(other),
        }
    }

    fn bail<T>(&self, value: impl UserDisplayType) -> Result<T> {
        bail!("Cannot cast `{}` to `datetime`", value.type_name())
    }

    /// Attempts to parse a string into a given datetime.
    ///
    /// If the string does not match any format defined for the given parser
    /// the string will be rejected.
    pub fn try_parse_str(&self, s: &str) -> Result<DateTime> {
        for format in self.string_formats.iter() {
            if let Ok(dt) = format.parse(s) {
                return Ok(dt);
            }
        }

        bail!(
            "Cannot cast `string` to `datetime` as it does not match any provided formats: {:?}",
            self.string_formats,
        );
    }

    /// Attempts to parse a unix timestamp into a given datetime.
    ///
    /// If the parser is not configured to convert timestamps then the value is rejected.
    pub fn try_convert_timestamp(&self, ts: i64) -> Result<DateTime> {
        if let Some(resolution) = self.integer_timestamp_resolution.as_ref() {
            resolution
                .cast(ts)
                .ok_or_else(|| anyhow!("Cannot cast timestamp to `datetime` as it goes beyond the bounds of the supported `datetime` range"))
        } else {
            bail!("Cannot cast timestamp to `datetime` as no timestamp resolution was provided by the schema")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_cast_str() {
        let value = TypeCast::String.try_cast_str("hello, world!");
        assert_eq!(value.unwrap(), Value::from("hello, world!"));

        let value = TypeCast::U64.try_cast_str("hello, world!");
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `string` to `u64` due to an invalid value being provided: \"hello, world!\"");

        let value = TypeCast::U64.try_cast_str("124321");
        assert_eq!(value.unwrap(), Value::from(124321u64));

        let value = TypeCast::U64.try_cast_str("-124321");
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `string` to `u64` due to an invalid value being provided: \"-124321\"");

        let value = TypeCast::I64.try_cast_str("124321");
        assert_eq!(value.unwrap(), Value::from(124321i64));
        let value = TypeCast::I64.try_cast_str("-124321");
        assert_eq!(value.unwrap(), Value::from(-124321i64));

        let value = TypeCast::F64.try_cast_str("124321");
        assert_eq!(value.unwrap(), Value::from(124321.0));
        let value = TypeCast::F64.try_cast_str("-124321");
        assert_eq!(value.unwrap(), Value::from(-124321.0));
        let value = TypeCast::F64.try_cast_str("nan");
        assert!(matches!(value.unwrap(), Value::F64(v) if v.is_nan()));

        let value = TypeCast::Bool.try_cast_str("true");
        assert_eq!(value.unwrap(), Value::from(true));
        let value = TypeCast::Bool.try_cast_str("false");
        assert_eq!(value.unwrap(), Value::from(false));
        let value = TypeCast::Bool.try_cast_str("1");
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `string` to `bool` due to an invalid value being provided: \"1\"");
        let value = TypeCast::Bool.try_cast_str("-");
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `string` to `bool` due to an invalid value being provided: \"-\"");

        let value = TypeCast::Bytes.try_cast_str("hello, world!");
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `string` to `bytes` due to an invalid value being provided: \"hello, world!\"");
        let value = TypeCast::Bytes.try_cast_str("aGVsbG8gd29ybGQ=");
        assert_eq!(
            value.unwrap(),
            Value::Bytes(vec![
                104u8, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100
            ])
        );

        let value = TypeCast::IpAddr.try_cast_str("192.168.0.1");
        assert_eq!(
            value.unwrap(),
            Value::IpAddr(Ipv4Addr::new(192, 168, 0, 1).to_ipv6_mapped())
        );
        let value = TypeCast::IpAddr
            .try_cast_str("2345:0425:2CA1:0000:0000:0567:5673:23b5");
        assert_eq!(
            value.unwrap(),
            Value::IpAddr(
                Ipv6Addr::from_str("2345:0425:2CA1:0000:0000:0567:5673:23b5").unwrap()
            )
        );
        let value = TypeCast::IpAddr.try_cast_str("hello, world!");
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `string` to `ip` due to an invalid value being provided: \"hello, world!\"");

        let value = TypeCast::DateTime(
            DateTimeParser::default().with_format(DateTimeFormat::Rfc3339),
        )
        .try_cast_str("2002-10-02T15:00:00Z");
        assert_eq!(
            value.unwrap(),
            Value::DateTime(DateTime::from_micros(1033570800000000).unwrap())
        );
        let value = TypeCast::DateTime(
            DateTimeParser::default()
                .with_format(DateTimeFormat::Rfc2822)
                .with_format(DateTimeFormat::Rfc3339),
        )
        .try_cast_str("2002-10-02T15:00:00Z");
        assert_eq!(
            value.unwrap(),
            Value::DateTime(DateTime::from_micros(1033570800000000).unwrap())
        );
        let value = TypeCast::DateTime(
            DateTimeParser::default().with_format(DateTimeFormat::Rfc3339),
        )
        .try_cast_str("hello, world!");
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `string` to `datetime` as it does not match any provided formats: [rfc3339]");
    }

    #[test]
    fn test_cast_u64() {
        let value = TypeCast::String.try_cast_u64(12456);
        assert_eq!(value.unwrap(), Value::from("12456"));
        let value = TypeCast::String.try_cast_u64(0);
        assert_eq!(value.unwrap(), Value::from("0"));

        let value = TypeCast::U64.try_cast_u64(0);
        assert_eq!(value.unwrap(), Value::U64(0));
        let value = TypeCast::U64.try_cast_u64(12456);
        assert_eq!(value.unwrap(), Value::U64(12456));

        let value = TypeCast::I64.try_cast_u64(12456);
        assert_eq!(value.unwrap(), Value::I64(12456));
        let value = TypeCast::I64.try_cast_u64(0);
        assert_eq!(value.unwrap(), Value::I64(0));
        let value = TypeCast::I64.try_cast_u64(u64::MAX);
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `u64` to `i64` because the input value cannot be safely represented by the target type");

        let value = TypeCast::F64.try_cast_u64(12456);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `f64`"
        );
        let value = TypeCast::F64.try_cast_u64(0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `f64`"
        );
        let value = TypeCast::F64.try_cast_u64(u64::MAX);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `f64`"
        );

        let value = TypeCast::Bool.try_cast_u64(0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `bool`"
        );
        let value = TypeCast::Bool.try_cast_u64(1);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `bool`"
        );
        let value = TypeCast::Bool.try_cast_u64(4);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `bool`"
        );

        let value = TypeCast::Bytes.try_cast_u64(0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `bytes`"
        );
        let value = TypeCast::Bytes.try_cast_u64(2235);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `bytes`"
        );
        let value = TypeCast::Bytes.try_cast_u64(u64::MAX);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `bytes`"
        );

        let value = TypeCast::IpAddr.try_cast_u64(0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `ip`"
        );
        let value = TypeCast::IpAddr.try_cast_u64(2235);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `ip`"
        );
        let value = TypeCast::IpAddr.try_cast_u64(u64::MAX);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `u64` to `ip`"
        );

        let value = TypeCast::DateTime(
            DateTimeParser::default()
                .with_timestamp_resolution(TimestampResolution::Seconds),
        )
        .try_cast_u64(0);
        assert_eq!(
            value.unwrap(),
            Value::DateTime(DateTime::from_secs(0).unwrap())
        );
        let value = TypeCast::DateTime(
            DateTimeParser::default()
                .with_timestamp_resolution(TimestampResolution::Micros),
        )
        .try_cast_u64(2235);
        assert_eq!(
            value.unwrap(),
            Value::DateTime(DateTime::from_micros(2235).unwrap())
        );
        let value = TypeCast::DateTime(
            DateTimeParser::default()
                .with_timestamp_resolution(TimestampResolution::Millis),
        )
        .try_cast_u64(2235);
        assert_eq!(
            value.unwrap(),
            Value::DateTime(DateTime::from_millis(2235).unwrap())
        );
        let value = TypeCast::DateTime(
            DateTimeParser::default()
                .with_timestamp_resolution(TimestampResolution::Millis),
        )
        .try_cast_u64(u64::MAX);
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `u64` to `datetime<unix_millis>` because the input value cannot be safely represented by the target type");
    }

    #[test]
    fn test_cast_i64() {
        let value = TypeCast::String.try_cast_i64(12456);
        assert_eq!(value.unwrap(), Value::from("12456"));
        let value = TypeCast::String.try_cast_i64(0);
        assert_eq!(value.unwrap(), Value::from("0"));

        let value = TypeCast::U64.try_cast_i64(0);
        assert_eq!(value.unwrap(), Value::U64(0));
        let value = TypeCast::U64.try_cast_i64(12456);
        assert_eq!(value.unwrap(), Value::U64(12456));
        let value = TypeCast::U64.try_cast_i64(-12456);
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `i64` to `u64` because the input value cannot be safely represented by the target type");
        let value = TypeCast::U64.try_cast_i64(i64::MAX);
        assert_eq!(value.unwrap(), Value::U64(i64::MAX as u64));
        let value = TypeCast::U64.try_cast_i64(i64::MIN);
        assert_eq!(value.unwrap_err().to_string(), "Cannot cast `i64` to `u64` because the input value cannot be safely represented by the target type");

        let value = TypeCast::F64.try_cast_i64(12456);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `f64`"
        );
        let value = TypeCast::F64.try_cast_i64(0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `f64`"
        );
        let value = TypeCast::F64.try_cast_i64(i64::MAX);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `f64`"
        );

        let value = TypeCast::Bool.try_cast_i64(0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `bool`"
        );
        let value = TypeCast::Bool.try_cast_i64(1);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `bool`"
        );
        let value = TypeCast::Bool.try_cast_i64(4);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `bool`"
        );

        let value = TypeCast::Bytes.try_cast_i64(0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `bytes`"
        );
        let value = TypeCast::Bytes.try_cast_i64(2235);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `bytes`"
        );
        let value = TypeCast::Bytes.try_cast_i64(i64::MAX);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `bytes`"
        );

        let value = TypeCast::IpAddr.try_cast_i64(0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `ip`"
        );
        let value = TypeCast::IpAddr.try_cast_i64(2235);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `ip`"
        );
        let value = TypeCast::IpAddr.try_cast_i64(i64::MAX);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `i64` to `ip`"
        );

        let value = TypeCast::DateTime(
            DateTimeParser::default()
                .with_timestamp_resolution(TimestampResolution::Seconds),
        )
        .try_cast_i64(0);
        assert_eq!(
            value.unwrap(),
            Value::DateTime(DateTime::from_secs(0).unwrap())
        );
        let value = TypeCast::DateTime(
            DateTimeParser::default()
                .with_timestamp_resolution(TimestampResolution::Micros),
        )
        .try_cast_i64(2235);
        assert_eq!(
            value.unwrap(),
            Value::DateTime(DateTime::from_micros(2235).unwrap())
        );
        let value = TypeCast::DateTime(
            DateTimeParser::default()
                .with_timestamp_resolution(TimestampResolution::Millis),
        )
        .try_cast_i64(2235);
        assert_eq!(
            value.unwrap(),
            Value::DateTime(DateTime::from_millis(2235).unwrap())
        );
        let value = TypeCast::DateTime(
            DateTimeParser::default()
                .with_timestamp_resolution(TimestampResolution::Millis),
        )
        .try_cast_i64(i64::MAX);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast timestamp to `datetime` as it goes beyond the bounds of the supported `datetime` range"
        );
    }

    #[test]
    fn test_cast_f64() {
        let value = TypeCast::String.try_cast_f64(12456.0);
        assert_eq!(value.unwrap(), Value::from("12456.0"));
        let value = TypeCast::String.try_cast_f64(0.0);
        assert_eq!(value.unwrap(), Value::from("0.0"));

        let value = TypeCast::F64.try_cast_f64(12456.0);
        assert_eq!(value.unwrap(), Value::from(12456.0));
        let value = TypeCast::F64.try_cast_f64(0.0);
        assert_eq!(value.unwrap(), Value::from(0.0));

        // Types we know we dont support
        let value = TypeCast::U64.try_cast_f64(0.0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `f64` to `u64`"
        );
        let value = TypeCast::I64.try_cast_f64(0.0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `f64` to `i64`"
        );
        let value = TypeCast::Bytes.try_cast_f64(0.0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `f64` to `bytes`"
        );
        let value = TypeCast::IpAddr.try_cast_f64(0.0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `f64` to `ip`"
        );
        let value = TypeCast::DateTime(DateTimeParser::default()).try_cast_f64(0.0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `f64` to `datetime<>`"
        );
        let value = TypeCast::Bool.try_cast_f64(0.0);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `f64` to `bool`"
        );
    }

    #[test]
    fn test_cast_bool() {
        let value = TypeCast::String.try_cast_bool(true);
        assert_eq!(value.unwrap(), Value::from("true"));
        let value = TypeCast::String.try_cast_bool(false);
        assert_eq!(value.unwrap(), Value::from("false"));

        let value = TypeCast::Bool.try_cast_bool(true);
        assert_eq!(value.unwrap(), Value::from(true));
        let value = TypeCast::Bool.try_cast_bool(false);
        assert_eq!(value.unwrap(), Value::from(false));

        // Types we know we dont support
        let value = TypeCast::U64.try_cast_bool(false);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `bool` to `u64`"
        );
        let value = TypeCast::I64.try_cast_bool(false);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `bool` to `i64`"
        );
        let value = TypeCast::Bytes.try_cast_bool(false);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `bool` to `bytes`"
        );
        let value = TypeCast::IpAddr.try_cast_bool(true);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `bool` to `ip`"
        );
        let value =
            TypeCast::DateTime(DateTimeParser::default()).try_cast_bool(false);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `bool` to `datetime<>`"
        );
    }

    #[test]
    fn test_cast_ip() {
        let ipv6 =
            Ipv6Addr::from_str("2345:0425:2CA1:0000:0000:0567:5673:23b5").unwrap();
        let ipv4 = Ipv4Addr::new(192, 168, 0, 1).to_ipv6_mapped();

        let value = TypeCast::String.try_cast_ip(ipv6);
        assert_eq!(
            value.unwrap(),
            Value::from("2345:425:2ca1::567:5673:23b5")
        );
        let value = TypeCast::String.try_cast_ip(ipv4);
        assert_eq!(value.unwrap(), Value::from("192.168.0.1"));

        let value = TypeCast::IpAddr.try_cast_ip(ipv6);
        assert_eq!(value.unwrap(), Value::from(ipv6));
        let value = TypeCast::IpAddr.try_cast_ip(ipv4);
        assert_eq!(value.unwrap(), Value::from(ipv4));

        // Types we know we dont support
        let value = TypeCast::U64.try_cast_ip(ipv4);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `ip` to `u64`"
        );
        let value = TypeCast::I64.try_cast_ip(ipv4);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `ip` to `i64`"
        );
        let value = TypeCast::Bytes.try_cast_ip(ipv4);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `ip` to `bytes`"
        );
        let value = TypeCast::DateTime(DateTimeParser::default()).try_cast_ip(ipv4);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `ip` to `datetime<>`"
        );
        let value = TypeCast::Bool.try_cast_ip(ipv4);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `ip` to `bool`"
        );
    }

    #[test]
    fn test_cast_datetime() {
        let max_time = DateTime::from_micros(i64::MAX).unwrap();
        let min_time = DateTime::from_micros(i64::MIN).unwrap();
        let random_time = DateTime::from_secs(2452352325).unwrap();

        let value = TypeCast::String.try_cast_datetime(max_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot format datetime as is beyond what the format supports rendering"
        );
        let value = TypeCast::String.try_cast_datetime(min_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot format datetime as is beyond what the format supports rendering"
        );
        let value = TypeCast::String.try_cast_datetime(random_time);
        assert_eq!(value.unwrap(), Value::from("2047-09-17T16:58:45Z"));

        let value = TypeCast::DateTime(DateTimeParser::default())
            .try_cast_datetime(max_time);
        assert_eq!(value.unwrap(), Value::from(max_time));
        let value = TypeCast::DateTime(DateTimeParser::default())
            .try_cast_datetime(min_time);
        assert_eq!(value.unwrap(), Value::from(min_time));
        let value = TypeCast::DateTime(DateTimeParser::default())
            .try_cast_datetime(random_time);
        assert_eq!(value.unwrap(), Value::from(random_time));

        // Types we know we dont support
        let value = TypeCast::I64.try_cast_datetime(max_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `datetime` to `i64`"
        );
        let value = TypeCast::I64.try_cast_datetime(min_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `datetime` to `i64`"
        );
        let value = TypeCast::I64.try_cast_datetime(random_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `datetime` to `i64`"
        );
        let value = TypeCast::U64.try_cast_datetime(max_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `datetime` to `u64`"
        );
        let value = TypeCast::U64.try_cast_datetime(min_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `datetime` to `u64`"
        );
        let value = TypeCast::U64.try_cast_datetime(random_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `datetime` to `u64`"
        );
        let value = TypeCast::Bytes.try_cast_datetime(random_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `datetime` to `bytes`"
        );
        let value = TypeCast::F64.try_cast_datetime(random_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `datetime` to `f64`"
        );
        let value = TypeCast::Bool.try_cast_datetime(random_time);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `datetime` to `bool`"
        );
    }

    #[test]
    fn test_cast_facet() {
        let facet = Facet::from_text("/hello/world/foo").unwrap();

        let value = TypeCast::String.try_cast_facet(facet.clone());
        assert_eq!(value.unwrap(), Value::from("/hello/world/foo"));
        let value = TypeCast::Facet.try_cast_facet(facet.clone());
        assert_eq!(value.unwrap(), Value::from(facet.clone()));


        // Types we know we dont support
        let value = TypeCast::I64.try_cast_facet(facet.clone());
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `facet` to `i64`"
        );
        let value = TypeCast::U64.try_cast_facet(facet.clone());
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `facet` to `u64`"
        );
        let value = TypeCast::Bytes.try_cast_facet(facet.clone());
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `facet` to `bytes`"
        );
        let value = TypeCast::F64.try_cast_facet(facet.clone());
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `facet` to `f64`"
        );
        let value = TypeCast::Bool.try_cast_facet(facet.clone());
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `facet` to `bool`"
        );
        let value = TypeCast::DateTime(DateTimeParser::default()).try_cast_facet(facet);
        assert_eq!(
            value.unwrap_err().to_string(),
            "Cannot cast `facet` to `datetime<>`"
        );
    }
}
