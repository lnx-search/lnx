use std::borrow::Cow;
use std::net::Ipv6Addr;

use base64::Engine;

use crate::value::DateTime;

#[inline]
pub fn to_base64_string(buf: &[u8]) -> String {
    base64::prelude::BASE64_STANDARD.encode(buf)
}

pub trait UserDisplayType {
    fn type_name(&self) -> Cow<'static, str>;
}

impl UserDisplayType for String {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("string")
    }
}

impl UserDisplayType for &str {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("string")
    }
}

impl<'a> UserDisplayType for Cow<'a, str> {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("string")
    }
}

impl UserDisplayType for Vec<u8> {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bytes")
    }
}

impl UserDisplayType for &[u8] {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bytes")
    }
}

impl UserDisplayType for u64 {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("u64")
    }
}

impl UserDisplayType for i64 {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("i64")
    }
}

impl UserDisplayType for f64 {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("f64")
    }
}

impl UserDisplayType for bool {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bool")
    }
}

impl UserDisplayType for DateTime {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("datetime")
    }
}

impl UserDisplayType for Ipv6Addr {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("ip")
    }
}

impl UserDisplayType for tantivy::schema::Facet {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("facet")
    }
}
