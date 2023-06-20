use std::borrow::Cow;
use std::net::Ipv6Addr;

use crate::typed_value::DateTime;

pub trait UserDiplayType {
    fn type_name(&self) -> Cow<'static, str>;
}

impl UserDiplayType for String {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("string")
    }
}

impl UserDiplayType for &str {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("string")
    }
}

impl<'a> UserDiplayType for Cow<'a, str> {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("string")
    }
}

impl UserDiplayType for Vec<u8> {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bytes")
    }
}

impl UserDiplayType for &[u8] {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bytes")
    }
}

impl UserDiplayType for u64 {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("u64")
    }
}

impl UserDiplayType for i64 {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("i64")
    }
}

impl UserDiplayType for f64 {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("f64")
    }
}

impl UserDiplayType for bool {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("bool")
    }
}

impl UserDiplayType for DateTime {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("datetime")
    }
}

impl UserDiplayType for Ipv6Addr {
    fn type_name(&self) -> Cow<'static, str> {
        Cow::Borrowed("ip")
    }
}
