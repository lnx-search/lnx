use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;

use smallvec::SmallVec;

#[derive(Default, Clone)]
/// A string type that's backed by a SmallVec
///
/// WARNING:
/// This type performs no UTF-8 checks, so it is possible
/// to create invalid UTF-8 strings if you're not careful when
/// truncating.
pub(crate) struct SmallStr(SmallVec<[u8; 32]>);

impl SmallStr {
    /// Extends the string with another string.
    pub fn push_str(&mut self, s: &str) {
        self.0.extend_from_slice(s.as_bytes())
    }

    /// Push a single Ascii byte to the string.
    pub fn push_byte(&mut self, b: u8) {
        self.0.push(b);
    }

    /// Returns the length of the string in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Truncates the string to a given number of bytes.
    pub fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    #[cfg(debug_assertions)]
    /// Returns the string representation of the key.
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.0.as_slice()).unwrap()
    }

    #[cfg(not(debug_assertions))]
    /// Returns the string representation of the key.
    pub fn as_key(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.0.as_slice()) }
    }

    /// Produces a slice of the string a ta given position (in bytes.)
    pub fn slice_at(&self, pos: usize) -> &[u8] {
        &self.0[pos..]
    }
}

impl Deref for SmallStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Debug for SmallStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

impl Display for SmallStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq for SmallStr {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialEq<str> for SmallStr {
    fn eq(&self, other: &str) -> bool {
        self.0.as_slice() == other.as_bytes()
    }
}

impl PartialEq<String> for SmallStr {
    fn eq(&self, other: &String) -> bool {
        self.0.as_slice() == other.as_bytes()
    }
}

impl<'a> From<&'a str> for SmallStr {
    fn from(value: &'a str) -> Self {
        let mut data = SmallVec::with_capacity(value.as_bytes().len());
        data.extend_from_slice(value.as_bytes());
        Self(data)
    }
}

impl From<String> for SmallStr {
    fn from(value: String) -> Self {
        Self(SmallVec::from_vec(value.into_bytes()))
    }
}
