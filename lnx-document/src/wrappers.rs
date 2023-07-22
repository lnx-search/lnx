use std::borrow::Cow;
use std::ops::{Deref, DerefMut};

use rkyv::{Archive, Serialize};

#[repr(transparent)]
#[derive(Clone, Debug, Archive, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[archive(check_bytes)]
/// A wrapper new-type that applies the `Raw` optimization to
/// the inner `Vec<T>`.
pub struct RawWrapper<T: Copy + Archive + 'static>(
    #[with(rkyv::with::AsBox)]
    #[with(rkyv::with::Raw)]
    pub Vec<T>,
);

impl<T: Copy + Archive + 'static> Default for RawWrapper<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T: Copy + Archive + 'static> Deref for RawWrapper<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Copy + Archive + 'static> DerefMut for RawWrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Copy + Archive + 'static> From<Vec<T>> for RawWrapper<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T: Copy + Archive + 'static> Deref for ArchivedRawWrapper<T> {
    type Target = [rkyv::Archived<T>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[repr(transparent)]
#[derive(Clone, Debug, Archive, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[archive(check_bytes)]
/// A wrapper new-type that applies the `CopyOptimize` optimization to
/// the inner `Vec<T>`.
pub struct CopyWrapper<T: Copy + Archive + 'static>(
    #[with(rkyv::with::AsBox)]
    #[with(rkyv::with::CopyOptimize)]
    pub Vec<T>,
);

impl<T: Copy + Archive + 'static> Default for CopyWrapper<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T: Copy + Archive + 'static> Deref for CopyWrapper<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Copy + Archive + 'static> DerefMut for CopyWrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Copy + Archive + 'static> From<Vec<T>> for CopyWrapper<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T: Copy + Archive + 'static> Deref for ArchivedCopyWrapper<T> {
    type Target = [rkyv::Archived<T>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[repr(transparent)]
#[derive(Clone, Debug, Archive, Serialize, Ord, PartialOrd, Eq, PartialEq)]
#[archive(check_bytes)]
/// An UTF-8 string wrapper.
///
/// This type implements some (de)serialization optimisations compared
/// to a regular string.
pub struct Text<'a>(
    #[with(rkyv::with::AsOwned)]
    // TODO: Re-enable #[with(rkyv::with::Raw)]
    Cow<'a, [u8]>,
);

impl<'a> Into<Cow<'a, str>> for Text<'a> {
    fn into(self) -> Cow<'a, str> {
        unsafe {
            match self.0 {
                Cow::Borrowed(buf) => Cow::Borrowed(std::str::from_utf8_unchecked(buf)),
                Cow::Owned(buf) => Cow::Owned(String::from_utf8_unchecked(buf)),
            }
        }
    }
}

impl<'a> From<Cow<'a, str>> for Text<'a> {
    fn from(value: Cow<'a, str>) -> Self {
        match value {
            Cow::Borrowed(b) => Self(Cow::Borrowed(b.as_bytes())),
            Cow::Owned(b) => {
                let v: String = b; // If you see an error here from your linter, this is just a linter issue.
                Self(Cow::Owned(v.into_bytes()))
            },
        }
    }
}

impl<'a> From<&'a str> for Text<'a> {
    fn from(value: &'a str) -> Self {
        Self(Cow::Borrowed(value.as_bytes()))
    }
}

impl<'a> From<String> for Text<'a> {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value.into_bytes()))
    }
}

impl<'a> AsRef<str> for Text<'a> {
    fn as_ref(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) }
    }
}

impl<'a> Deref for Text<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) }
    }
}

impl<'a> AsRef<str> for ArchivedText<'a> {
    fn as_ref(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) }
    }
}

#[repr(transparent)]
#[derive(Clone, Debug, Archive, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[archive(check_bytes)]
/// An arbitrary bytes sequence wrapper.
///
/// This type implements some (de)serialization optimisations compared
/// to a regular vec.
pub struct Bytes(#[with(rkyv::with::Raw)] pub Vec<u8>);

impl From<Vec<u8>> for Bytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl AsRef<[u8]> for ArchivedBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}
