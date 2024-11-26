use std::ffi::CString;
use std::fmt::{Debug, Display, Formatter};

#[doc(hidden)]
#[macro_export]
macro_rules! short_circuit_error_ptr {
    ($res:expr) => {{
        match $res {
            Ok(v) => v,
            Err(e) => {                
                let e = Box::new(e);
                return Box::into_raw(e);
            }
        }
    }};
}


#[repr(C)]
#[derive(Debug, thiserror::Error)]
#[error("{kind:?}: {message}")]
/// An error that can occur from the document FFI api.
pub struct DocumentError {
    pub kind: ErrorKind,
    message: AssumedSafeCString,
}

impl DocumentError {
    /// Creates a new [DocumentError] with the given kind and display message.
    pub fn new(kind: ErrorKind, message: impl Display) -> Self {
        Self {
            kind,
            message: AssumedSafeCString::from(message.to_string()),
        }
    }
}


#[repr(C)]
#[derive(Debug)]
/// The kind of error that originated.
pub enum ErrorKind {
    /// THe document is malformed.
    Malformed,
    /// The provided callback pointer is null.
    CallbackIsNull,
    /// The provided document pointer is null.
    DocIsNull,
    /// The provided buffer pointer is null.
    BufferIsNull,
    /// The document could not be serialized.
    SerializeError,
    /// The system failed to access the archived view of the document.
    AccessError,
}


#[derive(Default)]
#[repr(transparent)]
/// A wrapper type that internally knows the CString 
/// is safely UTF-8. This is just for interop.
struct AssumedSafeCString(CString);

impl AssumedSafeCString {
    fn as_str(&self) -> &str {
        let inner = self.0.as_bytes();
        unsafe { std::str::from_utf8_unchecked(inner) }
    }
}

impl Display for AssumedSafeCString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <str as Display>::fmt(self.as_str(), f)
    }
}

impl Debug for AssumedSafeCString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <str as Debug>::fmt(self.as_str(), f)
    }
}

impl From<&str> for AssumedSafeCString {
    fn from(value: &str) -> Self {
        let inner = CString::new(value)
            .expect("String should not contain null terminator within it");
        Self(inner)
    }
}

impl From<String> for AssumedSafeCString {
    fn from(value: String) -> Self {
        let inner = CString::new(value)
            .expect("String should not contain null terminator within it");
        Self(inner)
    }
}