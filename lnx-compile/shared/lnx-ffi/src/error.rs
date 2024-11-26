use std::ffi::{c_char, CStr, CString};
use std::{mem, ptr};
use std::fmt::{Debug, Display, Formatter};

#[repr(C)]
#[derive(Debug)]
/// An FFI safe [Result] type.
pub enum FFIResult<T> {
    /// The function completed Ok.
    Ok(T),
    /// AN error occurred.
    Err(DocumentError)
}

impl<T> From<FFIResult<T>> for Result<T, DocumentError> {
    #[inline]
    fn from(value: FFIResult<T>) -> Self {
        match value {
            FFIResult::Err(e) => Err(e),
            FFIResult::Ok(v) => Ok(v)
        }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
/// The kind of error that occurred.
pub enum ErrorKind {
    /// The document couldn't be serialized correctly.
    SerializeError,
    /// The document couldn't be deserialized.
    DeserializeError,
    /// The document couldn't be accessed via the Rkyv value.
    AccessError,
    /// The callback pointer is null.
    CallbackNull,
    /// The buffer pointer is null.
    BufferNull,
    /// The document is null.
    DocumentNull,
}

#[repr(C)]
/// An FFI safe error that can occur.
///
/// When dropped, this error will deallocate the message.
pub struct DocumentError {
    kind: ErrorKind,
    message: *mut c_char,
    drop_cb: extern "C" fn(*mut c_char),
}

impl DocumentError {
    /// Creates a new [DocumentError] with the given kind and message.
    pub fn new(kind: ErrorKind, message: impl Display) -> Self {
        let msg = CString::new(message.to_string())
            .expect("Message cannot contain nul terminator");
        Self {
            kind,
            message: msg.into_raw(),
            drop_cb: drop_error,
        }
    }

    #[inline]
    /// Returns the kind of error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[inline]
    /// Returns the message of the error if provided.
    pub fn message(&self) -> Option<&str> {
        if self.message.is_null() {
            return None;
        }

        unsafe {
            let msg = CStr::from_ptr(self.message);
            msg.to_str().ok()
        }
    }
}

impl Drop for DocumentError {
    fn drop(&mut self) {
        if !self.message.is_null() {
            let ptr = mem::replace(&mut self.message, ptr::null_mut());
            unsafe { CString::from_raw(ptr) };
        }
    }
}

impl Display for DocumentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(msg) = self.message() {
            write!(f, "{:?}: {msg}", self.kind)
        } else {
            write!(f, "{:?}", self.kind)
        }
    }
}

impl Debug for DocumentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentError")
            .field("kind", &self.kind)
            .field("message", &self.message())
            .finish()
    }
}

impl From<ErrorKind> for DocumentError {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            message: ptr::null_mut(),
            drop_cb: drop_error,
        }
    }
}

#[no_mangle]
extern "C" fn drop_error(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    
    unsafe {
        let _ = CString::from_raw(ptr);
    }
}