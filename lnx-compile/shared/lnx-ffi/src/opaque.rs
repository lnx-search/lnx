use std::{mem, ptr};

use crate::buffer::FFISafeBuffer;
use crate::DocumentError;


#[repr(C)]
/// An owned document with an opaque type that is FFI safe.
pub struct OpaqueDocument {
    inner: *mut u8,
    drop_cb: extern "C" fn(*mut u8),
    serialize_cb: extern "C" fn(*mut u8) -> Result<FFISafeBuffer, DocumentError>,
}

impl OpaqueDocument {
    /// Serializes the [OpaqueDocument] into bytes.
    pub fn serialize(&self) -> Result<FFISafeBuffer, DocumentError> {
        (self.serialize_cb)(self.inner)
    }
}

impl Drop for OpaqueDocument {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            let inner = mem::replace(&mut self.inner, ptr::null_mut());
            (self.drop_cb)(inner);
        }
    }
}


