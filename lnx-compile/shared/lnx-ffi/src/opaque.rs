use std::{mem, ptr};

use crate::buffer::FFISafeBuffer;
use crate::{DocumentError, SerializeFormat};
use crate::fields::RawField;
use crate::vec::FFISafeVec;

#[repr(C)]
/// An owned document with an opaque type that is FFI safe.
pub struct OpaqueDocument {
    inner: *mut u8,
    drop_cb: extern "C" fn(*mut u8),
    serialize_cb: extern "C" fn(*mut u8) -> Result<FFISafeBuffer, DocumentError>,
}

impl OpaqueDocument {
    #[inline]
    /// Serializes the [OpaqueDocument] into bytes.
    pub fn serialize(&self) -> Result<FFISafeBuffer, DocumentError> {
        (self.serialize_cb)(self.inner)
    }
    
    #[inline]
    /// Returns a vector of the fields for indexing.
    pub fn indexable_field_values(&self) -> FFISafeVec<RawField> {
        todo!()
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

unsafe impl Send for OpaqueDocument {}

#[repr(C)]
/// An opaque view of the archived document.
/// 
/// Its lifetime is implicitly tied to the lifetime of the data pointer.
pub struct OpaqueArchivedDocument {
    data_ptr: *const u8,
    view_ptr: *const u8,
    serialize_cb: extern "C" fn(*const u8, SerializeFormat) -> Result<FFISafeBuffer, DocumentError>,
}

impl OpaqueArchivedDocument {
    #[inline]
    /// Attempts to serialize the archived document into the given format.
    /// 
    /// # Safety
    /// This function does _not_ check that the pointer to the view is still
    /// valid and the buffer hasn't been dropped.
    pub unsafe fn serialize(&self, to: SerializeFormat) -> Result<FFISafeBuffer, DocumentError> {
        (self.serialize_cb)(self.view_ptr, to)
    }
}
