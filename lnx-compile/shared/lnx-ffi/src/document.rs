use crate::error::DocumentError;
use crate::ErrorKind;
use crate::format::ParseFormat;

#[macro_export]
/// A magic macro for exporting interactions between the
/// compiled library and main process.
/// 
/// This is done on the base implementation of a type implementing
/// [Document].
macro_rules! extern_magic {
    ($t:ident) => {
        pub extern "C" fn deserialize_document(buffer: &[u8], kind: ParseFormat, out: *mut FFIDocumentIterator) -> DocumentError {
            
        }
    };
}

/// A common document interface between the main lnx process and the compiled
/// libraries.
/// 
/// 
pub trait Document: Sized {
    /// The type the document uses as its [rkyv::Archived] view.
    type ArchivedView;
    
    /// Load a set of [Document] instances from the given buffer with the given format.
    fn load_owned_from_buffer(buffer: &[u8], kind: ParseFormat, callback: &DocumentCallback) -> Result<(), DocumentError>;

    /// Serializes the document to a rkyv buffer.
    fn serialize_to_rkyv(&self, writer: impl std::io::Write) -> Result<(), DocumentError>;
    
    /// Gets a reference to the archived document.
    fn access<'a>(buffer: &'a [u8]) -> Result<Self::ArchivedView, DocumentError>;
}

pub trait ArchivedDocument {
    /// Serialize the document to JSON string.
    unsafe fn serialize_to_json(&self, writer: impl std::io::Write) -> Result<(), DocumentError>;
    /// Serialize the document to MSGPACK binary data.
    unsafe fn serialize_to_msgpack(&self, writer: impl std::io::Write) -> Result<(), DocumentError>;
}


/// A callback type for submitting newly deserialized documents.
pub struct DocumentCallback {
    inner: *mut RawDocumentCallback,
}

impl DocumentCallback {
    /// Creates a new [DocumentCallback] from the raw pointer.
    pub fn from_raw(cb: *mut RawDocumentCallback) -> Result<Self, DocumentError> {
        if cb.is_null() {
            return Err(DocumentError::new(ErrorKind::CallbackIsNull, ""));
        }
        
        Ok(Self {
            inner: cb,
        })
    }
}

/// A callback type for writing buffer results.
pub struct BufferWriteCallback {
    inner: *mut RawBufferWriteCallback,
}

impl BufferWriteCallback {
    pub fn from_raw(cb: *mut RawBufferWriteCallback) -> Result<Self, DocumentError> {
        if cb.is_null() {
            return Err(DocumentError::new(ErrorKind::CallbackIsNull, ""));
        }

        Ok(Self {
            inner: cb,
        })
    }
}

impl std::io::Write for BufferWriteCallback {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        unsafe {
            let inner = &mut *self.inner;
            inner.write(buf);
        }
        Ok(buf.len())
    }
    
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[doc(hidden)]
#[repr(C)]
pub struct RawDocumentCallback {
    
}

impl RawDocumentCallback {
    
}

#[doc(hidden)]
#[repr(C)]
pub struct RawBufferWriteCallback {

}

impl RawBufferWriteCallback {
    fn write(&mut self, buf: &[u8]) {
        
    }
    
}