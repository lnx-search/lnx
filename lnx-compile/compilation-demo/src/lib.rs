use std::marker::PhantomData;
use std::ptr;
use rkyv::rancor;
use rkyv::ser::Serializer;
use rkyv::ser::sharing::Share;
use lnx_ffi::{ArchivedDocument, Document, RawDocumentCallback, DocumentCallback, DocumentError, ParseFormat, short_circuit_error_ptr, SerializeFormat, ErrorKind, BufferWriteCallback, RawBufferWriteCallback};

#[no_mangle]
pub extern "C" fn parse_document(
    buffer: *const u8,
    buffer_len: usize,
    format: ParseFormat,
    callback: *mut RawDocumentCallback,
) -> *const DocumentError {
    if buffer.is_null() {
        let err = Box::new(DocumentError::new(ErrorKind::BufferIsNull, ""));
        return Box::into_raw(err)
    }
    
    let buffer = unsafe { std::slice::from_raw_parts(buffer, buffer_len) };
    let callback = short_circuit_error_ptr!(DocumentCallback::from_raw(callback));
    
    short_circuit_error_ptr!(Foo::load_owned_from_buffer(buffer, format, &callback));
    
    ptr::null()
}

#[no_mangle]
pub extern "C" fn serialize_archived_document(
    document: *const FooAccess,
    callback: *mut RawBufferWriteCallback,
    format: SerializeFormat,
) -> *const DocumentError {
    if document.is_null() {
        let err = Box::new(DocumentError::new(ErrorKind::DocIsNull, ""));
        return Box::into_raw(err)
    }

    let writer = short_circuit_error_ptr!(BufferWriteCallback::from_raw(callback));

    let result = unsafe {
        let document = &*document;
        match format {
            SerializeFormat::Json => document.serialize_to_json(writer),
            SerializeFormat::Msgpack => document.serialize_to_msgpack(writer),
        }
    };
    short_circuit_error_ptr!(result);
    
    ptr::null()
}

#[no_mangle]
pub extern "C" fn serialize_document_to_rkyv(
    document: *const Foo,
    callback: *mut RawBufferWriteCallback,
) -> *const DocumentError {
    if document.is_null() {
        let err = Box::new(DocumentError::new(ErrorKind::DocIsNull, ""));
        return Box::into_raw(err)
    }

    let writer = short_circuit_error_ptr!(BufferWriteCallback::from_raw(callback));

    let result = unsafe {
        let document = &*document;
        document.serialize_to_rkyv(writer)        
    };
    short_circuit_error_ptr!(result);

    ptr::null()
}


#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Foo {
    foo: i32,
}

impl Document for Foo {
    type ArchivedView = FooAccess;

    fn load_owned_from_buffer(buffer: &[u8], kind: ParseFormat, callback: &DocumentCallback) -> Result<(), DocumentError> {
        todo!()
    }

    fn serialize_to_rkyv(&self, writer: impl std::io::Write) -> Result<(), DocumentError> {
        rkyv::util::with_arena(|arena| {
            let mut serializer = Serializer::new(
                rkyv::ser::writer::IoWriter::new(writer),
                arena.acquire(),
                Share::new(),
            );

            rkyv::api::serialize_using::<_, rancor::Error>(self, &mut serializer)
                .map_err(|e| DocumentError::new(ErrorKind::SerializeError, e))
        })?;
        
        Ok(())
    }

    fn access<'a>(buffer: &'a [u8]) -> Result<Self::ArchivedView, DocumentError> {
        let data = buffer.as_ptr();
        let inner: &rkyv::Archived<Self> = rkyv::access::<_, rancor::Error>(buffer)
            .map_err(|e| DocumentError::new(ErrorKind::AccessError, e))?;
        
        Ok(FooAccess {
            data,
            inner: inner as *const _
        })
    }
}


pub struct FooAccess {
    /// The inner data [FooAccess] is tied to.
    data: *const u8,
    /// The inner pointer to the Foo view.
    inner: *const rkyv::Archived<Foo>,
}

impl ArchivedDocument for FooAccess {
    unsafe fn serialize_to_json(&self, writer: impl std::io::Write) -> Result<(), DocumentError> {
        let actual = &*self.inner;
        
        let view = FooView {
            foo: actual.foo.to_native(),  
            _phantom: PhantomData,
        };
        
        serde_json::to_writer(writer, &view)
            .map_err(|_| DocumentError::new(ErrorKind::SerializeError, ""))
    }

    unsafe fn serialize_to_msgpack(&self, mut writer: impl std::io::Write) -> Result<(), DocumentError> {
        let actual = &*self.inner;

        let view = FooView {
            foo: actual.foo.to_native(),
            _phantom: PhantomData,
        };

        rmp_serde::encode::write_named(&mut writer, &view)
            .map_err(|_| DocumentError::new(ErrorKind::SerializeError, ""))
    }
}

#[derive(serde_derive::Serialize)]
struct FooView<'a> {
    foo: i32,
    #[serde(skip)]
    _phantom: PhantomData<&'a ()>,
}