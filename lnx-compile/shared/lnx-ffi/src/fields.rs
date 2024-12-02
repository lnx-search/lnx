
#[repr(C)]
/// A raw field with a name and value.
pub struct RawField {
    name: RawSlice,
    value: FieldValue
}

#[repr(C)]
/// An FFI safe dynamic field value
pub enum FieldValue {
    /// A `i64` value.
    I64(i64),
    /// A `u64` value.
    U64(u64),
    /// A `f64` value.
    F64(f64),
    /// A raw UTF-8 encoded byte array.
    Text(RawSlice),
    /// An arbitrary byte array.
    Bytes(RawSlice),
    /// A datetime value timestamp.
    DateTime(i64),
    /// A Ipv4 address.
    Ipv4([u8; 4]),
    /// A Ipv6 address.
    Ipv6(u128),
}


#[repr(C)]
/// A raw **borrowed** byte array.
struct RawSlice {
    ptr: *const u8,
    len: usize,
}

impl RawSlice {
    #[inline]
    /// Returns a byte array view of the [RawSlice].
    /// 
    /// # Safety
    /// The pointer must not be null and must still be alive.
    pub(crate) unsafe fn as_slice(&self) -> &[u8] {
         std::slice::from_raw_parts(self.ptr, self.len)
    }
}