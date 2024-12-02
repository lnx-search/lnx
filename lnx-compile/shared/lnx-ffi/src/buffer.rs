use std::{mem, ptr};

#[repr(C)]
/// An FFI safe buffer holding some number of bytes.
pub struct FFISafeBuffer {
    data: *mut u8,
    length: usize,
    capacity: usize,
    drop_cb: extern "C" fn(*mut u8, usize, usize),
}

impl FFISafeBuffer {
    #[inline]
    /// Returns a slice representation of the buffer
    /// if the pointer is not null.
    pub fn as_slice(&self) -> Option<&[u8]> {
        if self.data.is_null() {
            None
        } else {
            let slice = unsafe { std::slice::from_raw_parts(self.data, self.length) };
            Some(slice)            
        }
    }
}

impl Drop for FFISafeBuffer {
    fn drop(&mut self) {
        if !self.data.is_null() {
            let data = mem::replace(&mut self.data, ptr::null_mut());
            (self.drop_cb)(data, self.length, self.capacity);
        }
    }
}

impl From<Vec<u8>> for FFISafeBuffer {
    fn from(mut value: Vec<u8>) -> Self {       
        let slf = Self {
            data: value.as_mut_ptr(),
            length: value.len(),
            capacity: value.len(),
            drop_cb: drop_buffer,
        };
        
        mem::forget(value);

        slf
    }
}

#[no_mangle]
extern "C" fn drop_buffer(ptr: *mut u8, length: usize, capacity: usize) {
    if ptr.is_null() {
        return;
    }
    
    unsafe {
        let _ = Vec::from_raw_parts(ptr, length, capacity);
    }
}