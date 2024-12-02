use std::mem;

#[repr(C)]
pub struct FFISafeVec<T> {
    data: *const T,
    length: usize,
    capacity: usize,
    drop_cb: fn(ptr: *mut T, length: usize, capacity: usize)
}

impl<T> From<Vec<T>> for FFISafeVec<T> {
    fn from(mut value: Vec<T>) -> Self {
        let slf = Self {
            data: value.as_mut_ptr(),
            length: value.len(),
            capacity: value.len(),
            drop_cb: drop_vec,
        };

        mem::forget(value);

        slf
    }
}


#[no_mangle]
extern "C" fn drop_vec<T>(ptr: *mut T, length: usize, capacity: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Vec::from_raw_parts(ptr, length, capacity);
    }
}