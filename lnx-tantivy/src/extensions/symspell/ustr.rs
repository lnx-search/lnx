use std::cmp;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

#[derive(Copy, Clone, Default, Ord, PartialOrd, Eq, PartialEq)]
/// A fixed sized 16-byte long byte slice
pub struct UBytes {
    value: u128,
    len: u8,
}

impl UBytes {
    #[inline]
    /// Creates new [UBytes] from the input slice taking upto the first 16 bytes.
    pub fn from_slice_prefix(slice: &[u8]) -> Self {
        let len = cmp::min(slice.len(), size_of::<u128>());
        let mut bytes = [0; size_of::<u128>()];
        bytes[..len].copy_from_slice(&slice[..len]);
        Self {
            value: u128::from_be_bytes(bytes),
            len: len as u8,
        }
    }
    
    #[inline]
    /// Returns the length of the bytes.
    pub fn len(&self) -> usize {
        unsafe { std::hint::assert_unchecked(self.len <= 16) }
        self.len as usize
    }
    
    #[inline]
    /// Returns if the bytes are empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    
    #[inline]
    /// Removes the byte at the given `idx`.
    /// 
    /// # Note on correctness
    /// You must ensure `idx` value does not exceed the length
    /// and does not exceed 16 bytes otherwise the results
    /// will be random.
    pub fn remove(&self, idx: usize) -> Self  {
        let value = remove_byte_u128(self.value, idx);
        Self {
            value,
            len: self.len - 1
        }
    }
    
    #[inline]
    /// Slices the bytes selecting the given start and end
    pub fn slice(&self, start: usize, end: usize) -> Self {
        let value = slice_bytes_u128(self.value, start, end);
        Self {
            value,
            len: (end - start) as u8,
        }
    }

    #[inline]
    /// Selects the suffix of the bytes from the given start pos.
    /// 
    /// # Note on correctness
    /// You must ensure `idx` value does not exceed the length
    /// and does not exceed 16 bytes otherwise the results
    /// will be random.
    pub fn suffix(&self, start: usize) -> Self {
        self.slice(start, self.len())
    }

    #[inline]
    /// Returns the byte at the given position.
    /// 
    /// Panics if the `idx` lies outside the length of the bytes.
    pub fn at(&self, idx: usize) -> u8 {
        assert!(idx < self.len(), "Index to slice at was outside of range");
        let bytes: [u8; size_of::<u128>()] = bytemuck::cast(self.value);
        unsafe { *bytes.get_unchecked(idx) }
    }
}

impl Hash for UBytes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state)
    }
}

impl Debug for UBytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes = self.value.to_be_bytes();
        let str_lossy = String::from_utf8_lossy(&bytes[..self.len as usize]);
        write!(f, "{str_lossy:?}")
    }
}

const BYTE: usize = 8;

/// Removes a byte from the value at the given position and returns the new value.
/// 
/// For example:
/// 
/// `[1, 2, 3, 4, ...]` removing pos `2` will yield: `[1, 2, 4, 0, ...]`
fn remove_byte_u128(value: u128, pos: usize) -> u128 {
    debug_assert!(pos < size_of::<u128>());
    
    const FULL_MASK: u128 = 0xFF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF;
    
    // Edge case: pos-15 just requires a shift left...
    
    let sl = 15 - pos;
    
    let pos_shift_bits = pos * BYTE;    
    let back_mask = FULL_MASK >> pos_shift_bits;
    let front_mask = !back_mask;
    println!("msk-frn: {:?}", front_mask.to_be_bytes());
    println!("msk-bck: {:?}", back_mask.to_be_bytes());
    println!("bck: {:?}", (value & back_mask).to_be_bytes());
    println!("frnt: {:?}", (value & front_mask).to_be_bytes());
    let back = (value << (pos_shift_bits + BYTE)) >> pos_shift_bits;    
    let front = value & front_mask;
    println!("frt: {:?}", front.to_be_bytes());
    front | back
}

/// Slices the bytes within the u128 selecting the range between the provided
/// `start` and `end`.
fn slice_bytes_u128(value: u128, start: usize, end: usize) -> u128 {
    debug_assert!(start < end);
    debug_assert!(end <= size_of::<u128>());

    const SLICE_MASK: u128 = 0xFF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF_FF;
    
    let slice_len = end - start;
    let back_mask = SLICE_MASK << ((size_of::<u128>() - slice_len) * BYTE);   
    let without_front = value << (start * BYTE);
    without_front & back_mask
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_u128_byte() {
        let value = u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

        assert_eq!(remove_byte_u128(value, 0).to_be_bytes(), [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]);
        assert_eq!(remove_byte_u128(value, 1).to_be_bytes(), [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]);
        assert_eq!(remove_byte_u128(value, 2).to_be_bytes(), [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]);
        assert_eq!(remove_byte_u128(value, 3).to_be_bytes(), [1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]);
        assert_eq!(remove_byte_u128(value, 15).to_be_bytes(), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0]);
    }

    #[test]
    fn test_slice_u128_bytes() {
        let value = u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

        assert_eq!(slice_bytes_u128(value, 0, 3).to_be_bytes(), [1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(slice_bytes_u128(value, 4, 8).to_be_bytes(), [5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(slice_bytes_u128(value, 0, 16).to_be_bytes(), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        assert_eq!(slice_bytes_u128(value, 15, 16).to_be_bytes(), [16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    }
    
}