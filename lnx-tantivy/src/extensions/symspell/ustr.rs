use std::cmp;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

#[derive(Copy, Clone, Default, Eq, PartialEq)]
/// A fixed sized 16-byte long byte slice
pub struct UBytes {
    value: u128,
    len: u8,
}

impl AsRef<[u8]> for UBytes {
    fn as_ref(&self) -> &[u8] {
        let slice: &[u8; size_of::<u128>()] = bytemuck::cast_ref(&self.value);
        &slice[..self.len as usize]
    }
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
    
    #[inline]
    /// Returns if self starts with the bytes `prefix`.
    pub fn startswith(&self, prefix: Self) -> bool {
        startswith_bytes_u128(self.value, prefix.value, prefix.len())
    }

    #[inline]
    /// Returns if self ends with the bytes `suffix`.
    pub fn endswith(&self, suffix: Self) -> bool {
        endswith_bytes_u128(self.value, self.len(), suffix.value, suffix.len())
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

impl PartialOrd for UBytes {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UBytes {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.to_be_bytes()
            .cmp(&other.value.to_be_bytes())
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

    // Calculate the mask to apply to the original value to extract
    // the front remaining bytes.
    let front_mask = !(FULL_MASK >> (BYTE * pos));
    let front = value & front_mask;

    // Shift the value from the position being removed to zero out the front leaving
    // us with just the back of the bytes.
    let mut back = value;
    back <<= BYTE * ((pos + 1) % 16);
    back >>= BYTE * pos;
    back >>= BYTE * (pos == 15) as usize;

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

/// Returns if the `value` starts with the given `prefix`.
fn startswith_bytes_u128(value: u128, prefix: u128, prefix_len: usize) -> bool {
    let shift_right_by = BYTE * (size_of::<u128>() - prefix_len);
    let left = value.wrapping_shr(shift_right_by as u32);
    let right = prefix.wrapping_shr(shift_right_by as u32);
    left == right || prefix_len == 0    
}

/// Returns if the `value` ends with the given `suffix`.
fn endswith_bytes_u128(value: u128, value_len: usize, suffix: u128, prefix_len: usize) -> bool {
    let shift_left_by = BYTE * (value_len - prefix_len);
    let left = value.wrapping_shl(shift_left_by as u32);
    left == suffix || prefix_len == 0
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

    #[test]
    fn test_startswith_u128_bytes() {
        let value = u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 12, 13, 14, 15, 16]);

        assert!(
            startswith_bytes_u128(
                value, 
                u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0]), 
                7,
            )
        );
        assert!(
            startswith_bytes_u128(
                value,
                u128::from_be_bytes([1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                3,
            )
        );
        assert!(
            startswith_bytes_u128(
                value,
                u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 12, 0, 0, 0, 0]),
                12,
            )
        );
        assert!(
            startswith_bytes_u128(
                value,
                u128::from_be_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                0,
            )
        );
        assert!(
            !startswith_bytes_u128(
                value,
                u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0]),
                8,
            )
        );
    }

    #[test]
    fn test_endswith_u128_bytes() {
        let value = u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 12, 13, 14, 15, 16]);

        assert!(
            endswith_bytes_u128(
                value,
                16,
                u128::from_be_bytes([15, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                2,
            )
        );
        assert!(
            endswith_bytes_u128(
                value,
                16,
                u128::from_be_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                0,
            )
        );
        assert!(
            endswith_bytes_u128(
                value,
                16,
                u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 12, 13, 14, 15, 16]),
                16,
            )
        );
        assert!(
            !endswith_bytes_u128(
                value,
                16,
                u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0]),
                8,
            )
        );
        
        let value = u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(
            endswith_bytes_u128(
                value,
                7,
                u128::from_be_bytes([5, 6, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                3,
            )
        );
    }
    
    #[test]
    fn test_ubytes_ordering() {
        let mut sample: Vec<&'static [u8]> = vec![
            b"ello", 
            b"hell", 
            b"helo", 
            b"hllo",
            b"hello",
        ];
        sample.sort();
        
        let to_compare_against = sample.into_iter()
            .map(UBytes::from_slice_prefix)
            .collect::<Vec<_>>();
        
        let mut values: Vec<UBytes> = vec![
            UBytes::from_slice_prefix(b"ello"),
            UBytes::from_slice_prefix(b"hell"),
            UBytes::from_slice_prefix(b"helo"),
            UBytes::from_slice_prefix(b"hllo"),
            UBytes::from_slice_prefix(b"hello"),
        ];
        values.sort();
        
        dbg!(&values);
           
        assert_eq!(values, to_compare_against);
    }
}