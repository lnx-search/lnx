use std::mem;
use std::sync::Arc;

/// A helper type for having a single value on the stack of a heap allocated
/// value in an Arc.
///
/// The single value can be converted to a shared value and then cached.
pub(super) enum SingleOrShared<T> {
    #[doc(hidden)]
    None,
    Single(T),
    Shared(Arc<T>),
}

impl<T> SingleOrShared<T> {
    #[inline]
    /// Converts the `Single` variant of this type into the `Shared` variant,
    /// or if the value is already `Shared`, return a clone of the inner arc.
    pub fn share(&mut self) -> Arc<T> {
        let guard = mem::replace(self, SingleOrShared::None);
        match guard {
            SingleOrShared::None => unreachable!("variant should never be hit"),
            SingleOrShared::Single(single) => {
                let shared = Arc::new(single);
                *self = SingleOrShared::Shared(shared.clone());
                shared
            },
            SingleOrShared::Shared(shared) => {
                *self = SingleOrShared::Shared(shared.clone());
                shared
            },
        }
    }
}

pub(super) fn align_up(value: usize, align: usize) -> usize {
    value.div_ceil(align) * align
}

pub(super) fn align_down(value: usize, align: usize) -> usize {
    (value / align) * align
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_up() {
        assert_eq!(align_up(0, 4), 0);
        assert_eq!(align_up(1, 4), 4);
        assert_eq!(align_up(2, 4), 4);
        assert_eq!(align_up(3, 4), 4);
        assert_eq!(align_up(4, 4), 4);
    }

    #[test]
    fn test_align_down() {
        assert_eq!(align_down(0, 4), 0);
        assert_eq!(align_down(1, 4), 0);
        assert_eq!(align_down(4, 4), 4);
        assert_eq!(align_down(5, 4), 4);
    }
}
