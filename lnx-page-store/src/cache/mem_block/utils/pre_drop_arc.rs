//! This is a modification of Moka's MiniArc (https://github.com/moka-rs/moka), for use within
//! the page cache.
//!
//! It makes a few key modifications:
//!
//! - Code is brought up to using the latest stable Rust MSRV.
//! - A pre-drop hook is added in order to support the ticket GC system.
//! - Implements from_raw and into_raw calls.
//! - Implements [RefCnt].
//! - Removes some methods we don't use.
//!
//! Copy of Moka's MIT License:
//!
//! MIT License
//!
//! Copyright (c) 2020 - 2025 Tatsuya Kawano
//!
//! Permission is hereby granted, free of charge, to any person obtaining a copy
//! of this software and associated documentation files (the "Software"), to deal
//! in the Software without restriction, including without limitation the rights
//! to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//! copies of the Software, and to permit persons to whom the Software is
//! furnished to do so, subject to the following conditions:
//!
//! The above copyright notice and this permission notice shall be included in all
//! copies or substantial portions of the Software.
//!
//! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//! IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//! FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//! AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//! LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//! OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//! SOFTWARE.
//!
//! Original note from Moka:
//! >
//! > This module's source code was written by us, the `moka` developers, referring to
//! > the following book and code:
//! >
//! > - Chapter 6. Building Our Own "Arc" of the Rust Atomics and Locks book.
//! >     - Rust Atomics and Locks by Mara Bos (O’Reilly). Copyright 2023 Mara Bos,
//! >       ISBN: 978-1-098-11944-7
//! >     - https://marabos.nl/atomics/
//! > - The `triomphe` crate v0.1.13 and v0.1.11 by Manish Goregaokar (Manishearth)
//! >     - MIT or Apache-2.0 License
//! >     - https://github.com/Manishearth/triomphe
//! > - `std::sync::Arc` in the Rust Standard Library (1.81.0).
//! >     -  MIT or Apache-2.0 License

use std::alloc::Layout;
use std::hash::{Hash, Hasher};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{self, AtomicU32};
use std::{fmt, mem, ptr};

use arc_swap::{ArcSwapAny, RefCnt};

/// A type alias for [ArcSwapAny] and the [PreDropMiniArc].
pub type MiniArcSwap<T> = ArcSwapAny<PreDropMiniArc<T>>;

/// A thread-safe reference-counting pointer. `MiniArc` is similar to
/// `std::sync::Arc`, Atomically Reference Counted shared pointer, but with a few
/// differences:
///
/// - Smaller memory overhead:
///     - `MiniArc` does not support weak references, so it does not need to store a
///       weak reference count.
///     - `MiniArc` uses `AtomicU32` for the reference count, while `std::sync::Arc`
///       uses `AtomicUsize`. On a 64-bit system, `AtomicU32` is half the size of
///       `AtomicUsize`.
///         - Note: Depending on the value type `T`, the Rust compiler may add
///           padding to the internal struct of `MiniArc<T>`, so the actual memory
///           overhead may vary.
/// - Smaller code size:
///     - Only about 100 lines of code.
///         - This is because `MiniArc` provides only the methods needed for the
///           `moka` and `mini-moka` crates.
///     - Smaller code size means less chance of bugs.
pub(crate) struct PreDropMiniArc<T: ?Sized> {
    ptr: NonNull<ArcData<T>>,
}

#[repr(C)]
struct ArcData<T: ?Sized> {
    ref_count: AtomicU32,
    data: T,
}

impl<T: ?Sized> ArcData<T> {
    /// Compute the offset of the `data` field within `ArcData<T>`.
    ///
    /// # Safety
    ///
    /// - The pointer must be created from `PreDropMiniArc::into_raw` or similar functions
    /// - The pointee must be initialized (`&*value` must not be UB).
    ///   That happens automatically if the pointer comes from `Arc` and type was not changed.
    ///   This is **not** the case, for example, when `Arc` was uninitialized `MaybeUninit<T>`
    ///   and the pointer was cast to `*const T`.
    unsafe fn offset_of_data(value: *const T) -> usize {
        // We can use `Layout::for_value_raw` when it is stable.
        let value = unsafe { &*value };

        let layout = Layout::new::<AtomicU32>();
        let (_, offset) = layout.extend(Layout::for_value(value)).unwrap();
        offset
    }
}

/// A soft limit on the amount of references that may be made to an `MiniArc`.
///
/// Going above this limit will abort your program (although not necessarily)
/// at _exactly_ `MAX_REFCOUNT + 1` references.
const MAX_REFCOUNT: u32 = (i32::MAX) as u32;

unsafe impl<T: ?Sized + Send + Sync> Send for PreDropMiniArc<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for PreDropMiniArc<T> {}

impl<T> PreDropMiniArc<T> {
    pub(crate) fn new(data: T) -> PreDropMiniArc<T> {
        PreDropMiniArc {
            ptr: NonNull::from(Box::leak(Box::new(ArcData {
                ref_count: AtomicU32::new(1),
                data,
            }))),
        }
    }
}

impl<T: ?Sized> PreDropMiniArc<T> {
    #[cfg(test)]
    /// Gets the number of [`PreDropMiniArc`] pointers to this allocation
    fn count(this: &Self) -> u32 {
        use atomic::Ordering::Acquire;
        this.data().ref_count.load(Acquire)
    }

    /// Returns `true` if the two `MiniArc`s point to the same allocation in a
    /// vein similar to [`ptr::eq`].
    ///
    /// # Safety
    ///
    /// This function is unreliable when `T` is a `dyn Trait`. Currently
    /// coercing `MiniArc<SomeTime>` to `MiniArc<dyn Trait>` is not possible, so
    /// this is not a problem in practice. However, if this coercion becomes
    /// possible in the future, this function may return incorrect results when
    /// comparing `MiniArc<dyn Trait>` instances.
    ///
    /// To fix this, we must rise the minimum supported Rust version (MSRV) to
    /// 1.76 and use `std::ptr::addr_eq` internally instead of `eq` (`==`).
    /// `addr_eq` compares the _addresses_ of the pointers for equality,
    /// ignoring any metadata in fat pointers.
    ///
    /// See the following `triomphe` issue for more information:
    /// https://github.com/Manishearth/triomphe/pull/84
    ///
    /// Note that `triomphe` has a feature called `unsize`, which enables the
    /// coercion by using the `unsize` crate. `MiniArc` does not have such a
    /// feature, so we are safe for now.
    #[inline]
    fn ptr_eq(this: &Self, other: &Self) -> bool {
        ptr::addr_eq(this.ptr.as_ptr(), other.ptr.as_ptr())
    }

    #[inline]
    fn data(&self) -> &ArcData<T> {
        unsafe { self.ptr.as_ref() }
    }

    #[inline]
    fn into_raw(self) -> *const T {
        let this = ManuallyDrop::new(self);
        this.as_ptr()
    }

    #[inline]
    fn as_ptr(&self) -> *const T {
        // SAFETY: This cannot go through a reference to `data`, because this method
        // is used to implement `into_raw`. To reconstruct the full `Arc` from this
        // pointer, it needs to maintain its full provenance, and not be reduced to
        // just the contained `T`.
        unsafe { ptr::addr_of_mut!((*self.ptr()).data) }
    }

    #[inline]
    fn ptr(&self) -> *mut ArcData<T> {
        self.ptr.as_ptr()
    }

    /// Reconstruct the `PreDropMiniArc<T>` from a raw pointer obtained from into_raw()
    ///
    /// Note: This raw pointer will be offset in the allocation and must be preceded
    /// by the atomic count.
    ///
    /// It is recommended to use OffsetArc for this
    ///
    ///  # Safety
    /// - The given pointer must be a valid pointer to `T` that came from [`Arc::into_raw`].
    /// - After `from_raw`, the pointer must not be accessed.
    #[inline]
    unsafe fn from_raw(ptr: *const T) -> Self {
        // To find the corresponding pointer to the `ArcInner` we need
        // to subtract the offset of the `data` field from the pointer.
        unsafe {
            // SAFETY: `ptr` comes from `ArcInner.data`, so it must be initialized.
            let offset_of_data = ArcData::<T>::offset_of_data(ptr);

            // SAFETY: `from_raw_inner` expects a pointer to the beginning of the allocation,
            //   not a pointer to data part.
            //  `ptr` points to `ArcInner.data`, so subtraction results
            //   in the beginning of the `ArcInner`, which is the beginning of the allocation.
            let arc_inner_ptr = ptr.byte_sub(offset_of_data);
            PreDropMiniArc::from_raw_inner(arc_inner_ptr as *mut ArcData<T>)
        }
    }

    #[inline]
    /// Construct an `PreDropMiniArc` from an allocated `ArcData`.
    /// # Safety
    /// The `ptr` must point to a valid instance, allocated by an `PreDropMiniArc`.
    unsafe fn from_raw_inner(ptr: *mut ArcData<T>) -> Self {
        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
        }
    }
}

impl<T: ?Sized> Deref for PreDropMiniArc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data().data
    }
}

impl<T: ?Sized> Clone for PreDropMiniArc<T> {
    fn clone(&self) -> Self {
        use atomic::Ordering::Relaxed;

        if self.data().ref_count.fetch_add(1, Relaxed) > MAX_REFCOUNT {
            std::process::abort();
        }

        PreDropMiniArc { ptr: self.ptr }
    }
}

unsafe impl<T: Sized> RefCnt for PreDropMiniArc<T> {
    type Base = T;

    fn into_ptr(me: Self) -> *mut Self::Base {
        Self::into_raw(me) as *mut T
    }

    fn as_ptr(me: &Self) -> *mut Self::Base {
        // Mirrored from RefCnt in ArcSwap:
        //
        // Slightly convoluted way to do this, but this avoids stacked borrows violations. The same
        // intention as
        //
        // me as &T as *const T as *mut T
        //
        // We first create a "shallow copy" of me - one that doesn't really own its ref count
        // (that's OK, me _does_ own it, so it can't be destroyed in the meantime).
        // Then we can use into_raw (which preserves not having the ref count).
        //
        // We need to "revert" the changes we did. In current std implementation, the combination
        // of from_raw and forget is no-op. But formally, into_raw shall be paired with from_raw
        // and that read shall be paired with forget to properly "close the brackets". In future
        // versions of STD, these may become something else that's not really no-op (unlikely, but
        // possible), so we future-proof it a bit.

        // SAFETY: &T cast to *const T will always be aligned, initialised and valid for reads
        let ptr = Self::into_raw(unsafe { ptr::read(me) });
        let ptr = ptr as *mut T;

        // SAFETY: We got the pointer from into_raw just above
        mem::forget(unsafe { Self::from_raw(ptr) });

        ptr
    }

    unsafe fn from_ptr(ptr: *const Self::Base) -> Self {
        unsafe { Self::from_raw(ptr) }
    }
}

impl<T: ?Sized> Drop for PreDropMiniArc<T> {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering::{Acquire, Release};

        if self.data().ref_count.fetch_sub(1, Release) == 1 {
            atomic::fence(Acquire);
            unsafe {
                drop(Box::from_raw(self.ptr.as_ptr()));
            }
        }
    }
}

impl<T: Default> Default for PreDropMiniArc<T> {
    /// Creates a new `MiniArc<T>`, with the `Default` value for `T`.
    fn default() -> PreDropMiniArc<T> {
        PreDropMiniArc::new(Default::default())
    }
}

impl<T: ?Sized + PartialEq> PartialEq for PreDropMiniArc<T> {
    fn eq(&self, other: &PreDropMiniArc<T>) -> bool {
        // TODO: pointer equality is incorrect if `T` is not `Eq`.
        // See: https://github.com/Manishearth/triomphe/pull/88
        Self::ptr_eq(self, other) || *(*self) == *(*other)
    }

    #[allow(clippy::partialeq_ne_impl)]
    fn ne(&self, other: &PreDropMiniArc<T>) -> bool {
        !Self::ptr_eq(self, other) && *(*self) != *(*other)
    }
}

impl<T: ?Sized + Eq> Eq for PreDropMiniArc<T> {}

impl<T: ?Sized + fmt::Display> fmt::Display for PreDropMiniArc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for PreDropMiniArc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized> fmt::Pointer for PreDropMiniArc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Pointer::fmt(&self.ptr.as_ptr(), f)
    }
}

impl<T: ?Sized + Hash> Hash for PreDropMiniArc<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;

    use super::*;

    #[test]
    fn test_drop() {
        static NUM_DROPS: AtomicUsize = AtomicUsize::new(0);

        struct DetectDrop;

        impl Drop for DetectDrop {
            fn drop(&mut self) {
                NUM_DROPS.fetch_add(1, Relaxed);
            }
        }

        // Create two MiniArcs sharing an object containing a string
        // and a DetectDrop, to detect when it is dropped.
        let x = PreDropMiniArc::new(("hello", DetectDrop));
        let y = x.clone();

        // Send x to another thread, and use it there.
        let t = std::thread::spawn(move || {
            assert_eq!(x.0, "hello");
        });

        // In parallel, y should still be usable here.
        assert_eq!(y.0, "hello");
        assert!(PreDropMiniArc::count(&y) >= 1);

        // Wait for the thread to finish.
        t.join().unwrap();

        // One MiniArc, x, should be dropped by now.
        // We still have y, so the object should not have been dropped yet.
        assert_eq!(NUM_DROPS.load(Relaxed), 0);
        assert_eq!(PreDropMiniArc::count(&y), 1);

        // Drop the remaining `MiniArc`.
        drop(y);

        // Now that `y` is dropped too,
        // the object should have been dropped.
        assert_eq!(NUM_DROPS.load(Relaxed), 1);
    }

    #[test]
    fn test_eq() {
        let w = PreDropMiniArc::new(6502);
        let x = w.clone();
        let y = PreDropMiniArc::new(6502);
        let z = PreDropMiniArc::new(8086);

        assert_eq!(w, x);
        assert_eq!(x, w);
        assert_eq!(w, y);
        assert_eq!(y, w);
        assert_ne!(y, z);
        assert_ne!(z, y);
    }

    #[test]
    fn test_partial_eq_bug() {
        let float = f32::NAN;
        assert_ne!(float, float);
        let arc = PreDropMiniArc::new(f32::NAN);
        // TODO: this is a bug.
        // See: https://github.com/Manishearth/triomphe/pull/88
        assert_eq!(arc, arc);
    }

    #[test]
    fn test_arc_swap_impl() {
        let arc1 = PreDropMiniArc::new(1234);
        let arc2 = PreDropMiniArc::new(123);

        let swap = MiniArcSwap::new(arc1);
        let replaced = swap.swap(arc2);
        let loaded = swap.load();

        assert_eq!(*replaced, 1234);
        assert_eq!(**loaded, 123);
    }

    #[allow(dead_code)]
    const fn is_partial_eq<T: ?Sized + PartialEq>() {}

    #[allow(dead_code)]
    const fn is_eq<T: ?Sized + Eq>() {}

    // compile-time check that PartialEq/Eq is correctly derived
    const _: () = is_partial_eq::<PreDropMiniArc<i32>>();
    const _: () = is_eq::<PreDropMiniArc<i32>>();
}
