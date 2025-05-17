use std::sync::atomic::Ordering;

use super::flags::{AtomicPageFlags, PageFlags};

#[derive(Default, Debug)]
pub(super) struct PageStateEntry {
    flags: AtomicPageFlags,
    lock: parking_lot::Mutex<()>,
}

impl PageStateEntry {
    /// Load the current page flags with [Ordering::Relaxed] ordering.
    pub(super) fn flags(&self) -> PageFlags {
        self.flags.load(Ordering::Relaxed)
    }
}
