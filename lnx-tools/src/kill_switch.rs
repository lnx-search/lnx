use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Default)]
/// An atomic flag which automatically kills what ever it is attached to
/// once dropped.
pub struct KillSwitch {
    flag: Arc<AtomicBool>,
}

impl KillSwitch {
    #[inline]
    /// Creates a new kill switch watcher.
    pub fn watcher(&self) -> KillSwitchWatcher {
        KillSwitchWatcher {
            flag: self.flag.clone(),
        }
    }

    #[inline]
    /// Set the kill switch flag to true.
    pub fn kill(&self) {
        self.flag.store(true, Ordering::Relaxed)
    }

    #[inline]
    /// Returns if the flag has been set to true (killed).
    pub fn is_killed(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}

impl Drop for KillSwitch {
    fn drop(&mut self) {
        self.kill();
    }
}

#[derive(Clone)]
/// A handle to a kill switch.
pub struct KillSwitchWatcher {
    flag: Arc<AtomicBool>,
}

impl KillSwitchWatcher {
    #[inline]
    /// Returns if the flag has been set to true (killed).
    pub fn is_killed(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}
