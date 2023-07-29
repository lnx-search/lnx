use std::time::Duration;

/// A clock which produces `Instant`s to when a commit
/// should next happen.
pub struct CommitClock {

}

impl CommitClock {
    /// Create a new commit clock that triggers every `dur` points in time.
    pub fn every(dur: Duration) -> Self {

    }
}