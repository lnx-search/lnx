use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The unix timestamp in seconds which the commit clock starts from.
///
/// This is a UTC timestamp.
const START_POINT_IN_TIME: u64 = 1690649400;

#[derive(Clone)] // This can implement `Copy` but chooses not to in order to avoid confusion.
/// A clock which produces `Instant`s to when a commit
/// should next happen.
pub struct CommitClock {
    interval: Duration,
    next_commit_at_utc: u64,
}

impl CommitClock {
    /// Create a new commit clock that triggers every `dur` points in time.
    pub fn every(interval: Duration) -> Self {
        let now_utc_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self::for_timestamp_every(now_utc_unix, interval)
    }

    fn for_timestamp_every(timestamp: u64, interval: Duration) -> Self {
        let next_commit_at_utc = Self::next_commit_at_from_ts(timestamp, interval);
        Self {
            interval,
            next_commit_at_utc,
        }
    }

    fn next_commit_at_from_ts(timestamp: u64, interval: Duration) -> u64 {
        let delta = timestamp - START_POINT_IN_TIME;
        let num_past_commits = delta / interval.as_secs();
        let next_commit_at_delta = (num_past_commits + 1) * interval.as_secs();
        START_POINT_IN_TIME + next_commit_at_delta
    }

    fn get_next_commit_timestamp(&mut self, now_timestamp: u64) -> u64 {
        let timestamp = if now_timestamp > self.next_commit_at_utc {
            Self::next_commit_at_from_ts(now_timestamp, self.interval)
        } else {
            self.next_commit_at_utc
        };

        self.next_commit_at_utc = timestamp + self.interval.as_secs();
        timestamp
    }

    #[inline]
    /// The time between commit points.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Gets the next amount of time until the next commit point.
    pub fn next(&mut self) -> Duration {
        let now_utc_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let timestamp = self.get_next_commit_timestamp(now_utc_unix);

        let delta = timestamp - now_utc_unix;
        Duration::from_secs(delta)
    }

    /// Checks if the caller has missed the next commit point
    /// since it was calculated.
    pub fn has_missed_next_window(&self) -> bool {
        let now_utc_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        now_utc_unix > self.next_commit_at_utc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_gets_correct_steps() {
        let mut clock = CommitClock::for_timestamp_every(
            START_POINT_IN_TIME,
            Duration::from_secs(30),
        );

        assert_eq!(
            clock.get_next_commit_timestamp(START_POINT_IN_TIME),
            START_POINT_IN_TIME + 30
        );
        assert_eq!(
            clock.get_next_commit_timestamp(START_POINT_IN_TIME + 30),
            START_POINT_IN_TIME + 60
        );
        assert_eq!(
            clock.get_next_commit_timestamp(START_POINT_IN_TIME + 60),
            START_POINT_IN_TIME + 90
        );

        // Our clock will advance regardless of if our timestamp is lower.
        let mut clock = CommitClock::for_timestamp_every(
            START_POINT_IN_TIME,
            Duration::from_secs(30),
        );
        assert_eq!(
            clock.get_next_commit_timestamp(START_POINT_IN_TIME),
            START_POINT_IN_TIME + 30
        );
        assert_eq!(
            clock.get_next_commit_timestamp(START_POINT_IN_TIME),
            START_POINT_IN_TIME + 60
        );
    }

    #[test]
    fn test_consistent_triggers() {
        let clock = CommitClock::every(Duration::from_secs(1));

        let mut tasks = Vec::new();
        for _ in 0..3 {
            let mut clock = clock.clone();
            let thread = std::thread::spawn(move || {
                let delta = clock.next();
                std::thread::sleep(delta);
                let delta = clock.next();
                std::thread::sleep(delta);
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            });

            tasks.push(thread);
        }

        let times = tasks
            .into_iter()
            .map(|handle| handle.join())
            .collect::<Result<Vec<_>, _>>()
            .expect("Join threads");

        let first = times[0];
        if !times.iter().all(|el| *el == first) {
            panic!("All threads should trigger at the same time {times:?}");
        }
    }
}
