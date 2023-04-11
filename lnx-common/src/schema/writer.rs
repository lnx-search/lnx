use std::cmp;

/// 50MB default per thread.
const DEFAULT_BUFFER_SIZE: usize = 50 << 20;
const BUFFER_SIZE_WARN_THRESHOLD_PER_THREAD: usize = 250 << 20;
const BUFFER_HARD_CAP: u64 = 12 << 30;
const AUTO_COMMIT_DEFAULT: u64 = 30;

#[derive(
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[serde(from = "WriterSettingsSchema")]
pub struct WriterSettings {
    /// The duration of inactivity to elapse before flushing a segment.
    pub auto_commit_duration: u64,

    /// The number of indexing threads to use.
    pub num_threads: usize,

    /// The size of the memory buffer to use in total across the number of indexing threads.
    pub memory_buffer_size: usize,
}

impl Default for WriterSettings {
    fn default() -> Self {
        let threads = cmp::min(num_cpus::get(), 8);
        Self {
            auto_commit_duration: AUTO_COMMIT_DEFAULT,
            num_threads: threads,
            memory_buffer_size: DEFAULT_BUFFER_SIZE * threads,
        }
    }
}

impl From<WriterSettingsSchema> for WriterSettings {
    fn from(schema: WriterSettingsSchema) -> Self {
        Self {
            auto_commit_duration: schema.auto_commit_duration.unwrap_or(AUTO_COMMIT_DEFAULT),
            num_threads: schema.num_threads,
            memory_buffer_size: schema.memory_buffer_size
                .map(|size| {
                    let required_size = DEFAULT_BUFFER_SIZE * schema.num_threads;
                    let per_thread_size = size / schema.num_threads;

                    let total_pretty = humansize::format_size(size, humansize::DECIMAL);
                    let per_thread_pretty = humansize::format_size(per_thread_size, humansize::DECIMAL);

                    if size < required_size {
                        warn!(
                            total_buffer_size = %total_pretty,
                            per_thread_buffer_size = %per_thread_pretty,
                            "Total buffer size does not meet the minimum required size per thread. \
                            Using default value."
                        );

                        return required_size;
                    }

                    if per_thread_size > BUFFER_SIZE_WARN_THRESHOLD_PER_THREAD {
                        warn!(
                            total_buffer_size = %total_pretty,
                            per_thread_buffer_size = %per_thread_pretty,
                            "The per-thread buffer size allocated is likely larger than required. \
                            Recommend maximum per-thread is 500MB.",
                        )
                    }

                    size
                })
                .unwrap_or(DEFAULT_BUFFER_SIZE * schema.num_threads),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WriterSettingsSchema {
    #[serde(default = "WriterSettingsSchema::default_num_threads")]
    /// The number of threads the writer should use.
    ///
    /// If this is not specified, it uses the number of cores available
    /// on the machine but is limited at upto 8 threads.
    pub num_threads: usize,

    #[serde(default, deserialize_with = "human_size_format::deserialize")]
    /// The overall size of the memory pool to be used by the writer.
    ///
    /// The buffer is split across the number of indexing threads.
    ///
    /// Defaults to a 50MB allocation per thread.
    pub memory_buffer_size: Option<usize>,

    #[serde(default, deserialize_with = "human_time_format::deserialize")]
    /// The duration which should elapse before a segment is flushed and searchable.
    ///
    /// This duration must be greater than 1 second but less than 24 hours.
    pub auto_commit_duration: Option<u64>,
}

impl Default for WriterSettingsSchema {
    fn default() -> Self {
        Self {
            num_threads: cmp::min(num_cpus::get(), 8),
            memory_buffer_size: None,
            auto_commit_duration: None,
        }
    }
}

impl WriterSettingsSchema {
    fn default_num_threads() -> usize {
        cmp::min(num_cpus::get(), 8)
    }
}

mod human_time_format {
    use serde::de::Error;
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let dur = s
            .parse::<humantime::Duration>()
            .map_err(Error::custom)?
            .as_secs();

        Ok(Some(dur))
    }
}

mod human_size_format {
    use serde::de::Error;
    use serde::{Deserialize, Deserializer};

    use super::BUFFER_HARD_CAP;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let n = parse_size::parse_size(&s).map_err(Error::custom)?;

        if n > BUFFER_HARD_CAP {
            return Err(Error::custom(format!(
                "Size {:?} exceeded maximum buffer limit of 12GB",
                s
            )));
        }

        Ok(Some(n as usize))
    }
}
