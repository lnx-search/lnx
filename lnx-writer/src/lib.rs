#[macro_use]
extern crate tracing;

use std::io;
use std::path::Path;

mod clock;
mod finalizer;
mod indexer;
mod writer;

pub use clock::clock;
pub use finalizer::subscribe;
pub use writer::Writer;

/// 50MB default per thread.
const DEFAULT_BUFFER_SIZE: usize = 50 << 20;
const BUFFER_SIZE_WARN_THRESHOLD_PER_THREAD: usize = 250 << 20;
const BUFFER_HARD_CAP: u64 = 12 << 30;
const AUTO_COMMIT_DEFAULT: u64 = 30;

/// Initialise the writer global state.
pub async fn init(node_id: u32, tmp_path: &Path) {
    finalizer::init(tmp_path).await;
    clock::init(node_id).await;
}

#[derive(Debug, Copy, Clone)]
pub struct WriterSettings {
    pub auto_commit_duration: u64,
    pub num_threads: usize,
    pub memory_buffer_size: usize,
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

#[derive(
    Debug,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    utoipa::ToSchema,
)]
pub struct WriterSettingsSchema {
    #[serde(default = "WriterSettingsSchema::default_num_threads")]
    #[schema(default = "<calculated: num_logical_cores>", example = 8)]
    /// The number of threads the writer should use.
    ///
    /// If this is set to `None` it uses the number of cores available
    /// on the machine but is limited at upto 8 threads.
    pub num_threads: usize,

    #[serde(default, deserialize_with = "human_size_format::deserialize")]
    #[schema(
        default = "<calculated: 50MB * num_threads>",
        example = "100MB",
        value_type = String,
    )]
    /// The overall size of the memory pool to be used by the writer.
    ///
    /// The buffer is split across the number of indexing threads.
    ///
    /// Defaults to a 50MB allocation per thread.
    pub memory_buffer_size: Option<usize>,

    #[serde(default, deserialize_with = "human_time_format::deserialize")]
    #[schema(
        default = "30s",
        example = "1m 30s",
        value_type = String,
    )]
    /// The duration which should elapse before a segment is flushed and searchable.
    ///
    /// This duration must be greater than 1 second but less than 24 hours.
    pub auto_commit_duration: Option<u64>,
}

impl WriterSettingsSchema {
    fn default_num_threads() -> usize {
        std::cmp::min(num_cpus::get(), 8)
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

    use crate::BUFFER_HARD_CAP;

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

#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("Tantivy Error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("Failed to create new temporary indexing directory due to error: {0}")]
    TempDirectoryFailure(#[from] tantivy::directory::error::OpenDirectoryError),

    #[error("IO Error: {0}")]
    IoError(#[from] io::Error),

    #[error("{0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use utoipa::OpenApi;

    use super::*;

    #[test]
    fn test_utopia() {
        #[derive(OpenApi)]
        #[openapi(
            components(schemas(WriterSettingsSchema)),
            tags(
                (name = "todo", description = "Todo items management API")
            )
        )]
        struct ApiDoc;

        let doc = ApiDoc::openapi();
        println!("{}", doc.to_pretty_json().unwrap());
    }
}
