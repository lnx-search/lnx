pub static INDEX_KEYSPACE_PREFIX: &str = "lnx_search";
pub static SEARCH_ENGINE_CONFIGURATION_KEYSPACE: &str = "lnx_engine_controller";
pub static TANTIVY_DATA_FOLDER: &str = "data";
pub static METADATA_FOLDER: &str = "meta";
pub static NODE_ID_KEY: &str = "node_id";

pub const TASK_BACKLOG_SIZE: usize = 10;

/// The number of segments the system will split the documents into.
///
/// Note:
///     This is actually slightly deceptive but this is only for 0 -> 2^63  -1
///     there is also another 12505 segments for the negative bound.
///
///     This just comes from the unfortunate side affect of Scylla using i64s
///     with it's tokens.
pub const NUM_SEGMENTS: i64 = 12505;

/// The total number of documents that *could* be contained within a segment.
///
/// This is calculated based off of the total numeric range / number of segments
/// Although this is a very large number, documents are not stored sequentially,
/// so they will be evenly distributed across the ~30k segments. In theory, for
/// larger datasets having more segments would be beneficial however, the more
/// segments the slower the system is to detect changes.
pub const SEGMENT_SIZE: i64 = 1475149466110320;
