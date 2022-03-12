pub static INDEX_KEYSPACE_PREFIX: &str = "lnx_search";
pub static SEARCH_ENGINE_CONFIGURATION_KEYSPACE: &str = "lnx_engine_controller";
pub static TANTIVY_DATA_FOLDER: &str = "data";
pub static METADATA_FOLDER: &str = "meta";
pub static NODE_ID_KEY: &str = "node_id";

pub const TASK_BACKLOG_SIZE: usize = 10;
pub const NUM_SEGMENTS: i64 = 12505;
pub const SEGMENT_SIZE: i64 = 1475149466110320;
