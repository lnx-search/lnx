-- The metadata for the indexes themselves
CREATE TABLE IF NOT EXISTS lnx__indexes (
    index_id TEXT PRIMARY KEY,
    field_mapping TEXT,
    search_settings TEXT,
    last_modified BIGINT
);

-- The metadata store for the distributed file system.
CREATE TABLE IF NOT EXISTS lnx__dfs_metadata (
     file_id BIGINT PRIMARY KEY,
     file_name TEXT,
     seed_nodes TEXT,
     created_at BIGINT
);

-- Node information and discovery
CREATE TABLE IF NOT EXISTS lnx__nodes (
      node_id TEXT PRIMARY KEY,
      generation BIGINT,
      system_info TEXT,
      broadcast_address TEXT,
      connected_at BIGINT
);