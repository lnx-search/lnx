use super::primary_store::ScyllaPrimaryDataStore;


pub struct ScyllaBackend {
    local_store: sled::Db,
    doc_store: ScyllaPrimaryDataStore,
}

