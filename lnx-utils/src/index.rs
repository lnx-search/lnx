
pub fn index_id(name: &str) -> u64 {
    crc32fast::hash(name.as_ref()) as u64
}