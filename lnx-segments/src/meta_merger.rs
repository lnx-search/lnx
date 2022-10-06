use std::cmp;

use serde_json::Value;

#[derive(Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct MetaFile {
    pub index_settings: Value,
    pub segments: Vec<Value>,
    pub schema: Value,
    pub opstamp: u32,
}

impl MetaFile {
    /// Loads a given metafile from a JSON buffer.
    pub fn from_json(buf: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(buf)
    }

    /// Serializes the meta file to a JSON buffer.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Merges another meta file with the current meta file.
    ///
    /// The schema and index settings must be the same.
    pub fn merge(&mut self, other: Self) {
        if self.schema == Value::Null {
            self.schema = other.schema;
        }

        if self.index_settings == Value::Null {
            self.index_settings = other.index_settings;
        }

        self.segments.extend(other.segments);
        self.opstamp = cmp::max(self.opstamp, other.opstamp);
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct ManagedMeta(Vec<String>);

impl ManagedMeta {
    /// Loads a given metafile from a JSON buffer.
    pub fn from_json(buf: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(buf)
    }

    /// Serializes the meta file to a JSON buffer.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Merges another managed file with the current managed file.
    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0)
    }
}
