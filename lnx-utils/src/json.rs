use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait ToJSON: Serialize {
    fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self)
    }
}

pub trait FromJSON: DeserializeOwned {
    fn from_json(buff: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(buff)
    }
}

impl<T: Serialize> ToJSON for T {}
impl<T: DeserializeOwned> FromJSON for T {}
