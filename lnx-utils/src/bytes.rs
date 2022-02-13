use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait ToBytes: Serialize {
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(&self)
    }
}

pub trait FromBytes: DeserializeOwned {
    fn from_bytes(buff: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(buff)
    }
}

impl<T: Serialize> ToBytes for T {}
impl<T: DeserializeOwned> FromBytes for T {}
