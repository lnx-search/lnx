pub use bincode::{Decode, Encode};

pub trait ToBytes: Encode + Sized {
    #[instrument(name = "binary-serializer", skip(self))]
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::encode_to_vec(self, bincode::config::standard()).map_err(|e| {
            error!("Failed to complete serialization {}", e);
            e
        })
    }
}

pub trait FromBytes: Decode + Sized {
    #[instrument(name = "binary-deserializer", skip(buff))]
    fn from_bytes(buff: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        let (slf, _) = bincode::decode_from_slice(buff, bincode::config::standard())
            .map_err(|e| {
                error!("Failed to complete deserialization {}", e);
                e
            })?;

        Ok(slf)
    }
}

impl<T: Encode + Sized> ToBytes for T {}
impl<T: Decode + Sized> FromBytes for T {}
