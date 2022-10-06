use std::{io, mem};
use std::io::ErrorKind;
use rkyv::{Archive, Serialize, Deserialize};
use bytecheck::CheckBytes;

#[derive(Debug, Default, Archive, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct Deletes(Vec<String>);

impl Deletes {
    /// Loads a given metafile from a buffer.
    pub async fn from_compressed_bytes(buf: Vec<u8>) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let slice = buf.len() - mem::size_of::<u64>();

            let length_bytes: [u8; mem::size_of::<u64>()] = buf[slice..].try_into().unwrap();
            let length = u64::from_be_bytes(length_bytes) as usize;

            let buf = lz4_flex::decompress(&buf[8..], length)
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?;

            rkyv::from_bytes(&buf)
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))

        }).await.expect("spawn background thread")
    }

    /// Serializes the meta file to a buffer.
    pub async fn to_compressed_bytes(&self) -> io::Result<Vec<u8>> {
        let raw = rkyv::to_bytes::<_, 2048>(self)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?
            .into_vec();

        tokio::task::spawn_blocking(move || {
            let length_bytes = (raw.len() as u64).to_be_bytes();

            let mut buf = lz4_flex::compress(&raw);
            buf.extend_from_slice(&length_bytes);

            Ok(buf)
        }).await.expect("spawn background thread")
    }

    /// Merges another managed file with the current managed file.
    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0)
    }
}


