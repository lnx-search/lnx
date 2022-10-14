use std::io::ErrorKind;
use std::ops::{Deref, DerefMut};
use std::{io, mem};

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes, Debug))]
pub enum DeleteValue {
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct Delete {
    pub field: String,
    pub value: DeleteValue,
}

#[derive(Debug, Default, Archive, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct Deletes(pub Vec<Delete>);

impl Deref for Deletes {
    type Target = Vec<Delete>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Deletes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deletes {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    /// The estimated amount of space this structure will take up
    /// on disk.
    pub fn estimated_size_on_disk(&self) -> usize {
        rkyv::to_bytes::<_, 2048>(self)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Loads a given metafile from a buffer.
    ///
    /// This is compatible with both the glommio and tokio runtime contexts.
    pub async fn from_compressed_bytes(buf: Vec<u8>) -> io::Result<Self> {
        let (tx, rx) = oneshot::channel();

        std::thread::spawn(move || {
            let res = decompress_data(buf);
            let _ = tx.send(res);
        });

        rx.await.expect("Failed to spawn background thread")
    }

    /// Serializes the meta file to a buffer.
    ///
    /// This is compatible with both the glommio and tokio runtime contexts.
    pub async fn to_compressed_bytes(&self) -> io::Result<Vec<u8>> {
        let buf = rkyv::to_bytes::<_, 2048>(self)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?
            .into_vec();

        let (tx, rx) = oneshot::channel();

        std::thread::spawn(move || {
            let res = compress_data(buf);
            let _ = tx.send(res);
        });

        rx.await.expect("Failed to spawn background thread")
    }

    /// Merges another managed file with the current managed file.
    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0)
    }
}

fn decompress_data(buf: Vec<u8>) -> io::Result<Deletes> {
    let slice = buf.len() - mem::size_of::<u64>();

    let length_bytes: [u8; mem::size_of::<u64>()] = buf[slice..].try_into().unwrap();
    let length = u64::from_be_bytes(length_bytes) as usize;

    let buf = lz4_flex::decompress(&buf[..slice], length)
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?;

    rkyv::from_bytes(&buf)
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))
}

fn compress_data(buf: Vec<u8>) -> io::Result<Vec<u8>> {
    let length_bytes = (buf.len() as u64).to_be_bytes();

    let mut buf = lz4_flex::compress(&buf);
    buf.extend_from_slice(&length_bytes);

    Ok(buf)
}
