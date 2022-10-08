use std::io::ErrorKind;
use std::{io, mem};

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, Default, Archive, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct Deletes(pub(crate) Vec<String>);

impl Deletes {
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

    let length_bytes: [u8; mem::size_of::<u64>()] =
        buf[slice..].try_into().unwrap();
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