use std::hash::Hasher;
use std::io::{ErrorKind, Write};
use std::{io, mem};

use rkyv::ser::Serializer;

use crate::serializer::{
    DocSerializer,
    DocSerializerError, 
    DocWriteSerializer,
};

/// The default amount of stack scratch space to allocate.
pub const DEFAULT_SCRATCH_SPACE: usize = 1 << 10;

/// A document encoder that writes to a given output writer.
///
/// This encoder allocates 1KB of stack space for serializing.
pub struct Encoder<W: Write, const N: usize = DEFAULT_SCRATCH_SPACE> {
    writer: DocSerializer<N, DocWriteSerializer<ChecksumAndLenWriter<W>>>,
}

impl<W: Write, const N: usize> Encoder<W, N> {
    #[inline]
    /// Create a new document encoder.
    pub fn new(writer: W) -> Self {
        Self {
            writer: DocSerializer::new(DocWriteSerializer::new(
                ChecksumAndLenWriter::new(writer),
            )),
        }
    }

    #[inline]
    /// Encode a document and write the output to the writer.
    pub fn encode(&mut self, document: &crate::Document) -> io::Result<()> {
        let res = self.writer.serialize_value(document);

        let writer = self.writer.inner_mut().writer_mut();

        if res.is_err() {
            writer.reset();
        }

        match res {
            Ok(_) => {
                let res = writer.write_footer();
                writer.reset();
                res
            },
            Err(DocSerializerError::Serializer(e)) => Err(e),
            Err(other) => Err(io::Error::new(ErrorKind::InvalidData, other.to_string())),
        }
    }

    #[inline]
    /// Return a reference to the given writer.
    pub fn writer(&self) -> &W {
        self.writer.inner().writer().inner()
    }

    #[inline]
    /// Consumer the encoder and return the inner writer.
    pub fn into_writer(self) -> W {
        self.writer
            .into_inner_serializer() // Composite serializer
            .into_inner() // Writer serializer
            .into_inner() // Inner writer
    }
}

/// A helper wrapper that calculates the checksum
/// of the resulting document and it's length.
pub struct ChecksumAndLenWriter<W> {
    writer: W,
    length: usize,
    checksum_hasher: crc32fast::Hasher,
}

impl<W: Write> ChecksumAndLenWriter<W> {
    /// Creates a new checksum writer.
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            length: 0,
            checksum_hasher: crc32fast::Hasher::new(),
        }
    }

    #[inline]
    /// Get a reference to the inner writer.
    pub fn inner(&self) -> &W {
        &self.writer
    }

    #[inline]
    /// Reset the checksum state.
    pub fn reset(&mut self) {
        self.length = 0;
        self.checksum_hasher.reset();
    }

    #[inline]
    /// Appends the checksum and len to the end of the writer.
    ///
    /// The footer is written in the format of:
    /// | length(4 bytes) | checksum(4 bytes) |
    pub fn write_footer(&mut self) -> io::Result<()> {
        let checksum = mem::take(&mut self.checksum_hasher).finalize();
        self.writer.write_all(&(self.length as u32).to_le_bytes())?;
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.reset();
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> Write for ChecksumAndLenWriter<W> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.checksum_hasher.write(buf);
        self.length += n;
        Ok(n)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

// #[cfg(test)]
// mod tests {
//     use rkyv::AlignedVec;
//
//     use super::*;
//     use crate::{Document, Text, Value};
//
//     #[test]
//     fn test_encoder() {
//         let mut writer = AlignedVec::new();
//         let mut encoder = Encoder::<_, DEFAULT_SCRATCH_SPACE>::new(&mut writer);
//
//         let mut document = Document::default();
//         document.insert("bobby", Value::F64(123.1231241));
//         document.insert("name", Value::String(Text::from("Hello, world!")));
//
//         encoder.encode(&document).expect("Encode document");
//         let _ = encoder.into_writer();
//
//         assert_eq!(writer.len(), 80, "Written byte lengths should match");
//
//         let checksum =
//             u32::from_le_bytes(writer[writer.len() - 4..].try_into().unwrap());
//         let actual = crc32fast::hash(&writer[..writer.len() - 8]);
//         assert_eq!(checksum, actual, "Checksums should match");
//
//         let rkyved_data = &writer[..writer.len() - 8];
//         let returned_doc =
//             rkyv::from_bytes::<Document>(rkyved_data).expect("Deserialize document");
//
//         assert_eq!(
//             document, returned_doc,
//             "Documents deserialized should match"
//         );
//     }
// }
