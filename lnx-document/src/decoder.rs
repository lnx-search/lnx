use std::io::ErrorKind;
use std::marker::PhantomData;
use std::{io, mem};

use crate::Document;

const MINIMUM_BUFFER_LEN: usize = FOOTER_SIZE + 1;
pub const FOOTER_SIZE: usize = 8;

/// A document decoder that produces document from a given borrowed buffer.
///
/// This decoder reads from the tail of the buffer, backwards to head of the buffer
/// this is because the layout of the encoded documents is in the following format:
/// `| data | length | checksum |`
pub struct Decoder<'a> {
    buf: &'a [u8],
    validate_checksum: bool,
    known_positions: Option<&'a rkyv::Archived<Vec<u32>>>,
}

impl<'a> Decoder<'a> {
    /// Create a new document decoder.
    ///
    /// This decoder anticipates that it can read the docs
    /// from the end of the buffer working backwards.
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            validate_checksum: false,
            known_positions: None,
        }
    }

    /// Create a new document decoder with a set of positions
    /// that can be looked up for direct access.
    pub fn using_known_positions(
        buf: &'a [u8],
        positions: &'a rkyv::Archived<Vec<u32>>,
    ) -> Self {
        Self {
            buf,
            validate_checksum: false,
            known_positions: Some(positions),
        }
    }

    /// Gets the archived value located at the given idx position.
    ///
    /// ### Safety
    /// This performs no validation, you must ensure the buffer is correctly aligned
    /// and has the correct layout.
    pub unsafe fn archived_at(
        &self,
        idx: usize,
    ) -> io::Result<Option<&'a rkyv::Archived<Document>>> {
        let position_opt = self.known_positions.as_ref().and_then(|p| p.get(idx));
        let start = match position_opt {
            None => return Ok(None),
            Some(start) => *start as usize,
        };

        match read_doc_slice_at_position(self.buf, start, self.validate_checksum) {
            Some(Ok((doc_slice, _))) => {
                Ok(Some(rkyv::archived_root::<Document>(doc_slice)))
            },
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Deserialize the document at the given idx position.
    pub fn checked_archived_at(
        &self,
        idx: usize,
    ) -> io::Result<Option<&'a rkyv::Archived<Document>>> {
        let position_opt = self.known_positions.as_ref().and_then(|p| p.get(idx));
        let start = match position_opt {
            None => return Ok(None),
            Some(start) => *start as usize,
        };

        match read_doc_slice_at_position(self.buf, start, self.validate_checksum) {
            Some(Ok((doc_slice, _))) => rkyv::check_archived_root::<Document>(doc_slice)
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))
                .map(Some),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Enables validation of the document checksum.
    pub fn enable_checksum_validation(&mut self) {
        self.validate_checksum = true;
    }

    /// Create a new iterator for all docs nwithin the buffer.
    pub fn buffer_walker(&self) -> BufferWalker<'a> {
        BufferWalker::new(self.buf, self.validate_checksum)
    }

    /// An unsafe archive iterator.
    ///
    /// This has incredibly minimal overhead, especially when checksum calculations
    /// are disabled.
    ///
    /// This comes at a cost with safety however:
    ///
    /// ### Safety
    /// You **must** ensure the provided buffer is correctly aligned and has
    /// the correct layout, otherwise this is immediately UB.
    pub unsafe fn archived_iter(&self) -> ArchivedIterator<'a, UnsafeArchiver> {
        ArchivedIterator::new(self.buf, self.validate_checksum)
    }

    /// A archive iterator.
    ///
    /// This iterator uses requires validation to be enabled but provides a
    /// safe API rather than becoming UB on an invalid buffer being provided.
    pub fn checked_archived_iter(&self) -> ArchivedIterator<'a, CheckedArchiver> {
        ArchivedIterator::new(self.buf, self.validate_checksum)
    }
}

#[inline]
fn read_doc_slice_at_position(
    buf: &[u8],
    start: usize,
    validate_checksum: bool,
) -> Option<io::Result<(&[u8], usize)>> {
    let active_slice = &buf[..start];
    let footer_slice = &active_slice[active_slice.len() - FOOTER_SIZE..];

    let length =
        u32::from_le_bytes(footer_slice[..mem::size_of::<u32>()].try_into().ok()?)
            as usize;
    let expected_checksum =
        u32::from_le_bytes(footer_slice[mem::size_of::<u32>()..].try_into().ok()?);

    let total_length = length + FOOTER_SIZE;
    if active_slice.len() < total_length {
        return None;
    }

    let doc_slice = &active_slice
        [active_slice.len() - total_length..active_slice.len() - FOOTER_SIZE];

    if validate_checksum {
        let checksum = crc32fast::hash(doc_slice);

        if checksum != expected_checksum {
            return Some(Err(io::Error::new(
                ErrorKind::InvalidData,
                "Checksums did not match when validating doc data",
            )));
        }
    }

    Some(Ok((doc_slice, total_length)))
}

pub struct BufferWalker<'a> {
    buf: &'a [u8],
    validate_checksum: bool,
    cursor: usize,
}

impl<'a> BufferWalker<'a> {
    pub fn new(buf: &'a [u8], validate_checksum: bool) -> Self {
        Self {
            buf,
            validate_checksum,
            cursor: buf.len(),
        }
    }
}

impl<'a> Iterator for BufferWalker<'a> {
    type Item = io::Result<&'a [u8]>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < MINIMUM_BUFFER_LEN {
            return None;
        }

        match read_doc_slice_at_position(self.buf, self.cursor, self.validate_checksum)?
        {
            Ok((doc_slice, total_length)) => {
                self.cursor -= total_length;
                Some(Ok(doc_slice))
            },
            Err(e) => Some(Err(e)),
        }
    }
}

/// An iterator that produces archived values rather than
/// deserializing the data.
///
/// The safety behavour of this iterator can change depending on
/// the `Archiver` used (defaults to the safe `CheckedArchiver`.
pub struct ArchivedIterator<'a, A: Archiver> {
    walker: BufferWalker<'a>,
    _phantom: PhantomData<A>,
}

impl<'a, A: Archiver> ArchivedIterator<'a, A> {
    fn new(buf: &'a [u8], validate_checksum: bool) -> Self {
        Self {
            walker: BufferWalker::new(buf, validate_checksum),
            _phantom: PhantomData,
        }
    }
}

impl<'a, A: Archiver> Iterator for ArchivedIterator<'a, A> {
    type Item = io::Result<&'a rkyv::Archived<Document<'a>>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let data = match self.walker.next()? {
            Ok(data) => data,
            Err(e) => return Some(Err(e)),
        };

        Some(A::get_archived(data))
    }
}

/// A type which can load an archived value from a given buffer.
///
/// This makes not guarantees about safety and is down to the implementor
/// of the trait to ensure things are correct.
pub trait Archiver: private::Sealed {
    fn get_archived(buf: &[u8]) -> io::Result<&rkyv::Archived<Document>>;
}

mod private {
    pub trait Sealed {}
}

/// A archiver that performs no validation when loading
/// the archived root of the object.
///
/// This archiver is infallible, instead it becomes UB.
///
/// WARNING:
///     This requires the buffer being passed into it to both be
///     correctly aligned and have the correct layout otherwise this is UB.
pub struct UnsafeArchiver;
impl private::Sealed for UnsafeArchiver {}
impl Archiver for UnsafeArchiver {
    #[inline]
    fn get_archived(buf: &[u8]) -> io::Result<&rkyv::Archived<Document>> {
        // SAFETY:
        //  This requires the buffer to both be correctly aligned, and have
        //  the correct layout, otherwise this is UB.
        Ok(unsafe { rkyv::archived_root::<Document>(buf) })
    }
}

/// A archiver that performs validation when loading
/// the archived root of the object.
///
/// This archiver is fallible and will reject any buffers
/// that do not meet the safety requirements for rkyv.
pub struct CheckedArchiver;
impl private::Sealed for CheckedArchiver {}
impl Archiver for CheckedArchiver {
    #[inline]
    fn get_archived(buf: &[u8]) -> io::Result<&rkyv::Archived<Document>> {
        rkyv::check_archived_root::<Document>(buf)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))
    }
}

// #[cfg(test)]
// mod tests {
//     use rkyv::de::deserializers::SharedDeserializeMap;
//     use rkyv::{AlignedVec, Deserialize};
//
//     use super::*;
//     use crate::{Encoder, Text, Value, DEFAULT_SCRATCH_SPACE};
//
//     #[test]
//     fn test_read_doc_slice_at_position() {
//         let msg = b"Hello, world";
//         let length = msg.len();
//         let checksum = crc32fast::hash(msg);
//
//         let mut payload = Vec::new();
//         payload.extend_from_slice(msg);
//         payload.extend_from_slice(&(length as u32).to_le_bytes());
//         payload.extend_from_slice(&checksum.to_le_bytes());
//         let data = read_doc_slice_at_position(&payload, payload.len(), true);
//
//         assert!(data.is_some(), "Slice should be produced");
//         let (data, _) = data.unwrap().expect("Checksums should match");
//         assert_eq!(data, &payload[..length], "Slices should match");
//
//         let msg2 = b"Hello, world 2";
//         let length2 = msg2.len();
//         let checksum2 = crc32fast::hash(msg2);
//         let msg_start = payload.len();
//         payload.extend_from_slice(msg2);
//         payload.extend_from_slice(&(length2 as u32).to_le_bytes());
//         payload.extend_from_slice(&checksum2.to_le_bytes());
//
//         let data = read_doc_slice_at_position(&payload, payload.len(), true);
//         assert!(data.is_some(), "Slice should be produced");
//         let (data, _) = data.unwrap().expect("Checksums should match");
//         assert_eq!(data, msg2, "Slices should match");
//
//         let data = read_doc_slice_at_position(&payload, msg_start, true);
//         assert!(data.is_some(), "Slice should be produced");
//         let (data, _) = data.unwrap().expect("Checksums should match");
//         assert_eq!(data, msg, "Slices should match");
//     }
//
//     #[test]
//     fn test_buffer_walker() {
//         let msg = b"Hello, world";
//         let length = msg.len();
//         let checksum = crc32fast::hash(msg);
//         let mut payload = Vec::new();
//         payload.extend_from_slice(msg);
//         payload.extend_from_slice(&(length as u32).to_le_bytes());
//         payload.extend_from_slice(&checksum.to_le_bytes());
//
//         let msg2 = b"Hello, world 2";
//         let length2 = msg2.len();
//         let checksum2 = crc32fast::hash(msg2);
//         payload.extend_from_slice(msg2);
//         payload.extend_from_slice(&(length2 as u32).to_le_bytes());
//         payload.extend_from_slice(&checksum2.to_le_bytes());
//
//         let mut walker = BufferWalker::new(&payload, true);
//
//         let doc1 = walker
//             .next()
//             .expect("Doc should be returned")
//             .expect("Doc should be sliced");
//         let doc2 = walker
//             .next()
//             .expect("Doc should be returned")
//             .expect("Doc should be sliced");
//         let doc3 = walker.next();
//
//         assert_eq!(doc1, msg2, "Doc data should match");
//         assert_eq!(doc2, msg, "Doc data should match");
//         assert!(doc3.is_none(), "Document should not exist");
//     }
//
//     #[test]
//     fn test_encode_decode_safe_archived_root() {
//         let mut writer = AlignedVec::new();
//         let mut encoder = Encoder::<_, DEFAULT_SCRATCH_SPACE>::new(&mut writer);
//
//         let mut document = Document::default();
//         document.insert("id", Value::U64(1));
//         document.insert("terms", Value::ArrayString(vec![Text::from("hello")]));
//         encoder.encode(&document).expect("Encode document");
//
//         let mut document = Document::default();
//         document.insert("id", Value::U64(2));
//         document.insert("terms", Value::ArrayString(vec![Text::from("hello2")]));
//         encoder.encode(&document).expect("Encode document");
//
//         let mut document = Document::default();
//         document.insert("id", Value::U64(3));
//         document.insert("terms", Value::ArrayString(vec![Text::from("hello3")]));
//         encoder.encode(&document).expect("Encode document");
//
//         let decoder = Decoder::new(&writer);
//         let mut docs = decoder.checked_archived_iter();
//
//         // The linter may mark this as an error, but it's a false-positive.
//         let doc1: Document = docs
//             .next()
//             .expect("Doc should exist")
//             .expect("Doc should be archived")
//             .deserialize(&mut SharedDeserializeMap::default())
//             .expect("Deserialize doc");
//         let doc2: Document = docs
//             .next()
//             .expect("Doc should exist")
//             .expect("Doc should be archived")
//             .deserialize(&mut SharedDeserializeMap::default())
//             .expect("Deserialize doc");
//         let doc3: Document = docs
//             .next()
//             .expect("Doc should exist")
//             .expect("Doc should be archived")
//             .deserialize(&mut SharedDeserializeMap::default())
//             .expect("Deserialize doc");
//         let doc4 = docs.next();
//
//         assert_eq!(
//             doc1.fields()[0].1,
//             Value::U64(3),
//             "Document values should match."
//         );
//         assert_eq!(
//             doc1.fields()[1].1,
//             Value::ArrayString(vec![Text::from("hello3")]),
//             "Document values should match."
//         );
//         assert_eq!(
//             doc2.fields()[0].1,
//             Value::U64(2),
//             "Document values should match."
//         );
//         assert_eq!(
//             doc2.fields()[1].1,
//             Value::ArrayString(vec![Text::from("hello2")]),
//             "Document values should match."
//         );
//         assert_eq!(
//             doc3.fields()[0].1,
//             Value::U64(1),
//             "Document values should match."
//         );
//         assert_eq!(
//             doc3.fields()[1].1,
//             Value::ArrayString(vec![Text::from("hello")]),
//             "Document values should match."
//         );
//         assert!(doc4.is_none(), "Doc4 should not exist");
//     }
//
//     #[test]
//     fn test_encode_decode_deserialize() {
//         let mut writer = AlignedVec::new();
//         let mut encoder = Encoder::<_, DEFAULT_SCRATCH_SPACE>::new(&mut writer);
//
//         let mut document = Document::default();
//         document.insert("id", Value::U64(1));
//         document.insert("terms", Value::ArrayString(vec![Text::from("hello")]));
//         encoder.encode(&document).expect("Encode document");
//
//         let mut document = Document::default();
//         document.insert("id", Value::U64(2));
//         document.insert("terms", Value::ArrayString(vec![Text::from("hello2")]));
//         encoder.encode(&document).expect("Encode document");
//
//         let mut document = Document::default();
//         document.insert("id", Value::U64(3));
//         document.insert("terms", Value::ArrayString(vec![Text::from("hello3")]));
//         encoder.encode(&document).expect("Encode document");
//
//         let decoder = Decoder::new(&writer);
//         let mut docs = decoder.deserializer_iter();
//
//         let doc1: Document = docs
//             .next()
//             .expect("Doc should exist")
//             .expect("Doc should be archived");
//         let doc2: Document = docs
//             .next()
//             .expect("Doc should exist")
//             .expect("Doc should be archived");
//         let doc3: Document = docs
//             .next()
//             .expect("Doc should exist")
//             .expect("Doc should be archived");
//         let doc4 = docs.next();
//
//         assert_eq!(
//             doc1.fields()[0].0.as_ref(),
//             "id",
//             "Document key should match."
//         );
//         assert_eq!(
//             doc1.fields()[0].1,
//             Value::U64(3),
//             "Document values should match."
//         );
//         assert_eq!(
//             doc1.fields()[1].1,
//             Value::ArrayString(vec![Text::from("hello3")]),
//             "Document values should match."
//         );
//
//         assert_eq!(
//             doc2.fields()[0].0.as_ref(),
//             "id",
//             "Document key should match."
//         );
//         assert_eq!(
//             doc2.fields()[0].1,
//             Value::U64(2),
//             "Document values should match."
//         );
//         assert_eq!(
//             doc2.fields()[1].1,
//             Value::ArrayString(vec![Text::from("hello2")]),
//             "Document values should match."
//         );
//
//         assert_eq!(
//             doc3.fields()[0].0.as_ref(),
//             "id",
//             "Document key should match."
//         );
//         assert_eq!(
//             doc3.fields()[0].1,
//             Value::U64(1),
//             "Document values should match."
//         );
//         assert_eq!(
//             doc3.fields()[1].1,
//             Value::ArrayString(vec![Text::from("hello")]),
//             "Document values should match."
//         );
//
//         assert!(doc4.is_none(), "Doc4 should not exist");
//     }
//
//     #[test]
//     // This test basically just checks if we're going to segfault or not.
//     fn test_encode_decode_unsafe() {
//         let mut writer = AlignedVec::new();
//         let mut encoder = Encoder::<_, DEFAULT_SCRATCH_SPACE>::new(&mut writer);
//
//         let mut document = Document::default();
//         document.insert("id", Value::U64(1));
//         encoder.encode(&document).expect("Encode document");
//
//         let mut document = Document::default();
//         document.insert("id", Value::U64(2));
//         encoder.encode(&document).expect("Encode document");
//
//         let mut document = Document::default();
//         document.insert("id", Value::U64(3));
//         encoder.encode(&document).expect("Encode document");
//
//         let decoder = Decoder::new(&writer);
//         let mut docs = unsafe { decoder.archived_iter() };
//
//         // The linter may mark this as an error, but it's a false-positive.
//         let doc1: Document = docs
//             .next()
//             .expect("Doc should exist")
//             .expect("Doc should be archived")
//             .deserialize(&mut SharedDeserializeMap::default())
//             .expect("Deserialize doc");
//         let doc2: Document = docs
//             .next()
//             .expect("Doc should exist")
//             .expect("Doc should be archived")
//             .deserialize(&mut SharedDeserializeMap::default())
//             .expect("Deserialize doc");
//         let doc3: Document = docs
//             .next()
//             .expect("Doc should exist")
//             .expect("Doc should be archived")
//             .deserialize(&mut SharedDeserializeMap::default())
//             .expect("Deserialize doc");
//         let doc4 = docs.next();
//
//         assert_eq!(
//             doc1.fields()[0].0.as_ref(),
//             "id",
//             "Document key should match."
//         );
//         assert_eq!(
//             doc1.fields()[0].1,
//             Value::U64(3),
//             "Document values should match."
//         );
//
//         assert_eq!(
//             doc2.fields()[0].0.as_ref(),
//             "id",
//             "Document key should match."
//         );
//         assert_eq!(
//             doc2.fields()[0].1,
//             Value::U64(2),
//             "Document values should match."
//         );
//
//         assert_eq!(
//             doc3.fields()[0].0.as_ref(),
//             "id",
//             "Document key should match."
//         );
//         assert_eq!(
//             doc3.fields()[0].1,
//             Value::U64(1),
//             "Document values should match."
//         );
//
//         assert!(doc4.is_none(), "Doc4 should not exist");
//     }
// }
