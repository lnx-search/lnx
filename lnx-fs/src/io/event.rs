use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_derive::{Deserialize, Serialize};

use crate::config::COMMIT_MARKER_PREFIX;

pub static FOOTER_MAGIC_BYTES: &[u8] = b"__LNX_BLOB_ENTRY__";
pub const FOOTER_MAGIC_BYTES_LEN: usize = 18;
static EVENT_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
/// A unique, sortable event ID.
///
/// This is generated and maintains process-local guaranteed ordering.
pub struct EventId {
    ts: u64,
    uc: u64,
}

impl Default for EventId {
    fn default() -> Self {
        let ts = crate::utils::timestamp_now_ms();
        let uc = EVENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self { ts, uc }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileEvent {
    /// The unique ID assigned when the event was created.
    pub event_id: EventId,
    /// The transaction id attached to file.
    ///
    /// If this is Some(ID) the file is part of a bulk transaction
    /// and is only valid if a transaction commit marker exists.
    pub transaction_id: Option<ulid::Ulid>,
    /// Event specific data.
    pub data: EventData,
}

impl FileEvent {
    pub const FOOTER_LENGTH_BYTES: usize = size_of::<u32>();
    pub const FOOTER_ADDITIONAL_OVERHEAD_SIZE: usize =
        FOOTER_MAGIC_BYTES_LEN + Self::FOOTER_LENGTH_BYTES;

    /// Creates a new "CREATE" file event.
    pub fn create(
        transaction_id: Option<ulid::Ulid>,
        file_path: String,
        data_range: Range<u64>,
    ) -> Self {
        Self {
            event_id: EventId::default(),
            transaction_id,
            data: EventData::Create {
                file_path,
                data_range,
            },
        }
    }

    /// Creates a new "DELETE" file event.
    pub fn delete(transaction_id: Option<ulid::Ulid>, file_path: String) -> Self {
        Self {
            event_id: EventId::default(),
            transaction_id,
            data: EventData::Delete { file_path },
        }
    }

    /// Creates a new "RENAME" file event.
    pub fn rename(
        transaction_id: Option<ulid::Ulid>,
        from_path: String,
        to_path: String,
    ) -> Self {
        Self {
            event_id: EventId::default(),
            transaction_id,
            data: EventData::Rename { from_path, to_path },
        }
    }

    /// Returns if the event is a transaction commit marker.
    pub fn is_commit(&self) -> bool {
        match &self.data {
            EventData::Create { file_path, .. } => {
                file_path.starts_with(COMMIT_MARKER_PREFIX)
                    && file_path.ends_with(".commit")
            },
            _ => false,
        }
    }

    #[inline]
    /// Returns the timestamp in milliseconds when the event was created.
    pub fn created_at(&self) -> u64 {
        self.event_id.ts
    }

    /// Serializes the footer into a byte buffer.
    ///
    /// This will prefix the buffer with the [FOOTER_MAGIC_BYTES] and an u32 length
    /// of the buffer.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(FOOTER_MAGIC_BYTES);
        output.extend_from_slice(&[0; size_of::<u32>()]);

        rmp_serde::encode::write(&mut output, self)
            .expect("Footer serializer should never fail");

        let len = (output.len() - Self::FOOTER_ADDITIONAL_OVERHEAD_SIZE) as u32;
        output[FOOTER_MAGIC_BYTES_LEN..FOOTER_MAGIC_BYTES_LEN + size_of::<u32>()]
            .copy_from_slice(&len.to_le_bytes());

        output
    }

    /// Deserializes the footer from a byte buffer.
    ///
    /// This method expects the [FOOTER_MAGIC_BYTES] and buffer length as u32 and  to be at the start of the buffer.
    pub fn from_bytes(buffer: &[u8]) -> Option<Self> {
        if buffer.len() < Self::FOOTER_ADDITIONAL_OVERHEAD_SIZE + 1 {
            return None;
        }

        let len = u32::from_le_bytes(
            buffer[FOOTER_MAGIC_BYTES_LEN..FOOTER_MAGIC_BYTES_LEN + size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        let offset = Self::FOOTER_ADDITIONAL_OVERHEAD_SIZE;

        if buffer.len() < Self::FOOTER_ADDITIONAL_OVERHEAD_SIZE + len {
            return None;
        }

        rmp_serde::from_slice(&buffer[offset..offset + len]).ok()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// The event-specific data.
pub enum EventData {
    /// A file create event.
    Create {
        /// The path of the file.
        file_path: String,
        /// The start and end positions of the blob.
        data_range: Range<u64>,
    },
    /// A file deletion event.
    Delete {
        /// The path of the file.
        file_path: String,
    },
    /// A file rename event.
    Rename {
        /// The original name of the file before rename.
        from_path: String,
        /// The new name of the file after rename.
        to_path: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_footer_de_serialize() {
        let footer = FileEvent::create(None, "example/foo/bar.txt".into(), 0..123);

        let bytes = footer.to_bytes();
        assert_eq!(
            &bytes[..FOOTER_MAGIC_BYTES_LEN],
            FOOTER_MAGIC_BYTES,
            "Footer magic bytes have not been appended to buffer"
        );

        let read_footer = FileEvent::from_bytes(&bytes);
        assert_eq!(
            read_footer,
            Some(footer),
            "Deserialized footer does not match",
        );
    }

    #[test]
    fn test_delete_footer_de_serialize() {
        let footer = FileEvent::delete(None, "example/foo/bar.txt".into());

        let bytes = footer.to_bytes();
        println!("{bytes:?}");
        assert_eq!(
            &bytes[..FOOTER_MAGIC_BYTES_LEN],
            FOOTER_MAGIC_BYTES,
            "Footer magic bytes have not been appended to buffer"
        );

        let read_footer = FileEvent::from_bytes(&bytes);
        assert_eq!(
            read_footer,
            Some(footer),
            "Deserialized footer does not match",
        );
    }

    #[test]
    fn test_rename_footer_de_serialize() {
        let footer = FileEvent::rename(
            None,
            "example/foo/bar.txt".into(),
            "example/foo/baz.txt".into(),
        );

        let bytes = footer.to_bytes();
        assert_eq!(
            &bytes[..FOOTER_MAGIC_BYTES_LEN],
            FOOTER_MAGIC_BYTES,
            "Footer magic bytes have not been appended to buffer"
        );

        let read_footer = FileEvent::from_bytes(&bytes);
        assert_eq!(
            read_footer,
            Some(footer),
            "Deserialized footer does not match",
        );
    }

    #[test]
    fn test_footer_deserialize_buffer_too_short() {
        let footer = FileEvent::delete(None, "example/foo/bar.txt".into());
        let mut bytes = footer.to_bytes();
        bytes[FOOTER_MAGIC_BYTES_LEN..FOOTER_MAGIC_BYTES_LEN + 4]
            .copy_from_slice(&1000u32.to_le_bytes());

        let read_footer = FileEvent::from_bytes(&bytes);
        assert!(
            read_footer.is_none(),
            "Footer should be none due to size missmatch"
        );
    }

    #[test]
    fn test_footer_too_small_none() {
        let footer = FileEvent::from_bytes(&[]);
        assert!(footer.is_none());
    }

    #[test]
    fn test_is_commit() {
        let event =
            FileEvent::create(None, "__lnx_fs/transactions/foo.commit".into(), 0..0);
        assert!(event.is_commit());
        let event = FileEvent::create(None, "__lnx_fs/foo.commit".into(), 0..0);
        assert!(!event.is_commit());
        let event =
            FileEvent::create(None, "example/transactions/foo.commit".into(), 0..0);
        assert!(!event.is_commit());
        let event = FileEvent::rename(
            None,
            "example/transactions/foo.commit".into(),
            "other.txt".into(),
        );
        assert!(!event.is_commit());
        let event = FileEvent::delete(None, "example/transactions/foo.commit".into());
        assert!(!event.is_commit());
    }
}
