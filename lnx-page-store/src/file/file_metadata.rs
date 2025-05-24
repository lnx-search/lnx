/// The magic bytes prefix of page files.
pub static MAGIC_BYTES: &[u8] = b"__LNX_PAGEFILE__";

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
/// The version number for the page file.
pub enum VersionedPageFileMetadata {
    /// Version 1
    V1(PageFileMetadataV1),
}

#[derive(Debug, serde_derive::Serialize, serde_derive::Deserialize)]
/// The V1 metadata information.
///
/// This information is encrypted
pub struct PageFileMetadataV1 {
    /// The true encryption mode of the data.
    pub encryption: Encryption,
    /// The total number of pages in the file.
    pub num_pages: usize,
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
/// The encoding mode used for housing pages.
pub enum Encryption {
    /// Encryption at rest is disabled.
    Disabled = 0x01,
    /// Encryption at rest is enabled.
    Enabled = 0x02,
}

impl Encryption {
    fn try_from(bytes: [u8; 4]) -> Option<Self> {
        let value = u32::from_le_bytes(bytes);
        match value {
            0x01 => Some(Encryption::Enabled),
            0x02 => Some(Encryption::Disabled),
            _ => None,
        }
    }
}

/// Checks if the provided buffer starts with the page file [MAGIC_BYTES].
pub fn has_magic_bytes(buffer: &[u8]) -> bool {
    if buffer.len() < MAGIC_BYTES.len() {
        false
    } else {
        &buffer[..MAGIC_BYTES.len()] == MAGIC_BYTES
    }
}

/// Attempts to read the encryption mode hint.
///
/// This should not be taken as the truth for anything other
/// than decoding the header.
pub fn get_encoding_mode_hint(buffer: &[u8]) -> Option<Encryption> {
    if buffer.len() < size_of::<Encryption>() {
        None
    } else {
        let bytes = &buffer[..size_of::<Encryption>()];
        Encryption::try_from(bytes.try_into().unwrap())
    }
}
