use super::encrypt;

/// The magic bytes prefix of page files.
static MAGIC_BYTES: &[u8] = b"__LNX_PAGEFILE__";

#[derive(Debug, Eq, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
/// The version number for the page file.
pub enum VersionedPageFileMetadata {
    /// Version 1
    V1(PageFileMetadataV1),
}

impl VersionedPageFileMetadata {
    /// Returns the [Encryption] mode used by the page.
    pub fn encryption(&self) -> Encryption {
        match self {
            VersionedPageFileMetadata::V1(v) => v.encryption,
        }
    }

    /// Returns number of pages in the page.
    pub fn num_pages(&self) -> usize {
        match self {
            VersionedPageFileMetadata::V1(v) => v.num_pages,
        }
    }
}

#[derive(Debug, Eq, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
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
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, serde_derive::Serialize, serde_derive::Deserialize,
)]
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
            0x01 => Some(Encryption::Disabled),
            0x02 => Some(Encryption::Enabled),
            _ => None,
        }
    }
}

/// Checks if the provided buffer starts with the page file [MAGIC_BYTES].
fn has_magic_bytes(buffer: &[u8]) -> bool {
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
fn get_encoding_mode_hint(buffer: &[u8]) -> Option<Encryption> {
    if buffer.len() < size_of::<Encryption>() {
        None
    } else {
        let bytes = &buffer[..size_of::<Encryption>()];
        Encryption::try_from(bytes.try_into().unwrap())
    }
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the page file metadata header from
/// being decoded.
pub enum DecodeError {
    #[error("buffer missing magic bytes prefix")]
    /// The buffer is missing the magic bytes at the start of the buffer.
    MissingMagicBytes,
    #[error("buffer missing encryption mode hint")]
    /// The buffer is missing the [Encryption] hint.
    /// This is used to decode the metadata.
    MissingEncryptionHint,
    #[error("buffer missing context bytes")]
    /// The buffer is missing required context bytes.
    MissingContextBytes,
    #[error("buffer missing length indicator")]
    /// The buffer is missing the length of the metadata.
    MissingMetadataLen,
    #[error("decrypt metadata fail")]
    /// The buffer was unable to be decrypted.
    DecryptionFailed,
    #[error("metadata is encrypted but not decryption cipher provided")]
    /// The buffer is encrypted but no decryption cipher is provided.
    MissingDecryptionCipher,
    #[error("deserialize error: {0}")]
    /// The metadata was unable to be deserialized.
    Deserialize(serde_json::Error),
}

/// Decode a page file metadata entry from the provided buffer.
pub fn decode_page_file_metadata(
    cipher: Option<&encrypt::Cipher>,
    mut buffer: &mut [u8],
) -> Result<VersionedPageFileMetadata, DecodeError> {
    if !has_magic_bytes(buffer) {
        return Err(DecodeError::MissingMagicBytes);
    }
    buffer = &mut buffer[MAGIC_BYTES.len()..];

    let encryption_hint =
        get_encoding_mode_hint(buffer).ok_or(DecodeError::MissingEncryptionHint)?;
    buffer = &mut buffer[size_of::<Encryption>()..];

    if buffer.len() < encrypt::CONTEXT_LEN {
        return Err(DecodeError::MissingContextBytes);
    }

    let indices = [0..encrypt::CONTEXT_LEN, encrypt::CONTEXT_LEN..buffer.len()];
    let [context, mut buffer] = buffer.get_disjoint_mut(indices).unwrap();

    if let Encryption::Enabled = encryption_hint {
        let cipher = cipher.ok_or(DecodeError::MissingDecryptionCipher)?;
        encrypt::decrypt_in_place(cipher, buffer, context)
            .map_err(|_| DecodeError::DecryptionFailed)?;
    }

    let buffer_len = u32::from_le_bytes(buffer[..size_of::<u32>()].try_into().unwrap());
    buffer = &mut buffer[size_of::<u32>()..size_of::<u32>() + buffer_len as usize];
    serde_json::from_slice(buffer).map_err(DecodeError::Deserialize)
}

#[derive(Debug, thiserror::Error)]
/// An error that prevented the page file metadata from being
/// encoded.
pub enum EncodeError {
    #[error("buffer too small")]
    /// The provided buffer must be 8KB in size.
    IncorrectBufferSize,
    #[error("failed to encrypt buffer: {0}")]
    /// The data could not be encrypted due to an error.
    EncryptionFailed(encrypt::EncryptError),
    #[error("serialize error: {0}")]
    /// The metadata could not be serialized.
    Serialized(serde_json::Error),
}

/// Encode the metadata into the given buffer.
///
/// This data will be encrypted if the cipher is provided.
pub fn encode_page_file_metadata(
    cipher: Option<&encrypt::Cipher>,
    metadata: &VersionedPageFileMetadata,
    mut buffer: &mut [u8],
) -> Result<(), EncodeError> {
    if matches!(metadata.encryption(), Encryption::Enabled) && cipher.is_none() {
        panic!("metadata signals data is encrypted but cipher is not provided");
    }

    if buffer.len() != (8 << 10) {
        return Err(EncodeError::IncorrectBufferSize);
    }

    buffer[..MAGIC_BYTES.len()].copy_from_slice(MAGIC_BYTES);
    buffer = &mut buffer[MAGIC_BYTES.len()..];

    let encryption = if cipher.is_some() {
        Encryption::Enabled
    } else {
        Encryption::Disabled
    };

    buffer[..size_of::<Encryption>()]
        .copy_from_slice(&(encryption as u32).to_le_bytes());
    buffer = &mut buffer[size_of::<Encryption>()..];

    let indices = [0..encrypt::CONTEXT_LEN, encrypt::CONTEXT_LEN..buffer.len()];
    let [context, buffer] = buffer.get_disjoint_mut(indices).unwrap();

    let data = serde_json::to_vec(metadata).map_err(EncodeError::Serialized)?;
    buffer[..size_of::<u32>()].copy_from_slice(&(data.len() as u32).to_le_bytes());
    buffer[size_of::<u32>()..size_of::<u32>() + data.len()].copy_from_slice(&data);

    if let Some(cipher) = cipher {
        encrypt::encrypt_in_place(cipher, buffer, context)
            .map_err(EncodeError::EncryptionFailed)?;
    }

    Ok(())
}
