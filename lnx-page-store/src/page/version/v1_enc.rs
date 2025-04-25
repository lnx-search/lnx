use std::fmt::Formatter;

use anyhow::{Result, anyhow, bail};
use bytes::BufMut;
use chacha20poly1305::aead::OsRng;
use chacha20poly1305::{
    AeadCore,
    AeadInPlace,
    Key,
    KeyInit,
    Tag,
    XChaCha20Poly1305,
    XNonce,
};

use super::VersionProcessor;
use crate::page::LayoutVersion;

/// The [VersionProcessor] for [LayoutVersion::V1Enc](super::LayoutVersion::V1Enc)
/// decoding and encoding.
///
/// The reserved bytes are in the layout of:
///
/// ```ignore
/// | Tag (16 bytes) | Nonce (24 bytes) |
/// ```
pub struct VersionV1EncProcessor {
    cipher: XChaCha20Poly1305,
}

impl VersionV1EncProcessor {
    /// Create a new [VersionV1EncProcessor] using the given encryption key.
    pub fn create_with_key(key: &Key) -> Self {
        let cipher = XChaCha20Poly1305::new(key);
        Self { cipher }
    }
}

impl std::fmt::Debug for VersionV1EncProcessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Processor(V1 Layout w/Encryption at rest)")
    }
}

impl VersionProcessor for VersionV1EncProcessor {
    fn associated_layout_version(&self) -> LayoutVersion {
        LayoutVersion::V1Enc
    }

    fn decode(&self, encoded_bytes: &mut [u8], reserved_bytes: &[u8]) -> Result<()> {
        if reserved_bytes.len() < 40 {
            bail!("reserved bytes buffer too small")
        }

        let tag = Tag::from_slice(&reserved_bytes[..16]);
        let nonce = XNonce::from_slice(&reserved_bytes[16..40]);

        self.cipher
            .decrypt_in_place_detached(nonce, b"", encoded_bytes, tag)
            .map_err(|e| {
                tracing::error!(error = %e, "system failed to decrypt page");
                anyhow!("failed to encrypt page")
            })?;

        Ok(())
    }

    fn encode(&self, raw_bytes: &mut [u8], reserved_bytes: &mut [u8]) -> Result<()> {
        if reserved_bytes.len() < 40 {
            bail!("reserved bytes buffer too small")
        }

        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);

        let tag = self
            .cipher
            .encrypt_in_place_detached(&nonce, b"", raw_bytes)
            .map_err(|e| {
                tracing::error!(error = %e, "system failed to encrypt page");
                anyhow!("failed to encrypt page")
            })?;

        reserved_bytes[..16].copy_from_slice(tag.as_slice());
        reserved_bytes[16..40].copy_from_slice(nonce.as_slice());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case(10, 40)]
    #[case(0, 40)]
    #[case(7 << 10, 40)]
    #[case(51, 40)]
    #[case(51, 128)]
    #[should_panic]
    #[case(51, 20)]
    #[should_panic]
    #[case(12, 0)]
    fn test_buffer_encode_decode(#[case] data_len: usize, #[case] reserved_len: usize) {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let processor = VersionV1EncProcessor::create_with_key(&key);

        let mut input_bytes = vec![1; data_len];
        let mut reserved_bytes = vec![1; reserved_len];

        processor
            .encode(&mut input_bytes, &mut reserved_bytes) 
            .expect("encode data");
        assert!(input_bytes.is_empty() || input_bytes != vec![1; data_len]);
        assert!(reserved_bytes.is_empty() || reserved_bytes != vec![0; reserved_len]);

        processor
            .decode(&mut input_bytes, &reserved_bytes)
            .expect("decode data");
        assert_eq!(input_bytes, vec![1; data_len]);
    }
}
