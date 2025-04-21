use chacha20poly1305::aead::OsRng;
use chacha20poly1305::{AeadCore, AeadInPlace, Tag, XChaCha20Poly1305, XNonce};

use super::VersionProcessor;



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

impl VersionProcessor for VersionV1EncProcessor {
    fn decode(
        &self,
        encoded_bytes: &mut [u8],
        reserved_bytes: &[u8],
    ) -> Result<(), ()> {
        let tag = Tag::from_slice(&reserved_bytes[..16]);
        let nonce = XNonce::from_slice(&reserved_bytes[16..40]);        
        
        // TODO: Return error / handle
        self.cipher.decrypt_in_place_detached(nonce, b"", encoded_bytes, tag).unwrap();
        
        Ok(())
    }

    fn encode(
        &self,
        raw_bytes: &mut [u8],
        reserved_bytes: &mut [u8],
    ) -> Result<(), ()> {
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        
        // TODO: Return error / handle
        let tag = self.cipher.encrypt_in_place_detached(&nonce, b"", raw_bytes).unwrap();
        
        reserved_bytes[..16].copy_from_slice(tag.as_slice());
        reserved_bytes[16..40].copy_from_slice(nonce.as_slice());
        
        Ok(())
    }
}
