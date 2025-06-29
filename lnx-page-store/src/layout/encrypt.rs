use chacha20poly1305::aead::OsRng;
use chacha20poly1305::{AeadCore, AeadInPlace, Tag, XChaCha20Poly1305, XNonce};

/// The length of context data.
pub const CONTEXT_LEN: usize = 40;
pub type Cipher = XChaCha20Poly1305;

#[derive(Debug, thiserror::Error)]
#[error("failed to decrypt data")]
/// The given buffer could not be decrypted.
pub struct DecryptError;

/// Decrypt the provided buffer in place.
///
/// Returns an error if the buffer could not be decrypted.
pub fn decrypt_in_place(
    cipher: &XChaCha20Poly1305,
    associated_data: &[u8],
    encoded_bytes: &mut [u8],
    context: &[u8],
) -> Result<(), DecryptError> {
    if context.len() < CONTEXT_LEN {
        return Err(DecryptError);
    }

    let tag = Tag::from_slice(&context[..16]);
    let nonce = XNonce::from_slice(&context[16..CONTEXT_LEN]);

    cipher
        .decrypt_in_place_detached(nonce, associated_data, encoded_bytes, tag)
        .map_err(|_| DecryptError)?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
/// The given buffer could not be encrypted.
pub struct EncryptError(String);

/// Encrypt a buffer in place.
///
/// Requires a mutable `context` buffer to be provided
/// that is at least 40 bytes.
///
/// This context buffer contains the nonce and tag.
pub fn encrypt_in_place(
    cipher: &XChaCha20Poly1305,
    associated_data: &[u8],
    raw_bytes: &mut [u8],
    context: &mut [u8],
) -> Result<(), EncryptError> {
    if context.len() < CONTEXT_LEN {
        return Err(EncryptError("provided context buffer is too small".into()));
    }

    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);

    let tag = cipher
        .encrypt_in_place_detached(&nonce, associated_data, raw_bytes)
        .map_err(|e| EncryptError(e.to_string()))?;

    context[..16].copy_from_slice(tag.as_slice());
    context[16..CONTEXT_LEN].copy_from_slice(nonce.as_slice());

    Ok(())
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use chacha20poly1305::KeyInit;

    use super::*;

    #[rstest::rstest]
    #[case(10, 40)]
    #[case(0, CONTEXT_LEN)]
    #[case(7 << 10, CONTEXT_LEN)]
    #[case(51, CONTEXT_LEN)]
    #[case(51, 128)]
    fn test_buffer_encrypt_decrypt(
        #[case] data_len: usize,
        #[case] reserved_len: usize,
    ) {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);

        let mut input_bytes = vec![1; data_len];
        let mut reserved_bytes = vec![1; reserved_len];

        encrypt_in_place(&cipher, b"", &mut input_bytes, &mut reserved_bytes)
            .expect("encode data");
        assert!(input_bytes.is_empty() || input_bytes != vec![1; data_len]);
        assert!(reserved_bytes.is_empty() || reserved_bytes != vec![0; reserved_len]);

        decrypt_in_place(&cipher, b"", &mut input_bytes, &reserved_bytes)
            .expect("decode data");
        assert_eq!(input_bytes, vec![1; data_len]);
    }

    #[rstest::rstest]
    #[should_panic]
    #[case(51, 40, "")]
    #[case(51, 20, "provided context buffer is too small")]
    #[case(12, 0, "provided context buffer is too small")]
    fn test_buffer_encrypt_error(
        #[case] data_len: usize,
        #[case] reserved_len: usize,
        #[case] expected_msg: &str,
    ) {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);

        let mut input_bytes = vec![1; data_len];
        let mut reserved_bytes = vec![1; reserved_len];

        let err = encrypt_in_place(&cipher, b"", &mut input_bytes, &mut reserved_bytes)
            .expect_err("encode should error");
        assert_eq!(err.to_string(), expected_msg);
    }

    #[test]
    fn test_decrypt_context_too_small() {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);

        let err = decrypt_in_place(&cipher, b"", &mut [], &mut []).unwrap_err();
        assert_eq!(err.to_string(), DecryptError.to_string());
    }

    #[test]
    fn test_decrypt_key_miss_match() {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);

        let mut input_bytes = vec![1; 128];
        let mut reserved_bytes = vec![1; 40];

        encrypt_in_place(&cipher, b"", &mut input_bytes, &mut reserved_bytes).unwrap();

        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);
        let err = decrypt_in_place(&cipher, b"", &mut input_bytes, &reserved_bytes)
            .unwrap_err();
        assert_eq!(err.to_string(), DecryptError.to_string());
    }
}
