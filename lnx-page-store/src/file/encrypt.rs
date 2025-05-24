use chacha20poly1305::aead::OsRng;
use chacha20poly1305::{AeadCore, AeadInPlace, Tag, XChaCha20Poly1305, XNonce};

#[derive(Debug, thiserror::Error)]
#[error("failed to decrypt data")]
/// The given buffer could not be decrypted.
pub struct DecryptError;

/// Decrypt the provided buffer in place.
///
/// Returns an error if the buffer could not be decrypted.
pub fn decrypt_in_place(
    cipher: &XChaCha20Poly1305,
    encoded_bytes: &mut [u8],
    context: &[u8],
) -> Result<(), DecryptError> {
    if context.len() < 40 {
        return Err(DecryptError);
    }

    let tag = Tag::from_slice(&context[..16]);
    let nonce = XNonce::from_slice(&context[16..40]);

    cipher
        .decrypt_in_place_detached(nonce, b"", encoded_bytes, tag)
        .map_err(|_| DecryptError)?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("failed to encrypt data")]
/// The given buffer could not be encrypted.
pub struct EncryptError;

/// Encrypt a buffer in place.
///
/// Requires a mutable `context` buffer to be provided
/// that is at least 40 bytes.
///
/// This context buffer contains the nonce and tag.
pub fn encrypt_in_place(
    cipher: &XChaCha20Poly1305,
    raw_bytes: &mut [u8],
    context: &mut [u8],
) -> Result<(), EncryptError> {
    if context.len() < 40 {
        return Err(EncryptError);
    }

    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);

    let tag = cipher
        .encrypt_in_place_detached(&nonce, b"", raw_bytes)
        .map_err(|_| EncryptError)?;

    context[..16].copy_from_slice(tag.as_slice());
    context[16..40].copy_from_slice(nonce.as_slice());

    Ok(())
}

#[cfg(test)]
mod tests {
    use chacha20poly1305::KeyInit;

    use super::*;

    #[rstest::rstest]
    #[case(10, 40)]
    #[case(0, 40)]
    #[case(7 << 10, 40)]
    #[case(51, 40)]
    #[case(51, 128)]
    fn test_buffer_encode_decode(#[case] data_len: usize, #[case] reserved_len: usize) {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);

        let mut input_bytes = vec![1; data_len];
        let mut reserved_bytes = vec![1; reserved_len];

        encrypt_in_place(&cipher, &mut input_bytes, &mut reserved_bytes)
            .expect("encode data");
        assert!(input_bytes.is_empty() || input_bytes != vec![1; data_len]);
        assert!(reserved_bytes.is_empty() || reserved_bytes != vec![0; reserved_len]);

        decrypt_in_place(&cipher, &mut input_bytes, &reserved_bytes)
            .expect("decode data");
        assert_eq!(input_bytes, vec![1; data_len]);
    }

    #[rstest::rstest]
    #[should_panic]
    #[case(51, 40)]
    #[case(51, 20)]
    #[case(12, 0)]
    fn test_buffer_encode_error(#[case] data_len: usize, #[case] reserved_len: usize) {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);

        let mut input_bytes = vec![1; data_len];
        let mut reserved_bytes = vec![1; reserved_len];

        let err = encrypt_in_place(&cipher, &mut input_bytes, &mut reserved_bytes)
            .expect_err("encode should error");
        assert!(matches!(err, EncryptError));
    }
}
