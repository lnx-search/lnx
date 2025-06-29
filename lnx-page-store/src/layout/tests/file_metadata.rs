use chacha20poly1305::aead::OsRng;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305};

use crate::layout::encrypt;
use crate::layout::file_metadata::{
    DecodeError,
    EncodeError,
    Encryption,
    decode_metadata,
    encode_metadata,
};

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Debug, Eq, PartialEq)]
struct SampleMetadata {
    id: i32,
}

#[rstest::rstest]
#[case::encode_v1_no_enc(SampleMetadata { id: 1 }, false)]
#[case::encode_v1_enc(SampleMetadata { id: 1 }, true)]
fn test_encode(#[case] metadata: SampleMetadata, #[case] encrypt: bool) {
    let mut buffer = vec![0; 8 << 10];

    let cipher = if encrypt {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        Some(XChaCha20Poly1305::new(&key))
    } else {
        None
    };

    encode_metadata(cipher.as_ref(), b"", &metadata, &mut buffer)
        .expect("encode page correctly");
}

#[rstest::rstest]
#[case::encode_v1_buffer_too_big(4 << 10, EncodeError::IncorrectBufferSize)]
#[case::encode_v1_buffer_too_small(12 << 10, EncodeError::IncorrectBufferSize)]
fn test_encode_error(#[case] buffer_size: usize, #[case] expected_error: EncodeError) {
    let metadata = SampleMetadata { id: 1 };

    let mut buffer = vec![0; buffer_size];
    let err = encode_metadata(None, b"", &metadata, &mut buffer)
        .expect_err("metadata should not be encoded");
    assert_eq!(err.to_string(), expected_error.to_string());
}

#[rstest::rstest]
#[case::encode_v1_no_enc(SampleMetadata { id: 1 }, false)]
#[case::encode_v1_enc(SampleMetadata { id: 1 }, true)]
fn test_encode_decode(#[case] metadata: SampleMetadata, #[case] encrypt: bool) {
    let mut buffer = vec![0; 8 << 10];

    let cipher = if encrypt {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        Some(XChaCha20Poly1305::new(&key))
    } else {
        None
    };

    encode_metadata(cipher.as_ref(), b"", &metadata, &mut buffer)
        .expect("encode page correctly");

    let loaded_metadata =
        decode_metadata::<SampleMetadata>(cipher.as_ref(), b"", &mut buffer)
            .expect("encoded page should be decodable in the same config");

    assert_eq!(
        loaded_metadata, metadata,
        "loaded metadata does not match serialized value"
    );
}

#[rstest::rstest]
#[case::encryption_enabled(Encryption::Enabled)]
#[case::encryption_disabled(Encryption::Disabled)]
fn test_decode_err_missing_magic_bytes(#[case] encryption: Encryption) {
    let (mut buffer, cipher) = create_sample_buffer(encryption);
    buffer[..16].fill(0);

    let err = decode_metadata::<SampleMetadata>(cipher.as_ref(), b"", &mut buffer)
        .unwrap_err();
    assert_eq!(err.to_string(), DecodeError::MissingMagicBytes.to_string());
}

#[test]
fn test_decode_err_missing_magic_bytes_empty_buf() {
    let err = decode_metadata::<SampleMetadata>(None, b"", &mut []).unwrap_err();
    assert_eq!(err.to_string(), DecodeError::MissingMagicBytes.to_string());
}

#[rstest::rstest]
#[case::buf_short_encryption_enabled(Encryption::Enabled, 16)]
#[case::buf_short_encryption_disabled(Encryption::Disabled, 16)]
#[case::buf_invalid_encryption_enabled(Encryption::Enabled, 8 << 10)]
#[case::buf_invalid_encryption_disabled(Encryption::Disabled, 8 << 10)]
fn test_decode_err_missing_encryption_hint(
    #[case] encryption: Encryption,
    #[case] slice_at: usize,
) {
    let (mut buffer, cipher) = create_sample_buffer(encryption);
    buffer[16..20].fill(0);

    let err =
        decode_metadata::<SampleMetadata>(cipher.as_ref(), b"", &mut buffer[..slice_at])
            .unwrap_err();
    assert_eq!(
        err.to_string(),
        DecodeError::MissingEncryptionHint.to_string()
    );
}

#[test]
fn test_decode_err_missing_context() {
    let (mut buffer, cipher) = create_sample_buffer(Encryption::Disabled);

    let err = decode_metadata::<SampleMetadata>(cipher.as_ref(), b"", &mut buffer[..20])
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        DecodeError::MissingContextBytes.to_string()
    );
}

#[test]
fn test_decode_err_missing_decryption_cipher() {
    let (mut buffer, _) = create_sample_buffer(Encryption::Enabled);
    buffer[20..60].fill(0);

    let err = decode_metadata::<SampleMetadata>(None, b"", &mut buffer).unwrap_err();
    assert_eq!(
        err.to_string(),
        DecodeError::MissingDecryptionCipher.to_string()
    );
}

#[test]
fn test_decode_err_encryption_fail_context_wrong() {
    let (mut buffer, cipher) = create_sample_buffer(Encryption::Enabled);
    buffer[20..60].fill(0);

    let err = decode_metadata::<SampleMetadata>(cipher.as_ref(), b"", &mut buffer)
        .unwrap_err();
    assert_eq!(err.to_string(), DecodeError::DecryptionFailed.to_string());
}

#[test]
fn test_decode_err_encryption_fail_key_wrong() {
    let (mut buffer, _) = create_sample_buffer(Encryption::Enabled);

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);

    let err =
        decode_metadata::<SampleMetadata>(Some(&cipher), b"", &mut buffer).unwrap_err();
    assert_eq!(err.to_string(), DecodeError::DecryptionFailed.to_string());
}

#[test]
fn test_decode_err_malformed_json() {
    let (mut buffer, _) = create_sample_buffer(Encryption::Disabled);
    buffer[60..].fill(0);

    let err = decode_metadata::<SampleMetadata>(None, b"", &mut buffer).unwrap_err();
    assert!(matches!(err, DecodeError::Deserialize(_)));
}

fn create_sample_buffer(encryption: Encryption) -> (Vec<u8>, Option<encrypt::Cipher>) {
    let metadata = SampleMetadata { id: 4 };
    let mut buffer = vec![0; 8 << 10];

    let cipher = if encryption == Encryption::Enabled {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        Some(XChaCha20Poly1305::new(&key))
    } else {
        None
    };

    encode_metadata(cipher.as_ref(), b"", &metadata, &mut buffer)
        .expect("encode page correctly");

    (buffer, cipher)
}
