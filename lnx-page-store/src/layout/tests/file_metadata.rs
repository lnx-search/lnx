use VersionedPageFileMetadata::*;
use chacha20poly1305::aead::OsRng;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305};

use crate::layout::encrypt;
use crate::layout::file_metadata::{
    DecodeError,
    EncodeError,
    Encryption,
    PageFileMetadataV1,
    VersionedPageFileMetadata,
    decode_page_file_metadata,
    encode_page_file_metadata,
};

#[rstest::rstest]
#[case::encode_v1_no_enc(V1(PageFileMetadataV1 { encryption: Encryption::Disabled, num_pages: 1_000_000, page_size: 8 << 10 }), false)]
#[case::encode_v1_enc(V1(PageFileMetadataV1 { encryption: Encryption::Enabled, num_pages: 1_000_000, page_size: 8 << 10 }), true)]
#[should_panic]
#[case::encode_v1_sanity_check(V1(PageFileMetadataV1 { encryption: Encryption::Enabled, num_pages: 1_000_000, page_size: 8 << 10 }), false)]
fn test_encode(#[case] metadata: VersionedPageFileMetadata, #[case] encrypt: bool) {
    let mut buffer = vec![0; 8 << 10];

    let cipher = if encrypt {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        Some(XChaCha20Poly1305::new(&key))
    } else {
        None
    };

    encode_page_file_metadata(cipher.as_ref(), &metadata, &mut buffer)
        .expect("encode page correctly");
}

#[rstest::rstest]
#[case::encode_v1_buffer_too_big(4 << 10, EncodeError::IncorrectBufferSize)]
#[case::encode_v1_buffer_too_small(12 << 10, EncodeError::IncorrectBufferSize)]
fn test_encode_error(#[case] buffer_size: usize, #[case] expected_error: EncodeError) {
    let metadata = V1(PageFileMetadataV1 {
        encryption: Encryption::Disabled,
        num_pages: 1_000_000,
        page_size: 8 << 10,
    });

    let mut buffer = vec![0; buffer_size];
    let err = encode_page_file_metadata(None, &metadata, &mut buffer)
        .expect_err("metadata should not be encoded");
    assert_eq!(err.to_string(), expected_error.to_string());
}

#[rstest::rstest]
#[case::encode_v1_no_enc(V1(PageFileMetadataV1 { encryption: Encryption::Disabled, num_pages: 1_000_000, page_size: 8 << 10 }), false)]
#[case::encode_v1_enc(V1(PageFileMetadataV1 { encryption: Encryption::Enabled, num_pages: 1_000_000, page_size: 8 << 10 }), true)]
fn test_encode_decode(
    #[case] metadata: VersionedPageFileMetadata,
    #[case] encrypt: bool,
) {
    let mut buffer = vec![0; 8 << 10];

    let cipher = if encrypt {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        Some(XChaCha20Poly1305::new(&key))
    } else {
        None
    };

    encode_page_file_metadata(cipher.as_ref(), &metadata, &mut buffer)
        .expect("encode page correctly");

    let loaded_metadata = decode_page_file_metadata(cipher.as_ref(), &mut buffer)
        .expect("encoded page should be decodable in the same config");

    assert_eq!(
        loaded_metadata, metadata,
        "loaded metadata does not match serialized value"
    );
    assert_eq!(loaded_metadata.num_pages(), metadata.num_pages());
}

#[rstest::rstest]
#[case::encryption_enabled(Encryption::Enabled)]
#[case::encryption_disabled(Encryption::Disabled)]
fn test_decode_err_missing_magic_bytes(#[case] encryption: Encryption) {
    let (mut buffer, cipher) = create_sample_buffer(encryption);
    buffer[..16].fill(0);

    let err = decode_page_file_metadata(cipher.as_ref(), &mut buffer).unwrap_err();
    assert_eq!(err.to_string(), DecodeError::MissingMagicBytes.to_string());
}

#[test]
fn test_decode_err_missing_magic_bytes_empty_buf() {
    let err = decode_page_file_metadata(None, &mut []).unwrap_err();
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
        decode_page_file_metadata(cipher.as_ref(), &mut buffer[..slice_at]).unwrap_err();
    assert_eq!(
        err.to_string(),
        DecodeError::MissingEncryptionHint.to_string()
    );
}

#[test]
fn test_decode_err_missing_context() {
    let (mut buffer, cipher) = create_sample_buffer(Encryption::Disabled);

    let err = decode_page_file_metadata(cipher.as_ref(), &mut buffer[..20]).unwrap_err();
    assert_eq!(
        err.to_string(),
        DecodeError::MissingContextBytes.to_string()
    );
}

#[test]
fn test_decode_err_missing_decryption_cipher() {
    let (mut buffer, _) = create_sample_buffer(Encryption::Enabled);
    buffer[20..60].fill(0);

    let err = decode_page_file_metadata(None, &mut buffer).unwrap_err();
    assert_eq!(
        err.to_string(),
        DecodeError::MissingDecryptionCipher.to_string()
    );
}

#[test]
fn test_decode_err_encryption_fail_context_wrong() {
    let (mut buffer, cipher) = create_sample_buffer(Encryption::Enabled);
    buffer[20..60].fill(0);

    let err = decode_page_file_metadata(cipher.as_ref(), &mut buffer).unwrap_err();
    assert_eq!(err.to_string(), DecodeError::DecryptionFailed.to_string());
}

#[test]
fn test_decode_err_encryption_fail_key_wrong() {
    let (mut buffer, _) = create_sample_buffer(Encryption::Enabled);

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);

    let err = decode_page_file_metadata(Some(&cipher), &mut buffer).unwrap_err();
    assert_eq!(err.to_string(), DecodeError::DecryptionFailed.to_string());
}

#[test]
fn test_decode_err_malformed_json() {
    let (mut buffer, _) = create_sample_buffer(Encryption::Disabled);
    buffer[60..].fill(0);

    let err = decode_page_file_metadata(None, &mut buffer).unwrap_err();
    assert!(matches!(err, DecodeError::Deserialize(_)));
}

fn create_sample_buffer(encryption: Encryption) -> (Vec<u8>, Option<encrypt::Cipher>) {
    let metadata = V1(PageFileMetadataV1 {
        encryption,
        num_pages: 1_000_000,
        page_size: 8 << 10,
    });
    let mut buffer = vec![0; 8 << 10];

    let cipher = if encryption == Encryption::Enabled {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        Some(XChaCha20Poly1305::new(&key))
    } else {
        None
    };

    encode_page_file_metadata(cipher.as_ref(), &metadata, &mut buffer)
        .expect("encode page correctly");

    (buffer, cipher)
}
