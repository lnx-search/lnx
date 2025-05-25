use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
use hmac::digest::Key;

use crate::layout::encrypt;
use crate::layout::file_metadata::Encryption;
use crate::layout::page_metadata::{
    DecodeError,
    PageMetadata,
    PageMetadataX63Bock,
    decode_page_metadata_block,
    encode_page_metadata_block,
};
use crate::{PageGroupId, PageId};

const SAMPLE_PAGE_METADATA: PageMetadata = PageMetadata {
    id: PageId(1),
    checksum: 1234124,
    block: PageGroupId(1),
    revision: 123,
    data_len: 124124,
    context: [1; 40],
};

#[rstest::rstest]
#[case::encrypt_empty(true, &[])]
#[case::encrypt_one_page_start(true, &[(0, SAMPLE_PAGE_METADATA)])]
#[case::encrypt_one_page_end(true, &[(62, SAMPLE_PAGE_METADATA)])]
#[case::encrypt_one_page_middle(true, &[(34, SAMPLE_PAGE_METADATA)])]
#[case::encrypt_many_pages(true, &[(5, SAMPLE_PAGE_METADATA), (14, SAMPLE_PAGE_METADATA), (53, SAMPLE_PAGE_METADATA)])]
#[case::decrypt_empty(false, &[])]
#[case::decrypt_one_page_start(false, &[(0, SAMPLE_PAGE_METADATA)])]
#[case::decrypt_one_page_end(false, &[(62, SAMPLE_PAGE_METADATA)])]
#[case::decrypt_one_page_middle(false, &[(34, SAMPLE_PAGE_METADATA)])]
#[case::decrypt_many_pages(false, &[(5, SAMPLE_PAGE_METADATA), (14, SAMPLE_PAGE_METADATA), (53, SAMPLE_PAGE_METADATA)])]
fn test_encode_pages_metadata(
    #[case] encrypt: bool,
    #[case] pages: &[(usize, PageMetadata)],
) {
    let mut buffer = vec![0; 4 << 10];

    let cipher = if encrypt { Some(cipher_1()) } else { None };

    let mut block = PageMetadataX63Bock::empty();
    for (idx, page) in pages {
        block.pages[*idx] = *page;
    }

    encode_page_metadata_block(cipher.as_ref(), &block, &mut buffer)
        .expect("page metadata encoding failed");
}

#[rstest::rstest]
#[case::encrypt_empty(true, &[])]
#[case::encrypt_one_page_start(true, &[(0, SAMPLE_PAGE_METADATA)])]
#[case::encrypt_one_page_end(true, &[(62, SAMPLE_PAGE_METADATA)])]
#[case::encrypt_one_page_middle(true, &[(34, SAMPLE_PAGE_METADATA)])]
#[case::encrypt_many_pages(true, &[(5, SAMPLE_PAGE_METADATA), (14, SAMPLE_PAGE_METADATA), (53, SAMPLE_PAGE_METADATA)])]
#[case::decrypt_empty(false, &[])]
#[case::decrypt_one_page_start(false, &[(0, SAMPLE_PAGE_METADATA)])]
#[case::decrypt_one_page_end(false, &[(62, SAMPLE_PAGE_METADATA)])]
#[case::decrypt_one_page_middle(false, &[(34, SAMPLE_PAGE_METADATA)])]
#[case::decrypt_many_pages(false, &[(5, SAMPLE_PAGE_METADATA), (14, SAMPLE_PAGE_METADATA), (53, SAMPLE_PAGE_METADATA)])]
fn test_encode_decode_pages_metadata(
    #[case] encrypt: bool,
    #[case] pages: &[(usize, PageMetadata)],
) {
    let mut buffer = vec![0; 4 << 10];

    let cipher = if encrypt { Some(cipher_1()) } else { None };

    let mut block = PageMetadataX63Bock::empty();
    for (idx, page) in pages {
        block.pages[*idx] = *page;
    }

    encode_page_metadata_block(cipher.as_ref(), &block, &mut buffer)
        .expect("page metadata encoding failed");

    let decoded_block = decode_page_metadata_block(
        if encrypt {
            Encryption::Enabled
        } else {
            Encryption::Disabled
        },
        cipher.as_ref(),
        &mut buffer,
    )
    .expect("page metadata decode");
    assert_eq!(&*decoded_block, &block);
}

#[rstest::rstest]
#[case::encrypt(true)]
#[case::decrypt(false)]
fn test_encode_err_incorrect_buffer_size(#[case] encrypt: bool) {
    let mut buffer = vec![0; 2 << 10];

    let cipher = if encrypt { Some(cipher_1()) } else { None };
    let block = PageMetadataX63Bock::empty();

    let err = encode_page_metadata_block(cipher.as_ref(), &block, &mut buffer)
        .expect_err("page metadata encoding should fail");
    assert_eq!(err.to_string(), "provided buffer length is incorrect");
}

#[rstest::rstest]
#[case::encrypt(true)]
#[case::decrypt(false)]
fn test_decode_err_incorrect_buffer_size(#[case] encrypt: bool) {
    let mut buffer = vec![0; 4 << 10];

    let cipher = if encrypt { Some(cipher_1()) } else { None };
    let block = PageMetadataX63Bock::empty();
    encode_page_metadata_block(cipher.as_ref(), &block, &mut buffer).unwrap();

    let err = decode_page_metadata_block(
        if encrypt {
            Encryption::Enabled
        } else {
            Encryption::Disabled
        },
        cipher.as_ref(),
        &mut buffer[..2 << 10],
    )
    .expect_err("page metadata decode should fail");
    assert_eq!(err.to_string(), "provided buffer length is incorrect");
}

#[rstest::rstest]
#[case::encrypt_missing_cipher(
    Encryption::Enabled,
    Some(cipher_1()),
    None,
    DecodeError::MissingDecryptionCipher
)]
#[case::decrypt_non_encrypted_data(
    Encryption::Enabled,
    None,
    Some(cipher_1()),
    DecodeError::DecryptionFailed
)]
#[case::encrypt_missmatch_keys(
    Encryption::Enabled,
    Some(cipher_1()),
    Some(cipher_2()),
    DecodeError::DecryptionFailed
)]
#[case::checksum_missmatch(
    Encryption::Disabled,
    Some(cipher_1()),
    None,
    DecodeError::Corrupted
)]
fn test_decode_err_most_errors(
    #[case] mode: Encryption,
    #[case] encode_cipher: Option<encrypt::Cipher>,
    #[case] decode_cipher: Option<encrypt::Cipher>,
    #[case] expected_error: DecodeError,
) {
    let mut buffer = vec![0; 4 << 10];
    let block = PageMetadataX63Bock::empty();
    encode_page_metadata_block(encode_cipher.as_ref(), &block, &mut buffer).unwrap();

    let err = decode_page_metadata_block(mode, decode_cipher.as_ref(), &mut buffer)
        .expect_err("page metadata decode should fail");
    assert_eq!(err.to_string(), expected_error.to_string());
}

#[test]
fn test_empty_page_check() {
    let metadata = PageMetadata::empty();
    assert!(metadata.is_empty());
}

fn cipher_1() -> encrypt::Cipher {
    let key = Key::<XChaCha20Poly1305>::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee");
    XChaCha20Poly1305::new(key)
}

fn cipher_2() -> encrypt::Cipher {
    let key = Key::<XChaCha20Poly1305>::from_slice(b"8f4935bDBd0A771bA20fda47f44bf2bf");
    XChaCha20Poly1305::new(key)
}
