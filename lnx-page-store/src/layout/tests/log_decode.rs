use chacha20poly1305::aead::Key;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305};

use crate::layout::encrypt;
use crate::layout::log::*;
use crate::{PageFileId, PageId};

fn sample_log_block() -> LogBlock {
    let mut block = LogBlock::default();
    block
        .push_entry(
            LogEntry {
                sequence_id: 1,
                transaction_id: 3,
                transaction_n_entries: 4,
                page_id: PageId(5),
                page_file_id: PageFileId(6),
                op: LogOp::Write,
            },
            None,
        )
        .unwrap();
    block
}

#[test]
fn test_log_decode_buffer_wrong_size() {
    let block = sample_log_block();

    let mut buffer = [0; 1024];
    encode_log_block(None, b"", &block, &mut buffer[..512])
        .expect("log entry should be encoded successfully");

    let err = decode_log_block(None, b"", &mut buffer)
        .expect_err("entry should fail to decode");
    assert_eq!(
        err.to_string(),
        DecodeLogBlockError::BufferWrongSize.to_string()
    );
}

#[rstest::rstest]
#[case::decode_with_crc32_verification(sample_log_block(), false)]
#[case::decode_with_encryption(sample_log_block(), true)]
fn test_log_decoding(#[case] entry: LogBlock, #[case] encrypt_enable: bool) {
    let cipher = if encrypt_enable {
        Some(cipher_1())
    } else {
        None
    };

    let mut buffer = [0; 512];
    encode_log_block(cipher.as_ref(), b"", &entry, &mut buffer)
        .expect("log entry should be encoded successfully");

    let entry = decode_log_block(cipher.as_ref(), b"", &mut buffer)
        .map_err(|e| {
            eprintln!("{e}");
            e
        })
        .expect("entry should be decoded successfully");
    assert_eq!(entry.num_entries(), 1);
}

#[rstest::rstest]
#[case::decode_with_crc32_verification_fail(
    sample_log_block(),
    None,
    None,
    Some(b"overwrite".as_ref()),
    DecodeLogBlockError::VerificationFail,
)]
#[case::decode_with_decryption_fail(
    sample_log_block(),
    Some(cipher_1()),
    Some(cipher_2()),
    None,
    DecodeLogBlockError::DecryptionFail
)]
fn test_log_decoding_errors(
    #[case] entry: LogBlock,
    #[case] encode_cipher: Option<encrypt::Cipher>,
    #[case] decode_cipher: Option<encrypt::Cipher>,
    #[case] overwrite_digest: Option<&[u8]>,
    #[case] expected_error: DecodeLogBlockError,
) {
    let mut buffer = [0; 512];
    encode_log_block(encode_cipher.as_ref(), b"", &entry, &mut buffer)
        .expect("log entry should be encoded successfully");

    if let Some(overwrite) = overwrite_digest {
        buffer[..overwrite.len()].copy_from_slice(overwrite);
    }

    let err = decode_log_block(decode_cipher.as_ref(), b"", &mut buffer)
        .expect_err("entry should fail to decode");
    assert_eq!(err.to_string(), expected_error.to_string());
}

fn cipher_1() -> encrypt::Cipher {
    let key = Key::<XChaCha20Poly1305>::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee");
    XChaCha20Poly1305::new(key)
}

fn cipher_2() -> encrypt::Cipher {
    let key = Key::<XChaCha20Poly1305>::from_slice(b"8f4935bDBd0A771bA20fda47f44bf2bf");
    XChaCha20Poly1305::new(key)
}
