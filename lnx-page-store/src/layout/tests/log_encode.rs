use chacha20poly1305::aead::Key;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305};

use crate::PageFileId;
use crate::layout::encrypt;
use crate::layout::log::*;
use crate::layout::page_metadata::PageMetadata;

fn sample_log_block(entry: LogEntry) -> LogBlock {
    let mut block = LogBlock::default();
    block.push_entry(entry, None).unwrap();
    block
}

fn cipher_1() -> encrypt::Cipher {
    let key = Key::<XChaCha20Poly1305>::from_slice(b"F8E4FeD0098cF3Bf7968E1AC7Bbfacee");
    XChaCha20Poly1305::new(key)
}

#[rstest::rstest]
#[case::encode_op_write(
    sample_log_block(LogEntry {
        sequence_id: 0,
        last_flush_sequence_id: 0,
        transaction_id: 1,
        transaction_n_entries: 0,
        page_file_id: PageFileId(1),
        op: LogOp::Write,
    }),
    None,
)]
#[case::encode_op_free(
    sample_log_block(LogEntry {
        sequence_id: 0,
        last_flush_sequence_id: 0,
        transaction_id: 1,
        transaction_n_entries: 0,
        page_file_id: PageFileId(1),
        op: LogOp::Free,
        }),
    None,
)]
#[case::encode_op_flush(
    sample_log_block(LogEntry {
        sequence_id: 0,
        last_flush_sequence_id: 0,
        transaction_id: 1,
        transaction_n_entries: 0,
        page_file_id: PageFileId(1),
        op: LogOp::Flush,
        }),
    None,
)]
#[case::encode_op_update_table_metadata(
    sample_log_block(LogEntry {
        sequence_id: 0,
        last_flush_sequence_id: 0,
        transaction_id: 1,
        transaction_n_entries: 0,
        page_file_id: PageFileId(1),
        op: LogOp::Write,
        }),
    None,
)]
fn test_encode_log(#[case] block: LogBlock, #[case] cipher: Option<encrypt::Cipher>) {
    let mut output = [0; 512];
    encode_log_block(cipher.as_ref(), b"", &block, &mut output)
        .expect("log entry should be encoded successfully");
}

#[rstest::rstest]
#[case::buffer_too_small_crc32(32, None, EncodeLogBlockError::BufferWrongSize)]
#[case::buffer_too_big_crc32(1025, None, EncodeLogBlockError::BufferWrongSize)]
#[case::buffer_too_small_encrypt(
    32,
    Some(cipher_1()),
    EncodeLogBlockError::BufferWrongSize
)]
#[case::buffer_too_big_encrypt(
    1025,
    Some(cipher_1()),
    EncodeLogBlockError::BufferWrongSize
)]
fn test_encode_log_errors(
    #[case] buffer_size: usize,
    #[case] cipher: Option<encrypt::Cipher>,
    #[case] expected_error: EncodeLogBlockError,
) {
    let block = sample_log_block(LogEntry {
        sequence_id: 0,
        last_flush_sequence_id: 0,
        transaction_id: 1,
        transaction_n_entries: 0,
        page_file_id: PageFileId(1),
        op: LogOp::Write,
    });

    let mut output = vec![0; buffer_size];
    let error = encode_log_block(cipher.as_ref(), b"", &block, &mut output)
        .expect_err("log entry should fail to encode");
    assert_eq!(error.to_string(), expected_error.to_string());
}

#[test]
fn test_encode_block_full() {
    let entry = LogEntry {
        sequence_id: 0,
        last_flush_sequence_id: 0,
        transaction_id: 0,
        transaction_n_entries: 0,
        page_file_id: PageFileId(1),
        op: LogOp::Write,
    };

    let mut block = LogBlock::default();
    for _ in 0..8 {
        block.push_entry(entry, None).unwrap();
    }
    block
        .push_entry(entry, Some(PageMetadata::empty()))
        .unwrap();
    assert_eq!(block.remaining_capacity(), 24);

    let mut output = [0; 512];
    encode_log_block(None, b"", &block, &mut output)
        .expect("log block should be encoded successfully");
}

#[rstest::rstest]
#[case::yes_encryption_blocks_1(true, 1)]
#[case::yes_encryption_blocks_2(true, 2)]
#[case::yes_encryption_blocks_4(true, 4)]
#[case::yes_encryption_blocks_5(true, 5)]
#[case::yes_encryption_blocks_9(true, 11)]
#[should_panic(expected = "log entry should not be full")]
#[case::yes_encryption_blocks_11(true, 12)]
#[case::no_encryption_blocks_1(false, 1)]
#[case::no_encryption_blocks_2(false, 2)]
#[case::no_encryption_blocks_4(false, 4)]
#[case::no_encryption_blocks_5(false, 5)]
#[case::no_encryption_blocks_9(false, 11)]
#[should_panic(expected = "log entry should not be full")]
#[case::no_encryption_blocks_11(false, 12)]
fn test_encode_block_without_metadata(
    #[case] encryption: bool,
    #[case] n_blocks: usize,
) {
    let mut block = LogBlock::default();
    for _ in 0..n_blocks {
        let entry = LogEntry {
            sequence_id: 0,
            last_flush_sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 0,
            page_file_id: PageFileId(1),
            op: LogOp::Write,
        };
        block
            .push_entry(entry, None)
            .expect("log entry should not be full");
    }

    let cipher = if encryption { Some(cipher_1()) } else { None };
    let mut output = [0; 512];
    encode_log_block(cipher.as_ref(), b"", &block, &mut output)
        .expect("log block should be encoded successfully");
}

#[rstest::rstest]
#[case::yes_encryption_blocks_1(true, 1)]
#[case::yes_encryption_blocks_2(true, 2)]
#[case::yes_encryption_blocks_4(true, 4)]
#[should_panic(expected = "log entry should not be full")]
#[case::yes_encryption_blocks_5(true, 5)]
#[case::no_encryption_blocks_1(false, 1)]
#[case::no_encryption_blocks_2(false, 2)]
#[case::no_encryption_blocks_4(false, 4)]
#[should_panic(expected = "log entry should not be full")]
#[case::no_encryption_blocks_5(false, 5)]
fn test_encode_block_with_metadata(#[case] encryption: bool, #[case] n_blocks: usize) {
    let mut block = LogBlock::default();
    for _ in 0..n_blocks {
        let entry = LogEntry {
            sequence_id: 0,
            last_flush_sequence_id: 0,
            transaction_id: 0,
            transaction_n_entries: 0,
            page_file_id: PageFileId(1),
            op: LogOp::Write,
        };

        let metadata = PageMetadata::empty();

        block
            .push_entry(entry, Some(metadata))
            .expect("log entry should not be full");
    }

    let cipher = if encryption { Some(cipher_1()) } else { None };
    let mut output = [0; 512];
    encode_log_block(cipher.as_ref(), b"", &block, &mut output)
        .expect("log block should be encoded successfully");
}
