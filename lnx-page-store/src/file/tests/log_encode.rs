use crate::PageId;
use crate::file::log::*;

#[test]
fn test_ensure_log_size() {
    // 32 bytes for HMAC or SHA256
    assert_eq!(size_of::<ArchivedLogEntry>() + 32, 64);
}

#[rstest::rstest]
#[case::encode_op_write(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::Write
        },
    None,
)]
#[case::encode_op_free(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::Free
        },
    None,
)]
#[case::encode_op_commit(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::Commit
        },
    None,
)]
#[case::encode_op_update_table_metadata(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::UpdateTableMetadata
        },
    None,
)]
fn test_encode_log(#[case] entry: LogEntry, #[case] hmac_key: Option<&[u8]>) {
    let mut output = [0; LOG_ENTRY_SIZE];
    encode_log_entry(&entry, &mut output, hmac_key)
        .expect("log entry should be encoded successfully");
}

#[rstest::rstest]
#[case::buffer_too_small_sha256(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::Write
        },
    32,
    None,
    EncodeLogEntryError::BufferWrongSize,
)]
#[case::buffer_too_big_sha256(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::Free
        },
    128,
    None,
    EncodeLogEntryError::BufferWrongSize,
)]
#[case::buffer_too_small_hmac(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::Write
        },
    32,
    Some(b"hello, world!".as_ref()),
    EncodeLogEntryError::BufferWrongSize,
)]
#[case::buffer_too_big_hmac(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::Free
        },
    128,
    Some(b"hello, world!".as_ref()),
    EncodeLogEntryError::BufferWrongSize,
)]
#[case::buffer_too_small_empty_hmac_key(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::Write
        },
    32,
    Some([].as_ref()),
    EncodeLogEntryError::BufferWrongSize,
)]
#[case::buffer_too_big_empty_hmac_key(
    LogEntry {
        checkpoint: 0,
        page_id: PageId(1),
        transaction_id: 1,
        op: LogOp::Free
        },
    128,
    Some([].as_ref()),
    EncodeLogEntryError::BufferWrongSize,
)]
fn test_encode_log_errors(
    #[case] entry: LogEntry,
    #[case] buffer_size: usize,
    #[case] hmac_key: Option<&[u8]>,
    #[case] expected_error: EncodeLogEntryError,
) {
    let mut output = vec![0; buffer_size];
    let error = encode_log_entry(&entry, &mut output[..], hmac_key)
        .expect_err("log entry should reject data");
    assert_eq!(error.to_string(), expected_error.to_string());
}
