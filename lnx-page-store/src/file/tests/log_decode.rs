use rkyv::rancor;

use crate::PageId;
use crate::file::file_metadata::Encryption;
use crate::file::log::*;

static SAMPLE_LOG_ENTRY: LogEntry = LogEntry {
    checkpoint: 0,
    page_id: PageId(1),
    transaction_id: 0,
    op: LogOp::Write,
    padding: [0; 8],
};

#[rstest::rstest]
#[case::decode_with_sha256_verification(
    &SAMPLE_LOG_ENTRY,
    None,
    Encryption::Disabled,
)]
#[case::decode_with_hmac_verification(
    &SAMPLE_LOG_ENTRY,
    Some(b"test".as_ref()),
    Encryption::Enabled,
)]
#[should_panic]
#[case::decode_with_hmac_verification(
    &SAMPLE_LOG_ENTRY,
    None,
    Encryption::Enabled,
)]
fn test_log_decoding(
    #[case] entry: &LogEntry,
    #[case] hmac_key: Option<&[u8]>,
    #[case] verification: Encryption,
) {
    let mut output = [0; LOG_ENTRY_SIZE];
    encode_log_entry(entry, &mut output, hmac_key)
        .expect("log entry should be encoded successfully");

    let entry = decode_log_entry(verification, &output, hmac_key)
        .expect("entry should be decoded successfully");
    let entry = rkyv::deserialize::<LogEntry, rancor::Error>(entry).unwrap();
    assert_eq!(entry, SAMPLE_LOG_ENTRY);
}

#[rstest::rstest]
#[case::decode_with_sha256_verification_fail(
    &SAMPLE_LOG_ENTRY,
    None,
    None,
    Some(b"overwrite".as_ref()),
    Encryption::Disabled,
    DecodeLogEntryError::VerificationFail,
)]
#[case::decode_with_hmac_verification_fail(
    &SAMPLE_LOG_ENTRY,
    Some(b"test".as_ref()),
    Some(b"other".as_ref()),
    None,
    Encryption::Enabled,
    DecodeLogEntryError::VerificationFail,
)]
fn test_log_decoding_errors(
    #[case] entry: &LogEntry,
    #[case] sign_hmac_key: Option<&[u8]>,
    #[case] verify_hmac_key: Option<&[u8]>,
    #[case] overwrite_digest: Option<&[u8]>,
    #[case] verification: Encryption,
    #[case] expected_error: DecodeLogEntryError,
) {
    let mut output = [0; LOG_ENTRY_SIZE];
    encode_log_entry(entry, &mut output, sign_hmac_key)
        .expect("log entry should be encoded successfully");

    if let Some(overwrite) = overwrite_digest {
        output[..overwrite.len()].copy_from_slice(overwrite);
    }

    let err = decode_log_entry(verification, &output, verify_hmac_key)
        .expect_err("entry should fail to decode");
    assert_eq!(err.to_string(), expected_error.to_string());
}
