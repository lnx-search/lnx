use rkyv::rancor;

use crate::PageId;
use crate::file::log::*;

static SAMPLE_LOG_ENTRY: LogEntry = LogEntry {
    checkpoint: 0,
    page_id: PageId(1),
    transaction_id: 0,
    op: LogOp::Write,
};

#[rstest::rstest]
#[case::decode_with_sha256_verification(
    &SAMPLE_LOG_ENTRY,
    None,
    DecodeVerification::Sha256,
)]
#[case::decode_with_hmac_verification(
    &SAMPLE_LOG_ENTRY,
    Some(b"test".as_ref()),
    DecodeVerification::Hmac,
)]
#[should_panic]
#[case::decode_with_hmac_verification(
    &SAMPLE_LOG_ENTRY,
    None,
    DecodeVerification::Hmac,
)]
#[case::decode_with_either_hmac_verification(
    &SAMPLE_LOG_ENTRY,
    Some(b"test".as_ref()),
    DecodeVerification::DangerousIAbsolutelyKnowWhatImDoingHmacOrSha256,
)]
#[case::decode_with_either_sha256_verification(
    &SAMPLE_LOG_ENTRY,
    None,
    DecodeVerification::DangerousIAbsolutelyKnowWhatImDoingHmacOrSha256,
)]
fn test_log_decoding(
    #[case] entry: &LogEntry,
    #[case] hmac_key: Option<&[u8]>,
    #[case] verification: DecodeVerification,
) {
    let mut output = [0; LOG_ENTRY_SIZE];
    encode_log_entry(entry, &mut output, hmac_key)
        .expect("log entry should be encoded successfully");

    let decoder = LogDecoder {
        hmac_key,
        verification,
    };

    let entry = decoder
        .decode_entry(&output)
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
    DecodeVerification::Sha256,
    DecodeLogEntryError::VerificationFail,
)]
#[case::decode_with_hmac_verification_fail(
    &SAMPLE_LOG_ENTRY,
    Some(b"test".as_ref()),
    Some(b"other".as_ref()),
    None,
    DecodeVerification::Hmac,
    DecodeLogEntryError::VerificationFail,
)]
#[case::decode_with_either_hmac_verification_fail(
    &SAMPLE_LOG_ENTRY,
    Some(b"test".as_ref()),
    Some(b"other".as_ref()),
    None,
    DecodeVerification::DangerousIAbsolutelyKnowWhatImDoingHmacOrSha256,
    DecodeLogEntryError::VerificationFail,
)]
#[case::decode_with_either_sha_verification_fail(
    &SAMPLE_LOG_ENTRY,
    None,
    None,
    Some(b"overwrite".as_ref()),
    DecodeVerification::DangerousIAbsolutelyKnowWhatImDoingHmacOrSha256,
    DecodeLogEntryError::VerificationFail,
)]
fn test_log_decoding_errors(
    #[case] entry: &LogEntry,
    #[case] sign_hmac_key: Option<&[u8]>,
    #[case] verify_hmac_key: Option<&[u8]>,
    #[case] overwrite_digest: Option<&[u8]>,
    #[case] verification: DecodeVerification,
    #[case] expected_error: DecodeLogEntryError,
) {
    let mut output = [0; LOG_ENTRY_SIZE];
    encode_log_entry(entry, &mut output, sign_hmac_key)
        .expect("log entry should be encoded successfully");

    if let Some(overwrite) = overwrite_digest {
        output[..overwrite.len()].copy_from_slice(overwrite);
    }

    let decoder = LogDecoder {
        hmac_key: verify_hmac_key,
        verification,
    };

    let err = decoder
        .decode_entry(&output)
        .expect_err("entry should fail to decode");
    assert_eq!(err.to_string(), expected_error.to_string());
}
