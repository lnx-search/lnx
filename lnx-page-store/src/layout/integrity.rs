//! Attach integrity bytes to the start of a buffer and verify those bytes
//! when decoding a buffer.
//!
//! This system provides both HMAC & CRC32 integrity checks for encryption at REST being
//! enabled/disabled respectively.

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::layout::file_metadata::Encryption;

type HmacSha256 = Hmac<Sha256>;
const INTEGRITY_PREFIX_SIZE: usize = 32;

/// Verify the integrity of the buffer.
pub fn verify(
    mode: Encryption,
    hmac_key: Option<&[u8]>,
    buffer: &[u8],
    context: &[u8],
) -> bool {
    match mode {
        Encryption::Disabled => verify_crc32_buffer(buffer, context),
        Encryption::Enabled => verify_hmac_buffer(
            buffer,
            context,
            hmac_key
                .expect("HMAC key should be provided when HMAC verification is enabled"),
        ),
    }
}

/// Prefix the given output buffer with a set of check bytes
/// containing either a HMAC or CRC32 digest depending on
/// if a HMAC key is provided or not.
pub fn write_check_bytes(
    hmac_key: Option<&[u8]>,
    input_buffer: &[u8],
    context: &mut [u8],
) {
    if let Some(key) = hmac_key {
        let mut mac =
            HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(input_buffer);
        let result = mac.finalize().into_bytes();
        context[..32].copy_from_slice(result.as_slice());
    } else {
        let result = crc32fast::hash(input_buffer);
        context[..4].copy_from_slice(&result.to_le_bytes());
    }
}

/// Check the HMAC at the start of the buffer aligns with the calculated HMAC
/// with the provided key.
///
/// Returns `false` if the HMAC could not be verified.
fn verify_hmac_buffer(buffer: &[u8], context: &[u8], hmac_key: &[u8]) -> bool {
    if context.len() < INTEGRITY_PREFIX_SIZE {
        return false;
    }

    let hmac = &context[..INTEGRITY_PREFIX_SIZE];

    let mut mac =
        HmacSha256::new_from_slice(hmac_key).expect("HMAC can take key of any size");
    mac.update(buffer);
    mac.verify_slice(hmac).is_ok()
}

/// Check the CRC32 checksum encoded in `context` with the provided `buffer`.
///
/// Returns `false` if the checksums did not match
fn verify_crc32_buffer(buffer: &[u8], context: &[u8]) -> bool {
    if context.len() < INTEGRITY_PREFIX_SIZE {
        return false;
    }

    let expected_checksum = u32::from_le_bytes(context[..4].try_into().unwrap());
    let actual_checksum = crc32fast::hash(buffer);
    actual_checksum == expected_checksum
}

#[cfg(all(test, not(feature = "test-miri")))]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::no_hmac(
        b"hello, world!".as_ref(), 
        None,
        &[
            19, 141, 152, 88,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    )]
    #[case::hmac(
        b"hello, world!".as_ref(),
        Some(b"test".as_ref()),
        &[
            16, 104, 181, 61, 182, 2, 187, 163, 56, 84, 65,
            49, 243, 82, 216, 201, 1, 241, 168, 210, 233, 120,
            115, 132, 23, 211, 2, 112, 135, 87, 134, 90,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]
    )]
    fn test_write_check_bytes(
        #[case] input_buffer: &[u8],
        #[case] hmac_key: Option<&[u8]>,
        #[case] expected_ctx: &[u8],
    ) {
        let mut context = [0; 40];
        write_check_bytes(hmac_key, input_buffer, &mut context);
        assert_eq!(context, expected_ctx);
    }

    #[rstest::rstest]
    #[case::simple_matching_hmac(b"hello, world!", b"test", b"test", true)]
    #[case::simple_different_hmac(b"hello, world!", b"test", b"other", false)]
    #[case::empty_payload_matching_hmac(b"", b"test", b"test", true)]
    #[case::empty_payload_different_hmac(b"", b"test", b"other", false)]
    fn test_verify_hmac_buffer(
        #[case] input_buffer: &[u8],
        #[case] sign_hmac: &[u8],
        #[case] verify_hmac: &[u8],
        #[case] should_be_valid: bool,
    ) {
        let mut context = [0; 40];
        write_check_bytes(Some(sign_hmac), input_buffer, &mut context);

        let verified = verify_hmac_buffer(input_buffer, &context, verify_hmac);
        assert_eq!(verified, should_be_valid);
    }

    #[rstest::rstest]
    #[case::simple_matching_crc(b"hello, world!", None, true)]
    #[case::simple_different_crc(b"hello, world!", Some(b"other".as_ref()), false)]
    #[case::empty_payload_matching_crc(b"", None, true)]
    #[case::empty_payload_different_crc(b"", Some(b"other".as_ref()), false)]
    fn test_verify_crc32_buffer(
        #[case] input_buffer: &[u8],
        #[case] overwrite_digest: Option<&[u8]>,
        #[case] should_be_valid: bool,
    ) {
        let mut context = [0; 40];
        write_check_bytes(None, input_buffer, &mut context);

        if let Some(overwrite) = overwrite_digest {
            context[..overwrite.len()].copy_from_slice(overwrite);
        }

        let verified = verify_crc32_buffer(input_buffer, &context);
        assert_eq!(verified, should_be_valid);
    }
}
