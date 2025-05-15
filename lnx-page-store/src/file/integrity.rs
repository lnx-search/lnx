//! Attach integrity bytes to the start of a buffer and verify those bytes
//! when decoding a buffer.
//!
//! This system provides both HMAC & SHA256 integrity checks for encryption at REST being
//! enabled/disabled respectively.
//!
//! For the purposes of upgrades, the system can relax HMAC checks to treat it like a checksum,
//! but this SHOULD NEVER BE ENABLED IN PRODUCTION.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
const PREFIX_SIZE: usize = 32;

/// Prefix the given output buffer with a set of check bytes
/// containing either a HMAC or SHA256 digest depending on
/// if a HMAC key is provided or not.
pub fn copy_with_check_bytes(
    input_buffer: &[u8],
    output_buffer: &mut [u8],
    hmac_key: Option<&[u8]>,
) {
    assert!(output_buffer.len() >= PREFIX_SIZE + input_buffer.len());

    if let Some(key) = hmac_key {
        let mut mac =
            HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(input_buffer);
        let result = mac.finalize().into_bytes();
        output_buffer[..PREFIX_SIZE].copy_from_slice(result.as_slice());
    } else {
        let result = hash_sha256(input_buffer);
        output_buffer[..PREFIX_SIZE].copy_from_slice(result.as_slice());
    }

    output_buffer[PREFIX_SIZE..].copy_from_slice(input_buffer);
}

/// Check the HMAC at the start of the buffer aligns with the calculated HMAC
/// with the provided key.
///
/// Returns `false` if the HMAC could not be verified.
pub fn verify_hmac_buffer<'buf>(
    buffer: &'buf [u8],
    hmac_key: &[u8],
) -> (bool, &'buf [u8]) {
    if buffer.len() < PREFIX_SIZE {
        return (false, buffer);
    }

    let hmac = &buffer[..PREFIX_SIZE];
    let bytes = &buffer[PREFIX_SIZE..];

    let mut mac =
        HmacSha256::new_from_slice(hmac_key).expect("HMAC can take key of any size");
    mac.update(bytes);
    (mac.verify_slice(hmac).is_ok(), bytes)
}

/// Check the SHA256 checksum at the start of the buffer aligns with the calculated checksum.
///
/// Returns `false` if the checksums did not match
pub fn verify_sha256_buffer<'buf>(buffer: &'buf [u8]) -> (bool, &'buf [u8]) {
    if buffer.len() < PREFIX_SIZE {
        return (false, buffer);
    }

    let sha256 = &buffer[..PREFIX_SIZE];
    let bytes = &buffer[PREFIX_SIZE..];

    let result = hash_sha256(bytes);
    (result.as_slice() == sha256, bytes)
}

/// Check if the SHA256 or HMAC value at the start of the buffer matches the expected
/// value or not.
///
/// # DANGER!
/// This method _DOES NOT VERIFY THE AUTHENTICATION OF THE HMAC_ it simply turns it to a glorified
/// checksum.
pub fn dangerous_relaxed_hmac_or_sha256_check_buffer<'buf>(
    buffer: &'buf [u8],
    hmac_key: Option<&[u8]>,
) -> (bool, &'buf [u8]) {
    if buffer.len() < PREFIX_SIZE {
        return (false, buffer);
    }

    let hmac_or_sha256 = &buffer[..PREFIX_SIZE];
    let bytes = &buffer[PREFIX_SIZE..];

    if let Some(key) = hmac_key {
        let mut mac =
            HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(bytes);
        if mac.verify_slice(hmac_or_sha256).is_ok() {
            return (true, bytes);
        }
    }

    let result = hash_sha256(bytes);
    (result.as_slice() == hmac_or_sha256, bytes)
}

fn hash_sha256(data: &[u8]) -> sha2::digest::Output<Sha256> {
    use sha2::Digest;
    let mut digest = Sha256::default();
    digest.update(data);
    digest.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::no_hmac(b"hello, world!".as_ref(), 45, None)]
    #[case::hmac(b"hello, world!".as_ref(), 45, Some(b"test".as_ref()))]
    #[should_panic]
    #[case::no_hmac_too_small_buffer(b"hello, world!".as_ref(), 32, Some(b"test".as_ref()))]
    #[should_panic]
    #[case::hmac_too_small_buffer(b"hello, world!".as_ref(), 32, None)]
    fn test_copy_with_check_bytes(
        #[case] input_buffer: &[u8],
        #[case] output_buffer_len: usize,
        #[case] hmac_key: Option<&[u8]>,
    ) {
        let mut output = vec![0; output_buffer_len];
        copy_with_check_bytes(input_buffer, &mut output, hmac_key);
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
        let mut output = vec![0; input_buffer.len() + 32];
        copy_with_check_bytes(input_buffer, &mut output, Some(sign_hmac));

        let (verified, buffer) = verify_hmac_buffer(&output, verify_hmac);
        assert_eq!(buffer, input_buffer);
        assert_eq!(verified, should_be_valid);
    }

    #[rstest::rstest]
    #[case::simple_matching_sha(b"hello, world!", None, true)]
    #[case::simple_different_sha(b"hello, world!", Some(b"other".as_ref()), false)]
    #[case::empty_payload_matching_sha(b"", None, true)]
    #[case::empty_payload_different_sha(b"", Some(b"other".as_ref()), false)]
    fn test_verify_sha256_buffer(
        #[case] input_buffer: &[u8],
        #[case] overwrite_digest: Option<&[u8]>,
        #[case] should_be_valid: bool,
    ) {
        let mut output = vec![0; input_buffer.len() + 32];
        copy_with_check_bytes(input_buffer, &mut output, None);

        if let Some(overwrite) = overwrite_digest {
            output[..overwrite.len()].copy_from_slice(overwrite);
        }

        let (verified, buffer) = verify_sha256_buffer(&output);
        assert_eq!(buffer, input_buffer);
        assert_eq!(verified, should_be_valid);
    }

    #[rstest::rstest]
    #[case::simple_matching_hmac(b"hello, world!", Some(b"test".as_ref()), Some(b"test".as_ref()), None, true)]
    #[case::simple_different_hmac(b"hello, world!", Some(b"test".as_ref()), Some(b"other".as_ref()), None, false)]
    #[case::empty_payload_matching_hmac(b"", Some(b"test".as_ref()), Some(b"test".as_ref()), None, true)]
    #[case::empty_payload_different_hmac(b"", Some(b"test".as_ref()), Some(b"other".as_ref()), None, false)]
    #[case::simple_matching_sha(b"hello, world!", None, None, None, true)]
    #[case::simple_different_sha(b"hello, world!", None, None, Some(b"other".as_ref()), false)]
    #[case::empty_payload_matching_sha(b"", None, None, None, true)]
    #[case::empty_payload_different_sha(b"", None, None, Some(b"other".as_ref()), false)]
    #[case::simple_matching_either(b"hello, world!", None, Some(b"test".as_ref()), None, true)]
    #[case::simple_different_either(b"hello, world!", None, Some(b"test".as_ref()), Some(b"other".as_ref()), false)]
    #[case::empty_payload_different_either(b"", None, Some(b"test".as_ref()), Some(b"other".as_ref()), false)]
    fn test_dangerous_relaxed_hmac_or_sha256_check_buffer(
        #[case] input_buffer: &[u8],
        #[case] sign_hmac: Option<&[u8]>,
        #[case] verify_hmac: Option<&[u8]>,
        #[case] overwrite_digest: Option<&[u8]>,
        #[case] should_be_valid: bool,
    ) {
        let mut output = vec![0; input_buffer.len() + 32];
        copy_with_check_bytes(input_buffer, &mut output, sign_hmac);

        if let Some(overwrite) = overwrite_digest {
            output[..overwrite.len()].copy_from_slice(overwrite);
        }

        let (verified, buffer) =
            dangerous_relaxed_hmac_or_sha256_check_buffer(&output, verify_hmac);
        assert_eq!(buffer, input_buffer);
        assert_eq!(verified, should_be_valid);
    }
}
