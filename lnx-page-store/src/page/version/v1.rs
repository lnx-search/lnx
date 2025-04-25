use std::fmt::Formatter;

use anyhow::Result;

use super::VersionProcessor;
use crate::page::LayoutVersion;

#[derive(Default)]
/// The [VersionProcessor] for [LayoutVersion::V1]
/// decoding and encoding.
pub struct VersionV1Processor;

impl std::fmt::Debug for VersionV1Processor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Processor(V1 Layout w/Encryption at rest)")
    }
}

impl VersionProcessor for VersionV1Processor {
    fn associated_layout_version(&self) -> LayoutVersion {
        LayoutVersion::V1
    }

    fn decode(&self, _encoded_bytes: &mut [u8], _reserved_bytes: &[u8]) -> Result<()> {
        Ok(())
    }

    fn encode(&self, _raw_bytes: &mut [u8], _reserved_bytes: &mut [u8]) -> Result<()> {
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;


    #[rstest::rstest]
    #[case(10, 40)]
    #[case(0, 40)]
    #[case(7 << 10, 40)]
    #[case(51, 40)]
    #[case(51, 128)]
    #[case(51, 20)]
    #[case(12, 0)]
    fn test_buffer_encode_decode(#[case] data_len: usize, #[case] reserved_len: usize) {
        let processor = VersionV1Processor;
        assert_eq!(processor.associated_layout_version(), LayoutVersion::V1);

        let mut input_bytes = vec![1; data_len];
        let mut reserved_bytes = vec![0; reserved_len];

        processor
            .encode(&mut input_bytes, &mut reserved_bytes)
            .expect("encode data");
        assert_eq!(input_bytes, vec![1; data_len]);
        assert_eq!(reserved_bytes, vec![0; reserved_len]);

        processor
            .decode(&mut input_bytes, &reserved_bytes)
            .expect("decode data");
        assert_eq!(input_bytes, vec![1; data_len]);
        assert_eq!(reserved_bytes, vec![0; reserved_len]);
    }
}