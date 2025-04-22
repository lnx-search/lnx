use super::VersionProcessor;
use anyhow::Result;


#[derive(Default)]
/// The [VersionProcessor] for [LayoutVersion::V1](super::LayoutVersion::V1)
/// decoding and encoding.
pub struct VersionV1Processor;

impl VersionProcessor for VersionV1Processor {
    fn decode(
        &self,
        _encoded_bytes: &mut [u8], 
        _reserved_bytes: &[u8],
    ) -> Result<()> {
        Ok(())
    }

    fn encode(
        &self,
        _raw_bytes: &mut [u8],
        _reserved_bytes: &mut [u8],
    ) -> Result<()> {
        Ok(())
    }
}