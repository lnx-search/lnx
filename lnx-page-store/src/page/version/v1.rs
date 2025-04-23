use std::fmt::Formatter;
use super::VersionProcessor;
use anyhow::Result;
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