use std::cmp;
use std::ops::Deref;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug)]
pub struct NumThreads(usize);

impl Deref for NumThreads {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for NumThreads {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NumThreads {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inst = usize::deserialize(deserializer)?;

        if inst < 1 {
            return Err(D::Error::custom("number of threads must be at least 1"));
        }

        Ok(Self(inst))
    }
}

impl Default for NumThreads {
    fn default() -> Self {
        Self(cmp::min(num_cpus::get(), 8))
    }
}

#[derive(Debug)]
/// A struct that allows for the buffer size to be specified in short hand.
///
/// The following format is allowed:
///
/// - nGG
/// - nMB
/// - nKB
pub struct BufferSize(usize);

impl Deref for BufferSize {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for BufferSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BufferSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inst = String::deserialize(deserializer)?;

        let size = inst
            .get(inst.len() - 3..)
            .ok_or_else(|| D::Error::custom("A kind must be provided for the size of the buffer ('10GB', '10MB', '100KB')"))?;

        let n = inst
            .get(..inst.len() - 2)
            .ok_or_else(|| {
                D::Error::custom("The number of this size must be given. E.g. '10GB'")
            })?
            .parse::<f32>()
            .map_err(D::Error::custom)?;

        let size = size.to_lowercase();

        let multiplier = match size.as_str() {
            "gb" => 1024 * 1024 * 1024,
            "mb" => 1024 * 1024,
            "kb" => 1024,
            other => {
                return Err(D::Error::custom(format!(
                    "{:?} is not a valid size of bytes.",
                    other
                )))
            },
        };

        let num_bytes = (n * multiplier as f32) as usize;

        Ok(Self(num_bytes))
    }
}

impl Default for BufferSize {
    fn default() -> Self {
        Self(250_000_000)
    }
}
