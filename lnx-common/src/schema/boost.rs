use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Error;

#[derive(Debug, Copy, Clone)]
pub struct BoostFactor(pub f32);

impl Deref for BoostFactor {
    type Target = f32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for BoostFactor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BoostFactor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>
    {
        let score = f32::deserialize(deserializer)?;

        if score < 0.0 {
            return Err(D::Error::custom("boost factor must be greater than or equal to 0.0"))
        }

        Ok(Self(score))
    }
}