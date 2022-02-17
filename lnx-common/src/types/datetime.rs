use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use bincode::enc::Encoder;
use bincode::{Decode, Encode};
use bincode::de::Decoder;
use bincode::error::{DecodeError, EncodeError};
use tantivy::DateTime as TDateTime;
use tantivy::fastfield::FastValue;

#[derive(Debug, Clone)]
pub struct DateTime(TDateTime);

impl From<TDateTime> for DateTime {
    fn from(v: TDateTime) -> Self {
        Self(v)
    }
}

impl Display for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for DateTime {
    type Target = TDateTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Encode for DateTime {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.0.as_u64().encode(encoder)
    }
}

impl Decode for DateTime {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let slf = u64::decode(decoder)?;
        let dt = TDateTime::from_u64(slf);

        Ok(Self(dt))
    }
}

impl DateTime {
    pub fn into_inner(self) -> TDateTime {
        self.0
    }
}