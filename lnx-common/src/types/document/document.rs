use std::ops::{Deref, DerefMut};

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::field::DocField;
use crate::schema::FieldName;

pub type DocId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
/// A raw, unprocessed document.
///
/// Anything handling this type should assume that it may not
/// follow any of the schema, validations, constraints or types.
pub struct Document(pub HashMap<FieldName, DocField>);

impl DerefMut for Document {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for Document {
    type Target = HashMap<FieldName, DocField>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
/// A checked and safe document.
///
/// This document should be treated as if all fields align with the schema
/// and pass all validations, constraints and type checks.
///
/// The type of the value at the given field can be assumed to be the
/// the type of the schema field.
pub struct TypeSafeDocument(pub Vec<(String, DocField)>);

impl Deref for TypeSafeDocument {
    type Target = Vec<(String, DocField)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_parse_combo_single_value() -> Result<()> {
        let payload = json!({
            "id": "12345",
            "title": "The lion the witch and the wardrobe",
            "rating": 3.444,
            "likes": 4,
        });

        serde_json::from_value::<Document>(payload)?;

        Ok(())
    }

    #[test]
    fn test_parse_combo_multi_value() -> Result<()> {
        let payload = json!({
            "id": "12345",
            "title": ["The lion the witch and the wardrobe", "Some guy in a wardrobe with a lion"],
            "rating": [3.444, 3],
            "likes": [4, 3],
        });

        serde_json::from_value::<Document>(payload)?;

        Ok(())
    }
}
