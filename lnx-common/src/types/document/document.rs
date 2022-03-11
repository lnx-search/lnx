use std::ops::{Deref, DerefMut};

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::field::DocField;
use crate::schema::FieldName;

pub type DocId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
