use std::ops::Deref;

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

use super::field::DocField;
use crate::schema::FieldName;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document(pub HashMap<FieldName, DocField>);

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

    #[test]
    fn test_parse_pre_tokenized() -> Result<()> {
        let payload = json!({
            "id": "12345",
            "title": {
                "text": "hello",
                "tokens": [{
                    "offset_from": 0,
                    "offset_to": 5,
                    "position": 0,
                    "text": "hello",
                    "position_length": 0
                }]
            }
        });

        serde_json::from_value::<Document>(payload)?;

        Ok(())
    }
}
