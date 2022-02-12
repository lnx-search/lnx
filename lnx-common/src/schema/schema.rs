use hashbrown::{HashMap, HashSet};
use serde::{Serialize, Deserialize};

use lnx_utils::Validator;

use crate::schema::error::SchemaError;
use super::field_name::FieldName;
use super::field_info::FieldInfo;
use super::boost::BoostFactor;


#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    /// The index's fields.
    ///
    /// Document entries will be implicitly converted into the index's schema types
    /// if they need to be and providing they fall within the tolerance of the
    /// converter.
    fields: HashMap<FieldName, FieldInfo>,

    #[serde(default)]
    /// The fields what are actually searched via tantivy.
    ///
    /// This can be any indexed fields, these are intern passed to the query parser
    /// as the default set of fields to use.
    ///
    /// Note that fuzzy search only uses string type fields however,
    /// which means it will ignore non-string fields in the search.
    ///
    /// Term searches are also impervious to this, instead requiring
    /// you to explicitly specify what field you want to search explicitly.
    ///
    /// By default this is all indexed fields.
    search_fields: Vec<FieldName>,

    #[serde(default)]
    /// A set of fields to boost by a given factor.
    ///
    /// The score of each field is adjusted so that it is the score of the original
    /// query multiplied by the boost factor.
    ///
    /// Note that the boost factor must be >= 0.0.
    boost_fields: HashMap<FieldName, BoostFactor>,

    #[serde(skip)]
    required_fields: HashSet<String>,

    #[serde(skip)]
    multi_value_fields: HashSet<String>,
}

impl Validator for Schema {
    type Error = SchemaError;

    fn validate(&mut self) -> Result<(), Self::Error> {
        if !self.fields.values().any(|v| v.is_indexed()) {
            return Err(SchemaError::MissingIndexedFields);
        }

        {
            let mut rejected_fields = vec![];
            for field_name in self.boost_fields.keys() {
                if !self.has_field(field_name) {
                    rejected_fields.push(field_name.to_string());
                }
            }

            if !rejected_fields.is_empty() {
                return Err(SchemaError::UnknownFields("boost", rejected_fields));
            }
        }

        // If it is empty we default to the indexed field.
        // So we know they are valid.
        if self.search_fields.is_empty() {
            self.search_fields = self.fields
                .iter()
                .filter(|(_, v)| v.is_indexed())
                .map(|(k, _)| k)
                .cloned()
                .collect();
        } else {
            let mut rejected_fields = vec![];
            for field_name in self.search_fields.iter() {
                if !self.has_field(field_name) {
                    rejected_fields.push(field_name.to_string());
                }
            }

            if !rejected_fields.is_empty() {
                return Err(SchemaError::UnknownFields("search", rejected_fields));
            }
        }

        self.required_fields = HashSet::from_iter(
            self.fields
                .iter()
                .filter_map(
                    |(name, info)| if info.is_required() {
                        Some(name.to_string())
                    } else {
                        None
                    },
                ),
        );

        self.multi_value_fields = HashSet::from_iter(
            self.fields
                .iter()
                .filter_map(
                    |(name, info)| if info.is_multi() {
                        Some(name.to_string())
                    } else {
                        None
                    },
                ),
        );

        Ok(())
    }
}


impl Schema {
    #[inline]
    pub fn has_field(&self, field_name: &FieldName) -> bool {
        self.fields.contains_key(field_name)
    }

    #[inline]
    pub fn boost_fields(&self) -> &HashMap<FieldName, BoostFactor> {
        &self.boost_fields
    }

    #[inline]
    pub fn fields(&self) -> &HashMap<FieldName, FieldInfo> {
        &self.fields
    }

    #[inline]
    pub fn required_fields(&self) -> &HashSet<String> {
        &self.required_fields
    }

    #[inline]
    pub fn multi_value_fields(&self) -> &HashSet<String> {
        &self.multi_value_fields
    }
}