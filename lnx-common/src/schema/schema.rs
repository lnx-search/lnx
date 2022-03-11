use hashbrown::{HashMap, HashSet};
use lnx_utils::Validator;
use serde::{Deserialize, Serialize};
use tantivy::schema::{FieldEntry, FAST, INDEXED, STORED};

use super::boost::BoostFactor;
use super::field_info::FieldInfo;
use super::field_name::FieldName;
use crate::schema::error::SchemaError;
use crate::schema::validations::FieldValidator;
use crate::schema::{ConstraintViolation, INDEX_PK, SEGMENT_KEY};
use crate::types::document::{DocField, Document, TypeSafeDocument};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl Schema {
    pub fn field_eq(&self, other: &Self) -> bool {
        self.fields.iter().all(|(name, info)| {
            other
                .fields
                .get(name)
                .map(|v| v == info)
                .unwrap_or_default()
        })
    }

    pub fn search_fields_eq(&self, other: &Self) -> bool {
        self.search_fields
            .iter()
            .all(|name| other.search_fields.iter().any(|n| n == name))
    }

    pub fn boost_fields_eq(&self, other: &Self) -> bool {
        self.boost_fields.iter().all(|(name, boost)| {
            other
                .boost_fields
                .get(name)
                .map(|v| v == boost)
                .unwrap_or_default()
        })
    }
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
            self.search_fields = self
                .fields
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

        self.required_fields =
            HashSet::from_iter(self.fields.iter().filter_map(|(name, info)| {
                if info.is_required() {
                    Some(name.to_string())
                } else {
                    None
                }
            }));

        self.multi_value_fields =
            HashSet::from_iter(self.fields.iter().filter_map(|(name, info)| {
                if info.is_multi() {
                    Some(name.to_string())
                } else {
                    None
                }
            }));

        Ok(())
    }
}

impl Schema {
    #[inline]
    pub fn has_field(&self, field_name: &FieldName) -> bool {
        self.fields.contains_key(field_name)
    }

    pub fn indexed_fields(&self) -> Vec<String> {
        self.fields
            .iter()
            .filter(|(_, v)| v.is_indexed())
            .map(|(k, _)| k.to_string())
            .collect()
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

    pub fn as_tantivy_schema(&self) -> tantivy::schema::Schema {
        let mut schema = tantivy::schema::SchemaBuilder::new();

        let fields = self.fields.iter().filter(|(_, v)| v.is_indexed());
        schema.add_bytes_field(INDEX_PK, FAST | STORED | INDEXED);
        schema.add_i64_field(SEGMENT_KEY, FAST | STORED | INDEXED);

        for (name, info) in fields {
            schema.add_field(FieldEntry::new(name.to_string(), info.as_field_type()));
        }

        schema.build()
    }

    pub fn validate_with_tantivy_schema(
        &self,
        schema: &tantivy::schema::Schema,
    ) -> Result<(), SchemaError> {
        let fields = self
            .fields
            .iter()
            .filter(|(_, v)| v.is_indexed())
            .map(|(k, _)| k);

        for name in fields {
            if schema.get_field(name).is_none() {
                return Err(SchemaError::CorruptedSchema);
            };
        }

        Ok(())
    }

    pub fn validate_document(
        &self,
        mut document: Document,
    ) -> Result<TypeSafeDocument, Vec<ConstraintViolation>> {
        let mut validated_fields = Vec::with_capacity(self.fields().len());
        let mut violations = vec![];
        for (name, info) in self.fields() {
            if let Some(violation) =
                check_field(name, info, &mut document, &mut validated_fields)
            {
                violations.push(violation);
            }
        }

        if violations.is_empty() {
            Ok(TypeSafeDocument(validated_fields))
        } else {
            Err(violations)
        }
    }
}

fn check_field(
    name: &FieldName,
    info: &FieldInfo,
    doc: &mut Document,
    validated_fields: &mut Vec<(String, DocField)>,
) -> Option<ConstraintViolation> {
    let field = match doc.remove(name) {
        None => {
            return if info.is_required() {
                Some(ConstraintViolation::MissingRequiredField(name.to_string()))
            } else {
                validated_fields.push((name.to_string(), info.default_value()));
                None
            }
        },
        Some(field) => {
            if !info.is_multi() && field.is_multi() {
                return Some(ConstraintViolation::TooManyValues(
                    name.to_string(),
                    field.len(),
                ));
            }

            field
        },
    };

    if let Some(fail) = info.validate(&field) {
        Some(ConstraintViolation::ValidatorError(name.to_string(), fail))
    } else {
        match field.cast_into_schema_type(info) {
            Ok(converted) => {
                validated_fields.push((name.to_string(), converted));
                None
            },
            Err(e) => Some(ConstraintViolation::TypeError(name.to_string(), e)),
        }
    }
}
