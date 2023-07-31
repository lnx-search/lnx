use std::collections::hash_map::{DefaultHasher, RandomState};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};

use lnx_tools::hashers::NoOpRandomState;
use tantivy::schema::Field;

#[derive(Debug, Default, Clone)]
pub struct IndexingSchema {
    /// The random state used for
    random_state: RandomState,
    /// The known schema fields with specific types.
    fields: HashMap<u64, FieldType, NoOpRandomState>,
    /// Should the transformer capture any unknown fields and
    /// index them as a dynamic field.
    catch_unknown_fields_as_dynamic: Option<Field>,
    /// Any fields which should be excluded from indexing.
    ///
    /// This only matters if `catch_unknown_fields_as_dynamic` is `Some`.
    exclude_fields: HashSet<u64, NoOpRandomState>,
}

impl IndexingSchema {
    /// Creates a new `ahash` hasher.
    pub fn get_hasher(&self) -> DefaultHasher {
        self.random_state.build_hasher()
    }

    pub fn hash_str(&self, field: &str) -> u64 {
        let mut hasher = self.get_hasher();
        hasher.write(field.as_bytes());
        hasher.finish()
    }

    /// Add a new field to the indexing schema.
    pub fn add_field(&mut self, field: &str, field_type: FieldType) {
        let field_id = self.hash_str(field);
        self.fields.insert(field_id, field_type);
    }

    /// Enable catching unknown fields as part of the default field.
    pub fn set_catch_unknown_fields(&mut self, field_id: Field) {
        self.catch_unknown_fields_as_dynamic = Some(field_id);
    }

    /// Excludes a field from indexing.
    pub fn exclude_field(&mut self, field: &str) {
        let field_id = self.hash_str(field);
        self.exclude_fields.insert(field_id);
    }

    /// Returns if the field is excluded from indexing.
    pub fn is_excluded(&self, field_id: u64) -> bool {
        self.exclude_fields.contains(&field_id)
    }

    /// Get the information of how a given indexing field should
    /// be handled.
    pub fn get_field_info(&self, field_id: u64) -> FieldInfo {
        // Do explicitly describe the field in our schema?
        if let Some(field_type) = self.fields.get(&field_id) {
            return match field_type {
                FieldType::Text { field_id } => FieldInfo::Field(*field_id),
                FieldType::RawStr { field_id } => FieldInfo::Field(*field_id),
                FieldType::U64 { field_id } => FieldInfo::Field(*field_id),
                FieldType::I64 { field_id } => FieldInfo::Field(*field_id),
                FieldType::F64 { field_id } => FieldInfo::Field(*field_id),
                FieldType::Ip { field_id } => FieldInfo::Field(*field_id),
                FieldType::Bytes { field_id } => FieldInfo::Field(*field_id),
                FieldType::Facet { field_id } => FieldInfo::Field(*field_id),
                FieldType::Bool { field_id } => FieldInfo::Field(*field_id),
                FieldType::Datetime { field_id } => FieldInfo::Field(*field_id),
                FieldType::DynamicObject { field_id } => FieldInfo::Dynamic(*field_id),
            };
        }

        // Check that we dont explicitly deny the field.
        if self.is_excluded(field_id) {
            return FieldInfo::Exclude;
        }

        match self.catch_unknown_fields_as_dynamic {
            None => FieldInfo::Exclude,
            Some(field_id) => FieldInfo::CatchAll(field_id),
        }
    }
}

#[derive(Copy, Clone, Debug)]
/// Information describing how a given field should be handled.
pub enum FieldInfo {
    /// The field should be ignored from indexing.
    Exclude,
    /// The field exists and should be indexed.
    Field(Field),
    /// The field is a dynamic object.
    Dynamic(Field),
    /// The field is caught via a catch all.
    CatchAll(Field),
}

#[derive(Debug, Copy, Clone)]
pub enum FieldType {
    /// The field is a tokenized text field.
    Text { field_id: Field },
    /// The field is a non-tokenized text field.
    RawStr { field_id: Field },
    /// The field is a u64 integer field.
    U64 { field_id: Field },
    /// The field is a i64 integer field.
    I64 { field_id: Field },
    /// The field is a f64 integer field.
    F64 { field_id: Field },
    /// The field is a ip field.
    Ip { field_id: Field },
    /// The field is a bytes field.
    Bytes { field_id: Field },
    /// The field is a facet field.
    Facet { field_id: Field },
    /// The field is a bool field.
    Bool { field_id: Field },
    /// The field is a datetime field.
    Datetime { field_id: Field },
    /// The field is a dynamic field.
    DynamicObject { field_id: Field },
}
