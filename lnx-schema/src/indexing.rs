use std::ops::{Deref, DerefMut};

use hashbrown::HashMap;
use tantivy::schema::Field;

#[derive(Debug, Default)]
pub struct IndexingSchema {
    /// The known schema fields with specific types.
    fields: HashMap<String, FieldType>,
    /// Should the transformer capture any unknown fields and
    /// index them as a dynamic field.
    catch_unknown_fields_as_dynamic: Option<Field>,
    /// Any fields which should be excluded from indexing.
    ///
    /// This only matters if `catch_unknown_fields_as_dynamic` is `Some`.
    exclude_fields: ExcludeFilter,
}

impl IndexingSchema {
    /// Add a new field to the indexing schema.
    pub fn add_field(&mut self, field: &str, field_type: FieldType) {
        self.fields.insert(field.to_string(), field_type);
    }

    /// Enable catching unknown fields as part of the default field.
    pub fn set_catch_unknown_fields(&mut self, field_id: Field) {
        self.catch_unknown_fields_as_dynamic = Some(field_id);
    }

    /// Excludes a field from indexing.
    pub fn exclude_field(&mut self, field: &str, filter: Option<ExcludeFilter>) {
        self.exclude_fields.insert(field.to_string(), filter);
    }

    /// Get the information of how a given indexing field should
    /// be handled.
    pub fn get_field_info(&mut self, field: &str) -> FieldInfo {
        // Do explicitly describe the field in our schema?
        if let Some(field_type) = self.fields.get(field) {
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
                FieldType::DynamicObject { field_id } => FieldInfo::Field(*field_id),
                FieldType::Object(nested_schema) => FieldInfo::Nested(nested_schema),
            };
        }

        // Check that we dont explicitly deny the field.
        if let Some(filter) = self.exclude_fields.get(field) {
            return FieldInfo::Exclude(filter.as_ref());
        }

        let dynamic_field = match self.catch_unknown_fields_as_dynamic {
            None => return FieldInfo::Exclude(None),
            Some(field_id) => field_id,
        };

        FieldInfo::Field(dynamic_field)
    }
}

#[derive(Debug)]
/// Information describing how a given field should be handled.
pub enum FieldInfo<'a> {
    /// The field should be ignored from indexing.
    Exclude(Option<&'a ExcludeFilter>),
    /// The field exists and should be indexed.
    Field(Field),
    /// The field is part of a nested schema.
    Nested(&'a IndexingSchema),
}

#[derive(Clone, Debug, Default)]
pub struct ExcludeFilter(HashMap<String, Option<ExcludeFilter>>);

impl Deref for ExcludeFilter {
    type Target = HashMap<String, Option<ExcludeFilter>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ExcludeFilter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
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
    /// The field is a nested object.
    Object(IndexingSchema),
}
