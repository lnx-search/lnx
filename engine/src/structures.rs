use tantivy::DateTime;
use tantivy::schema::{
    IntOptions,
    TextOptions,
    BytesOptions,
    Cardinality,
    Schema as InternalSchema,
    SchemaBuilder as InternalSchemaBuilder,
    TextFieldIndexing,
};

use hashbrown::{HashSet, HashMap};
use serde::{Serialize, Deserialize};


/// A declared schema field type.
///
/// Each field has a set of relevant options as specified
/// by the tantivy docs.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum FieldDeclaration {
    /// A f64 field with given options
    F64(IntOptions),

    /// A u64 field with given options.
    U64(IntOptions),

    /// A I64 field with given options.
    I64(IntOptions),

    /// A Datetime<Utc> field with given options.
    ///
    /// This is treated as a u64 integer timestamp.
    Date(IntOptions),

    /// A string field with given options.
    String(TextOptions),

    /// A arbitrary bytes field with given options.
    Bytes(BytesOptions),
}


#[derive(Serialize, Deserialize)]
pub struct IndexDeclaration<'a> {
    name: &'a str,
    fields: HashMap<&'a str, FieldDeclaration>
}

impl<'a> IndexDeclaration<'a> {
    pub fn into_schema(self) -> Schema {
        let mut schema = InternalSchemaBuilder::new();

        for (name, field) in self.fields {
            match field {
                FieldDeclaration::F64(opts) =>
                    schema.add_f64_field(name, opts),
                FieldDeclaration::U64(opts) =>
                    schema.add_u64_field(name, opts),
                FieldDeclaration::I64(opts) =>
                    schema.add_f64_field(name, opts),
                FieldDeclaration::Date(opts) =>
                    schema.add_date_field(name, opts),
                FieldDeclaration::String(opts) =>
                    schema.add_text_field(name, opts),
                FieldDeclaration::Bytes(opts) =>
                    schema.add_bytes_field(name, opts),
            };
        }

        Schema {
            name: self.name.into(),
            inner: schema.build(),
        }
    }
}


pub struct Schema {
    name: String,
    inner: InternalSchema,
}
