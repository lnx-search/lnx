use tantivy::{DateTime, IndexWriter};
use tantivy::schema::{
    IntOptions,
    TextOptions,
    BytesOptions,
    Cardinality,
    Schema as InternalSchema,
    SchemaBuilder as InternalSchemaBuilder,
    TextFieldIndexing,
    TEXT,
    STORED,
    STRING,
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
    ///
    /// This will be tokenized.
    Text {
        stored: bool,
    },

    /// A string field with given options.
    ///
    /// This wont be tokenized.
    String {
        stored: bool,
    },

    /// A arbitrary bytes field with given options.
    Bytes(BytesOptions),
}


#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexStorageType {
    TempFile,
    Memory,
    FileSystem(String)
}


#[derive(Serialize, Deserialize)]
pub struct IndexDeclaration<'a> {
    name: &'a str,
    writer_buffer: usize,
    writer_threads: usize,
    storage_type: IndexStorageType,
    fields: HashMap<&'a str, FieldDeclaration>
}

impl<'a> IndexDeclaration<'a> {
    pub fn into_schema(self) -> LoadedIndex {
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
                FieldDeclaration::String { stored } => {
                    let mut opts = STRING;

                    if stored {
                        opts = opts | STORED;
                    }

                    schema.add_text_field(name, opts)
                },
                FieldDeclaration::Text { stored } => {
                    let mut opts = TEXT;

                    if stored {
                        opts = opts | STORED;
                    }

                    schema.add_text_field(name, opts)
                },
                FieldDeclaration::Bytes(opts) =>
                    schema.add_bytes_field(name, opts),
            };
        }

        LoadedIndex {
            name: self.name.into(),
            writer_buffer: self.writer_buffer,
            writer_threads: self.writer_threads,
            storage_type: self.storage_type,
            schema: schema.build(),
        }
    }
}


pub struct LoadedIndex {
    pub(crate) name: String,
    pub(crate) writer_buffer: usize,
    pub(crate) writer_threads: usize,
    pub(crate) storage_type: IndexStorageType,
    pub(crate) schema: InternalSchema,
}