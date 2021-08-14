use tantivy::schema::{
    BytesOptions, Cardinality, IntOptions, Schema as InternalSchema,
    SchemaBuilder as InternalSchemaBuilder, TextFieldIndexing, TextOptions, STORED, STRING, TEXT,
};
use tantivy::{DateTime, IndexWriter};

use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

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
    Text { stored: bool },

    /// A string field with given options.
    ///
    /// This wont be tokenized.
    String { stored: bool },

    /// A arbitrary bytes field with given options.
    Bytes(BytesOptions),
}

/// The storage backend to store index documents in.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexStorageType {
    /// Creates a temporary file.
    TempFile,

    /// Creates the index in memory (generally only for debugging)
    Memory,

    /// Store the index in a persistence setup in a given directory.
    FileSystem(String),
}

#[derive(Serialize, Deserialize)]
pub struct IndexDeclaration<'a> {
    name: &'a str,
    writer_buffer: usize,
    writer_threads: Option<usize>,
    max_concurrency: u32,
    storage_type: IndexStorageType,
    fields: HashMap<&'a str, FieldDeclaration>,
}

impl<'a> IndexDeclaration<'a> {
    pub fn into_schema(self) -> LoadedIndex {
        let mut schema = InternalSchemaBuilder::new();

        for (name, field) in self.fields {
            match field {
                FieldDeclaration::F64(opts) => schema.add_f64_field(name, opts),
                FieldDeclaration::U64(opts) => schema.add_u64_field(name, opts),
                FieldDeclaration::I64(opts) => schema.add_f64_field(name, opts),
                FieldDeclaration::Date(opts) => schema.add_date_field(name, opts),
                FieldDeclaration::String { stored } => {
                    let mut opts = STRING;

                    if stored {
                        opts = opts | STORED;
                    }

                    schema.add_text_field(name, opts)
                }
                FieldDeclaration::Text { stored } => {
                    let mut opts = TEXT;

                    if stored {
                        opts = opts | STORED;
                    }

                    schema.add_text_field(name, opts)
                }
                FieldDeclaration::Bytes(opts) => schema.add_bytes_field(name, opts),
            };
        }

        LoadedIndex {
            name: self.name.into(),
            writer_buffer: self.writer_buffer,
            writer_threads: self.writer_threads.unwrap_or_else(|| num_cpus::get()),
            max_concurrency: self.max_concurrency,
            storage_type: self.storage_type,
            schema: schema.build(),
        }
    }
}

/// The loaded and processed index declaration.
///
/// This is used for controlling the behaviour of the
/// generated indexes, thread pools and writers.
pub struct LoadedIndex {
    /// The name of the index.
    pub(crate) name: String,

    /// The amount of bytes to allocate to the writer buffer.
    pub(crate) writer_buffer: usize,

    /// The amount of worker threads to dedicate to a writer.
    pub(crate) writer_threads: usize,
    pub(crate) max_concurrency: u32,
    pub(crate) storage_type: IndexStorageType,
    pub(crate) schema: InternalSchema,
}
