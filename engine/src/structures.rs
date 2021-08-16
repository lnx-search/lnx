use tantivy::schema::{
    BytesOptions, IntOptions, Schema as InternalSchema, SchemaBuilder as InternalSchemaBuilder,
    STORED, STRING, TEXT,
};

use hashbrown::HashMap;
use serde::__private::TryFrom;
use serde::{Deserialize, Serialize};
use tantivy::{DateTime, DocAddress};

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
pub struct IndexDeclaration {
    pub(crate) name: String,
    writer_buffer: usize,
    writer_threads: Option<usize>,
    max_concurrency: u32,
    reader_threads: Option<u32>,
    search_fields: Vec<String>,
    #[serde(default)]
    boost_fields: HashMap<String, tantivy::Score>,
    storage_type: IndexStorageType,
    fields: HashMap<String, FieldDeclaration>,
}

impl IndexDeclaration {
    pub(crate) fn into_schema(self) -> LoadedIndex {
        let mut schema = InternalSchemaBuilder::new();

        for (name, field) in self.fields {
            match field {
                FieldDeclaration::F64(opts) => schema.add_f64_field(&name, opts),
                FieldDeclaration::U64(opts) => schema.add_u64_field(&name, opts),
                FieldDeclaration::I64(opts) => schema.add_f64_field(&name, opts),
                FieldDeclaration::Date(opts) => schema.add_date_field(&name, opts),
                FieldDeclaration::String { stored } => {
                    let mut opts = STRING;

                    if stored {
                        opts = opts | STORED;
                    }

                    schema.add_text_field(&name, opts)
                }
                FieldDeclaration::Text { stored } => {
                    let mut opts = TEXT;

                    if stored {
                        opts = opts | STORED;
                    }

                    schema.add_text_field(&name, opts)
                }
                FieldDeclaration::Bytes(opts) => schema.add_bytes_field(&name, opts),
            };
        }

        LoadedIndex {
            name: self.name.into(),
            writer_buffer: self.writer_buffer,
            writer_threads: self.writer_threads.unwrap_or_else(|| num_cpus::get()),
            max_concurrency: self.max_concurrency,
            reader_threads: self.reader_threads.unwrap_or(1),
            search_fields: self.search_fields,
            storage_type: self.storage_type,
            schema: schema.build(),
            boost_fields: self.boost_fields,
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

    /// The maximum searches that can be done at any one time.
    pub(crate) max_concurrency: u32,

    /// The number of reader threads to use.
    ///
    /// The current implementation is rather naive : multithreading is by splitting search
    /// into as many task as there are segments.
    /// It is powerless at making search faster if your index consists in one large segment.
    /// Also, keep in my multithreading a single query on several threads will not improve
    /// your throughput. It can actually hurt it.
    /// It will however, decrease the average response time.
    pub(crate) reader_threads: u32,

    /// The fields what are actually searched via tantivy.
    ///
    /// These values need to either be a fast field (ints) or TEXT.
    pub(crate) search_fields: Vec<String>,

    /// The storage type for the index backend.
    pub(crate) storage_type: IndexStorageType,

    /// The defined tantivy schema.
    pub(crate) schema: InternalSchema,

    /// A set of fields to boost by a given factor.
    pub(crate) boost_fields: HashMap<String, tantivy::Score>,
}

/// The mode of the query.
///
/// This can change how the system parses and handles the query.
#[derive(Debug, Copy, Clone, Deserialize)]
pub enum QueryMode {
    /// This uses the tantivy query parser.
    Normal,

    /// Processes the query via the FuzzyQuery system. (Default)
    Fuzzy,

    /// Gets documents similar to the reference document.
    MoreLikeThis,
}

impl Default for QueryMode {
    fn default() -> Self {
        Self::Fuzzy
    }
}

/// A reference address for a given document.
#[derive(Debug)]
pub struct RefAddress {
    pub(super) segment_id: u32,
    pub(super) doc_id: u32,
}

impl RefAddress {
    pub fn as_doc_address(&self) -> DocAddress {
        DocAddress {
            segment_ord: self.segment_id,
            doc_id: self.doc_id,
        }
    }
}

impl Into<String> for RefAddress {
    fn into(self) -> String {
        format!("{}-{}", self.segment_id, self.doc_id)
    }
}

impl From<DocAddress> for RefAddress {
    fn from(v: DocAddress) -> Self {
        RefAddress {
            segment_id: v.segment_ord,
            doc_id: v.doc_id,
        }
    }
}

impl TryFrom<String> for RefAddress {
    type Error = anyhow::Error;

    fn try_from(v: String) -> Result<Self, Self::Error> {
        let mut split = v.splitn(1, "-");

        let segment_id = split
            .next()
            .map(|v| Ok(v))
            .unwrap_or_else(|| Err(Self::Error::msg("invalid id")))?
            .parse::<u32>()
            .map_err(Self::Error::msg)?;

        let doc_id = split
            .next()
            .map(|v| Ok(v))
            .unwrap_or_else(|| Err(Self::Error::msg("invalid id")))?
            .parse::<u32>()
            .map_err(Self::Error::msg)?;

        Ok(RefAddress { segment_id, doc_id })
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryPayload {
    /// A query string for `QueryMode::Fuzzy` and `QueryMode::Normal` queries.
    pub(crate) query: Option<String>,

    /// A reference document for `QueryMode::MoreLikeThis`.
    pub(crate) ref_document: Option<String>,

    /// The query mode which determines which query system will be
    /// used.
    #[serde(default)]
    pub(crate) mode: QueryMode,

    /// The amount of results to limit by, the default is 20.
    #[serde(default = "default_query_data::default_limit")]
    pub(crate) limit: usize,

    /// The amount of results to limit by, the default is 20.
    #[serde(default = "default_query_data::default_offset")]
    pub(crate) offset: usize,

    /// The field to order content by, this has to be a fast field if
    /// not `None`.
    pub(crate) order_by: Option<String>,
}

mod default_query_data {
    pub fn default_limit() -> usize {
        20
    }

    pub fn default_offset() -> usize {
        0
    }
}

/// A set of values that can be used to extract a `Term`.
///
/// This system is designed to handle JSON based deserialization
/// so Bytes and datetime are handled as base64 encoded strings and u64 timestamps
/// respectively.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type", content = "value")]
pub enum FieldValue {
    /// A signed 64 bit integer.
    I64(i64),

    /// A 64 bit floating point number.
    F64(f64),

    /// A unsigned 64 bit integer.
    U64(u64),

    /// A datetime field, deserialized as a u64 int.
    #[serde(with = "deserialize_datetime")]
    Datetime(DateTime),

    /// A text field.
    Text(String),

    /// A bytes field, deserialized as a base64 encoded string.
    #[serde(with = "deserialize_base64")]
    Bytes(Vec<u8>),
}

mod deserialize_datetime {
    use serde::{Deserialize, Deserializer};
    use tantivy::fastfield::FastValue;
    use tantivy::DateTime;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = u64::deserialize(deserializer)?;
        Ok(DateTime::from_u64(s))
    }
}

mod deserialize_base64 {
    use serde::de::Error;
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        base64::decode(s).map_err(D::Error::custom)
    }
}
