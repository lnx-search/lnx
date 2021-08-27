use hashbrown::HashMap;
use anyhow::{Result, Error};
use core::fmt;

use serde::{Deserialize, Serialize, Deserializer};
use serde::de::Visitor;

use std::str::FromStr;
use std::collections::BTreeMap;

use tantivy::schema::{Cardinality, Field, IntOptions, Schema as InternalSchema, SchemaBuilder as InternalSchemaBuilder, Document as InternalDocument, STORED, STRING, TEXT, FieldType};
use tantivy::{DateTime, Score};
use tantivy::fastfield::FastValue;

use crate::helpers::hash;

/// A declared schema field type.
///
/// Each field has a set of relevant options as specified
/// by the tantivy docs.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// The storage backend to store index documents in.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexStorageType {
    /// Creates a temporary file.
    TempDir,

    /// Creates the index in memory (generally only for debugging)
    Memory,

    /// Store the index in a persistence setup in a given directory.
    FileSystem,
}

#[derive(Clone, Serialize, Deserialize)]
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
    #[serde(default)]
    set_conjunction_by_default: bool,
    #[serde(default)]
    use_fast_fuzzy: bool,
    #[serde(default)]
    strip_stop_words: bool,
}

impl IndexDeclaration {
    pub(crate) fn into_schema(self) -> LoadedIndex {
        let mut indexed_text_fields = vec![];
        let mut fuzzy_search_fields = vec![];
        let mut schema = InternalSchemaBuilder::new();

        let opts = IntOptions::default()
            .set_fast(Cardinality::SingleValue)
            .set_stored()
            .set_indexed();

        schema.add_u64_field("_id", opts);

        for (name, field) in self.fields {
            if name == "_id" {
                continue;
            }

            match field {
                FieldDeclaration::F64(opts) => {
                    schema.add_f64_field(&name, opts);
                }
                FieldDeclaration::U64(opts) => {
                    schema.add_u64_field(&name, opts);
                }
                FieldDeclaration::I64(opts) => {
                    schema.add_f64_field(&name, opts);
                }
                FieldDeclaration::Date(opts) => {
                    schema.add_date_field(&name, opts);
                }
                FieldDeclaration::String { stored } => {
                    let mut opts = STRING;

                    if stored {
                        opts = opts | STORED;
                    }
                    schema.add_text_field(&name, opts);
                }
                FieldDeclaration::Text { stored } => {
                    let field = if !(self.use_fast_fuzzy && crate::correction::enabled()) {
                        let mut opts = TEXT;

                        if stored {
                            opts = opts | STORED;
                        }

                        schema.add_text_field(&name, opts)
                    } else {
                        if stored {
                            schema.add_text_field(&name, STORED);
                        }

                        indexed_text_fields.push(name.clone());

                        let id = hash(&name);
                        schema.add_text_field(&format!("_{}", id), TEXT)
                    };

                    let boost = match self.boost_fields.get(&name) {
                        Some(b) => *b,
                        None => 0f32,
                    };

                    fuzzy_search_fields.push((field, boost));
                }
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
            set_conjunction_by_default: self.set_conjunction_by_default,
            indexed_text_fields,
            fuzzy_search_fields,
            use_fast_fuzzy: self.use_fast_fuzzy,
            strip_stop_words: self.strip_stop_words,
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
    pub(crate) boost_fields: HashMap<String, Score>,

    /// If set to true, this switches Tantivy's default query parser
    /// behaviour to use AND instead of OR.
    pub(crate) set_conjunction_by_default: bool,

    /// The set of fields which are indexed.
    pub(crate) indexed_text_fields: Vec<String>,

    /// The set of fields which are indexed.
    pub(crate) fuzzy_search_fields: Vec<(Field, Score)>,

    /// Whether or not to use the fast fuzzy system or not.
    ///
    /// The fast fuzzy system must be enabled on the server overall
    /// for this feature.
    pub(crate) use_fast_fuzzy: bool,

    /// Whether or not to strip out stop words in fuzzy queries.
    ///
    /// This only applies to the fast-fuzzy query system.
    pub(crate) strip_stop_words: bool,
}

/// The mode of the query.
///
/// This can change how the system parses and handles the query.
#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
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

#[derive(Debug, Deserialize)]
pub struct QueryPayload {
    /// A query string for `QueryMode::Fuzzy` and `QueryMode::Normal` queries.
    pub(crate) query: Option<String>,

    /// A reference document for `QueryMode::MoreLikeThis`.
    pub(crate) document: Option<u64>,

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
    I64(Vec<i64>),

    /// A 64 bit floating point number.
    F64(Vec<f64>),

    /// A unsigned 64 bit integer.
    U64(Vec<u64>),

    /// A datetime field, deserialized as a u64 int.
    #[serde(with = "deserialize_datetime")]
    Datetime(Vec<DateTime>),

    /// A text field.
    Text(Vec<String>),
}

#[derive(Debug, Deserialize)]
pub struct Document(pub BTreeMap<String, Vec<DocumentValue>>);

/// A document that can be processed by tantivy.
#[derive(Debug)]
pub enum DocumentValue {
    /// A signed 64 bit integer.
    I64(i64),

    /// A 64 bit floating point number.
    F64(f64),

    /// A unsigned 64 bit integer.
    U64(u64),

    /// A datetime field, deserialized as a u64 int.
    Datetime(DateTime),

    /// A text field.
    Text(String),
}

impl<'de> Deserialize<'de> for DocumentValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = DocumentValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string or u32")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(DocumentValue::I64(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(DocumentValue::U64(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(DocumentValue::F64(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                if let Ok(dt) = tantivy::DateTime::from_str(&v) {
                    return Ok(DocumentValue::Datetime(dt))
                }

                Ok(DocumentValue::Text(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                if let Ok(dt) = tantivy::DateTime::from_str(&v) {
                    return Ok(DocumentValue::Datetime(dt))
                }
                Ok(DocumentValue::Text(v))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl Document {
    pub(crate) fn parse_into_document(self, schema: &InternalSchema) -> Result<InternalDocument> {
        let mut doc = InternalDocument::new();
        for (key, values) in self.0 {
            let field = schema.get_field(&key)
                .ok_or_else(|| Error::msg(format!("field {:?} does not exist in schema", &key)))?;

            let entry = schema.get_field_entry(field);
            let field_type = entry.field_type();

            for value in values {
                match (value, field_type) {
                    (DocumentValue::I64(v), FieldType::I64(_)) => doc.add_i64(field, v),
                    (DocumentValue::U64(v), FieldType::U64(_)) => doc.add_u64(field, v),
                    (DocumentValue::F64(v), FieldType::F64(_)) => doc.add_f64(field, v),
                    (DocumentValue::Text(v), FieldType::Str(_)) => doc.add_text(field, v),
                    (DocumentValue::Datetime(v), FieldType::Str(_)) => doc.add_text(field, v.to_string()),
                    (DocumentValue::Datetime(v), FieldType::Date(_)) => doc.add_date(field, &v),
                    (DocumentValue::U64(v), FieldType::Date(_)) => doc.add_date(field, &DateTime::from_u64(v)),
                    _ => return Err(Error::msg(format!("filed {:?} is type {:?} in schema but did not get a valid value", &key, field_type)))
                }
            }
        }

        Ok(doc)
    }
}


mod deserialize_datetime {
    use serde::{Deserialize, Deserializer};
    use tantivy::fastfield::FastValue;
    use tantivy::DateTime;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<DateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let multi = Vec::<u64>::deserialize(deserializer)?;
        let values: Vec<DateTime> = multi.iter().map(|v| DateTime::from_u64(*v)).collect();

        Ok(values)
    }
}
