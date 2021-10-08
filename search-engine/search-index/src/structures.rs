use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Error, Result};
use chrono::{NaiveDateTime, Utc};
use hashbrown::HashMap;
use serde::de::value::{MapAccessDeserializer, SeqAccessDeserializer};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use tantivy::directory::MmapDirectory;
use tantivy::fastfield::FastValue;
use tantivy::schema::{
    Facet,
    FacetOptions,
    FacetParseError,
    Field,
    FieldType,
    FieldValue,
    IntOptions,
    Schema,
    SchemaBuilder,
    Value,
    FAST,
    INDEXED,
    STORED,
    STRING,
    TEXT,
};
use tantivy::{DateTime, Document as InternalDocument, Index, Score};

use crate::corrections::{SymSpellCorrectionManager, SymSpellManager};
use crate::helpers::Validate;
use crate::query::QueryContext;
use crate::reader::ReaderContext;
use crate::stop_words::StopWordManager;
use crate::storage::StorageBackend;
use crate::writer::WriterContext;

pub static INDEX_STORAGE_PATH: &str = "./index-storage";
pub static INDEX_METADATA_PATH: &str = "./index-metas";
pub static PRIMARY_KEY: &str = "_id";

/// The possible index storage backends.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    /// Store the index fully in memory.
    Memory,

    /// Store the index in a temporary directory.
    TempDir,

    /// Store the index in a file system store.
    FileSystem,
}

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

    /// A facet field.
    ///
    /// This is typically represented as a path e.g. `videos/moves/ironman`
    Facet(FacetOptions),
}

fn add_boost_fields(
    schema: &Schema,
    boost_fields: &HashMap<String, Score>,
    fields: &Vec<Field>,
    fields_with_boost: &mut Vec<(Field, Score)>,
) {
    for field in fields {
        let name = schema.get_field_name(*field);
        let boost = if let Some(data) = boost_fields.get(name) {
            *data
        } else {
            0f32
        };

        fields_with_boost.push((*field, boost));
    }
}

/// A given index declaration that describes the behaviour of a new index.
#[derive(Serialize, Deserialize)]
pub struct IndexDeclaration {
    /// The name of the index.
    pub(crate) name: String,

    /// The storage type used to store index data.
    pub(crate) storage_type: StorageType,

    /// The given schema fields.
    pub(crate) fields: HashMap<String, FieldDeclaration>,

    #[serde(flatten)]
    reader_ctx: ReaderContext,

    #[serde(flatten)]
    writer_ctx: WriterContext,

    /// If set to true, this switches Tantivy's default query parser
    /// behaviour to use AND instead of OR.
    #[serde(default)]
    pub(crate) set_conjunction_by_default: bool,

    /// Whether or not to use the fast fuzzy system or not.
    ///
    /// The fast fuzzy system must be enabled on the server overall
    /// for this feature.
    #[serde(default)]
    pub(crate) use_fast_fuzzy: bool,

    /// Whether or not to strip out stop words in fuzzy queries.
    ///
    /// This only applies to the fast-fuzzy query system.
    #[serde(default)]
    pub(crate) strip_stop_words: bool,

    /// The fields what are actually searched via tantivy.
    ///
    /// These values need to either be a fast field (ints) or TEXT.
    pub(crate) search_fields: Vec<String>,

    /// A set of fields to boost by a given factor.
    #[serde(default)]
    pub(crate) boost_fields: HashMap<String, Score>,
}

impl Validate for IndexDeclaration {
    fn validate(&self) -> Result<()> {
        if self.fields.len() == 0 {
            return Err(Error::msg("index must have at least one indexed field."));
        }

        if self.search_fields.len() == 0 {
            return Err(Error::msg(
                "at least one indexed field must be given to search.",
            ));
        }

        Ok(())
    }
}

impl IndexDeclaration {
    /// Builds IndexContext from the declaration, applying any validation in
    /// the process.
    pub fn create_context(&self) -> Result<IndexContext> {
        self.validate()?;
        self.writer_ctx.validate()?;
        self.reader_ctx.validate()?;

        let path = &format!("{}/{}", INDEX_STORAGE_PATH, &self.name);
        let index = {
            if Path::new(&format!("{}/meta.json", &path)).exists() {
                info!(
                    "[ INDEX-BUILDER ] using existing data for index {}",
                    &self.name
                );
                let dir = MmapDirectory::open(path)?;
                Index::open(dir)?
            } else {
                info!(
                    "[ INDEX-BUILDER ] using blank storage setup for index {}",
                    &self.name
                );
                let schema = self.schema_from_fields();
                match self.storage_type {
                    StorageType::FileSystem => {
                        info!(
                            "[ INDEX-BUILDER ] ensuring directory exists for path: {}",
                            &path
                        );
                        std::fs::create_dir_all(&path)?;
                        Index::create_in_dir(path, schema)?
                    },
                    StorageType::TempDir => Index::create_from_tempdir(schema)?,
                    StorageType::Memory => Index::create_in_ram(schema),
                }
            }
        };

        let schema = index.schema();
        self.verify_search_fields(&schema)?;

        let query_context = {
            let default_fields = self.get_search_fields(&schema);
            let fuzzy_fields = self.get_fuzzy_search_fields(&schema);

            let mut default_fields_with_boost = Vec::with_capacity(default_fields.len());
            add_boost_fields(
                &schema,
                &self.boost_fields,
                &default_fields,
                &mut default_fields_with_boost,
            );

            let mut fuzzy_fields_with_boost = Vec::with_capacity(fuzzy_fields.len());
            add_boost_fields(
                &schema,
                &self.boost_fields,
                &fuzzy_fields,
                &mut fuzzy_fields_with_boost,
            );

            QueryContext {
                id_field: schema.get_field(PRIMARY_KEY).expect("get pk"),
                set_conjunction_by_default: self.set_conjunction_by_default,
                use_fast_fuzzy: self.use_fast_fuzzy,
                strip_stop_words: self.strip_stop_words,
                default_search_fields: default_fields_with_boost,
                fuzzy_search_fields: fuzzy_fields_with_boost,
            }
        };

        let corrections = Arc::new(SymSpellManager::new());

        let fp;
        if let StorageType::FileSystem = self.storage_type {
            fp = Some(format!("{}/{}", INDEX_METADATA_PATH, &self.name));
            if !Path::new(&fp).exists() {
                std::fs::create_dir_all(&fp)?;
            }
        } else {
            fp = None;
        }
        let storage = StorageBackend::connect(fp)?;

        Ok(IndexContext {
            name: self.name.clone(),
            storage,
            correction_manager: corrections,
            index,
            reader_ctx: self.reader_ctx.clone(),
            writer_ctx: self.writer_ctx.clone(),
            query_ctx: query_context,
            fuzzy_search_fields: self.get_fuzzy_search_fields(&schema),
            stop_words: StopWordManager::init()?,
        })
    }

    /// Validates all search fields so that they're all indexed.
    ///
    /// If the search fields contain any fields that are not indexed,
    /// the system will list all rejected fields in a Error.
    fn verify_search_fields(&self, schema: &Schema) -> Result<()> {
        let mut reject = vec![];

        for (_, entry) in schema.fields() {
            let name = entry.name().to_string();
            if !self.search_fields.contains(&name) {
                continue;
            }

            if !entry.is_indexed() {
                reject.push(name)
            }
        }

        if reject.len() == 0 {
            Ok(())
        } else {
            Err(Error::msg(format!(
                "the given search fields contain non-indexed fields, \
                 fields cannot be searched without being index. Invalid fields: {}",
                reject.join(", ")
            )))
        }
    }

    /// Gets all fields that exist in the schema and are marked as search
    /// fields.
    fn get_search_fields(&self, schema: &Schema) -> Vec<Field> {
        let mut search_fields = vec![];

        for (field, entry) in schema.fields() {
            if entry.name() == PRIMARY_KEY {
                continue;
            }

            // if it's not searchable, it's pointless having it be searched.
            if !entry.is_indexed() {
                continue;
            }

            if !self.search_fields.contains(&entry.name().to_string()) {
                continue;
            }

            search_fields.push(field);
        }

        search_fields
    }

    /// Gets all TEXT and STRING fields that are marked at search fields.
    ///
    /// If the index uses fast-fuzzy this uses the pre-computed fields.
    fn get_fuzzy_search_fields(&self, schema: &Schema) -> Vec<Field> {
        let mut search_fields = vec![];

        for (field, entry) in schema.fields() {
            // if it's not searchable, it's pointless having it be searched.
            if !entry.is_indexed() {
                continue;
            }

            let name = entry.name().to_string();
            if !self.search_fields.contains(&name) {
                continue;
            };

            match entry.field_type() {
                FieldType::Str(_) => {
                    search_fields.push(field);
                },
                _ => {},
            }
        }

        search_fields
    }

    /// Generates a new schema from the given fields.
    pub(crate) fn schema_from_fields(&self) -> Schema {
        let mut schema = SchemaBuilder::new();
        schema.add_u64_field(PRIMARY_KEY, FAST | STORED | INDEXED);

        for (field, details) in self.fields.iter() {
            if field == PRIMARY_KEY {
                continue;
            }

            match details {
                FieldDeclaration::U64(opts) => {
                    schema.add_u64_field(field, opts.clone());
                },
                FieldDeclaration::I64(opts) => {
                    schema.add_i64_field(field, opts.clone());
                },
                FieldDeclaration::F64(opts) => {
                    schema.add_f64_field(field, opts.clone());
                },
                FieldDeclaration::Date(opts) => {
                    schema.add_date_field(field, opts.clone());
                },
                FieldDeclaration::Facet(opts) => {
                    schema.add_facet_field(field, opts.clone());
                },
                FieldDeclaration::Text { stored } => {
                    let mut opts = TEXT;

                    if *stored {
                        opts = opts | STORED;
                    }

                    schema.add_text_field(field, opts);
                },
                FieldDeclaration::String { stored } => {
                    let mut opts = STRING;

                    if *stored {
                        opts = opts | STORED;
                    }

                    schema.add_text_field(field, opts);
                },
            }
        }

        schema.build()
    }
}

#[derive(Debug)]
pub struct IndexContext {
    /// The name of the index.
    pub(crate) name: String,

    /// An SQLite DB instance used for storing engine state.
    pub(crate) storage: StorageBackend,

    /// The index's custom stop words.
    pub(crate) stop_words: StopWordManager,

    /// The index's fast-fuzzy pre-processor.
    pub(crate) correction_manager: SymSpellCorrectionManager,

    /// The tantivy Index.
    pub(crate) index: Index,

    /// The context for the readers.
    pub(crate) reader_ctx: ReaderContext,

    /// The context for the writer actor.
    pub(crate) writer_ctx: WriterContext,

    /// The context for the query handler.
    pub(crate) query_ctx: QueryContext,

    /// All search fields used for fuzzy searching.
    ///
    /// This is only TEXT / STRING fields.
    pub(crate) fuzzy_search_fields: Vec<Field>,
}

impl IndexContext {
    #[inline]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Get the schema of the index.
    #[inline]
    pub(crate) fn schema(&self) -> Schema {
        self.index.schema()
    }

    /// Gets the fuzzy-query fields.
    #[inline]
    pub(crate) fn fuzzy_search_fields(&self) -> &Vec<Field> {
        self.fuzzy_search_fields.as_ref()
    }
}

/// A document value that can be processed by tantivy.
#[derive(Debug, Clone)]
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

impl DocumentValue {
    pub fn as_string(&self) -> String {
        match self {
            Self::I64(v) => format!("{}", v),
            Self::F64(v) => format!("{}", v),
            Self::U64(v) => format!("{}", v),
            Self::Datetime(v) => format!("{}", v),
            Self::Text(v) => format!("{}", v),
        }
    }
}

impl TryInto<String> for DocumentValue {
    type Error = Error;

    /// Attempts to convert the value into a `String`.
    ///
    /// This never fails.
    ///
    /// ```
    /// use std::convert::TryInto;
    /// use tantivy::DateTime;
    /// use search_index::structures::DocumentValue;
    ///
    /// let value = DocumentValue::Text("12323".into());
    /// let float: String = value.try_into().expect("parse into Sting");
    /// ```
    fn try_into(self) -> Result<String> {
        Ok(self.as_string())
    }
}

impl TryInto<DateTime> for DocumentValue {
    type Error = Error;

    /// Attempts to convert the value into a `DateTime`.
    ///
    /// ```
    /// use std::convert::TryInto;
    /// use tantivy::DateTime;
    /// use search_index::structures::DocumentValue;
    ///
    /// let value = DocumentValue::Text("12323".into());
    /// let float: DateTime = value.try_into().expect("parse into DateTime");
    ///
    /// let value = DocumentValue::Text("2002-10-02T15:00:00Z".into());
    /// let float: DateTime = value.try_into().expect("parse into DateTime");
    /// ```
    fn try_into(self) -> Result<DateTime> {
        let v = match self.clone() {
            Self::I64(v) => {
                let dt = NaiveDateTime::from_timestamp_opt(v, 0)
                    .ok_or_else(|| Error::msg("invalid i64 timestamp given"))?;
                DateTime::from_utc(dt, Utc)
            },
            Self::U64(v) => {
                let dt = NaiveDateTime::from_timestamp_opt(v as i64, 0)
                    .ok_or_else(|| Error::msg("invalid i64 timestamp given"))?;
                DateTime::from_utc(dt, Utc)
            },
            Self::F64(_) => {
                return Err(Error::msg(
                    "value cannot be interpreted as a datetime value",
                ))
            },
            Self::Datetime(v) => v,
            Self::Text(v) => {
                if let Ok(ts) = self.clone().try_into() {
                    let dt: DateTime = Self::I64(ts).try_into()?;
                    return Ok(dt);
                }

                if let Ok(ts) = self.try_into() {
                    let dt: DateTime = Self::U64(ts).try_into()?;
                    return Ok(dt);
                }

                DateTime::from_str(&v).map_err(|_| {
                    Error::msg(
                        "cannot convert value into a datetime value, \
                        datetime should be formatted in RFC 3339, a u64 \
                        timestamp or a i64 timestamp",
                    )
                })?
            },
        };

        Ok(v)
    }
}

impl TryInto<u64> for DocumentValue {
    type Error = Error;

    /// Attempts to convert the value into a `u64`.
    ///
    /// ```
    /// use std::convert::TryInto;
    /// use search_index::structures::DocumentValue;
    ///
    /// let value = DocumentValue::Text("12323".into());
    ///
    /// let float: u64 = value.try_into().expect("parse into u64");
    /// ```
    fn try_into(self) -> Result<u64> {
        let v = match self {
            Self::I64(v) => v as u64,
            Self::F64(v) => v as u64,
            Self::U64(v) => v,
            Self::Datetime(dt) => dt.as_u64(),
            Self::Text(v) => v
                .parse::<u64>()
                .map_err(|_| Error::msg("cannot convert value into u64 value"))?,
        };

        Ok(v)
    }
}

impl TryInto<i64> for DocumentValue {
    type Error = Error;

    /// Attempts to convert the value into a `i64`.
    ///
    /// ```
    /// use std::convert::TryInto;
    /// use search_index::structures::DocumentValue;
    ///
    /// let value = DocumentValue::Text("-123".into());
    ///
    /// let float: i64 = value.try_into().expect("parse into i64");
    /// ```
    fn try_into(self) -> Result<i64> {
        let v = match self {
            Self::I64(v) => v,
            Self::F64(v) => v as i64,
            Self::U64(v) => v as i64,
            Self::Datetime(_) => {
                return Err(Error::msg("value cannot be interpreted as a i64 value"))
            },
            Self::Text(v) => v
                .parse::<i64>()
                .map_err(|_| Error::msg("cannot convert value into i64 value"))?,
        };

        Ok(v)
    }
}

impl TryInto<f64> for DocumentValue {
    type Error = Error;

    /// Attempts to convert the value into a `f64`.
    ///
    /// ```
    /// use std::convert::TryInto;
    /// use search_index::structures::DocumentValue;
    ///
    /// let value = DocumentValue::Text("123.0".into());
    ///
    /// let float: f64 = value.try_into().expect("parse into f64");
    /// ```
    fn try_into(self) -> Result<f64> {
        let v = match self {
            Self::I64(_) => {
                return Err(Error::msg("value cannot be interpreted as a f64 value"))
            },
            Self::F64(v) => v,
            Self::U64(_) => {
                return Err(Error::msg("value cannot be interpreted as a f64 value"))
            },
            Self::Datetime(_) => {
                return Err(Error::msg("value cannot be interpreted as a f64 value"))
            },
            Self::Text(v) => v
                .parse::<f64>()
                .map_err(|_| Error::msg("cannot convert value into f64 value"))?,
        };

        Ok(v)
    }
}

impl TryInto<Facet> for DocumentValue {
    type Error = Error;

    /// Attempts to convert the value into a `f64`.
    ///
    /// ```
    /// use std::convert::TryInto;
    /// use tantivy::schema::Facet;
    /// use search_index::structures::DocumentValue;
    ///
    /// let value = DocumentValue::Text("/foo/bar".into());
    ///
    /// let float: Facet = value.try_into().expect("parse into facet");
    /// ```
    fn try_into(self) -> Result<Facet> {
        let facet: String = self.try_into()?;

        let facet = Facet::from_text(&facet).map_err(|e| {
            let e = match e {
                FacetParseError::FacetParseError(e) => e,
            };
            Error::msg(e)
        })?;

        Ok(facet)
    }
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
                formatter.write_str("a string, int or float")
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
                    return Ok(DocumentValue::Datetime(dt));
                }

                Ok(DocumentValue::Text(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                if let Ok(dt) = tantivy::DateTime::from_str(&v) {
                    return Ok(DocumentValue::Datetime(dt));
                }
                Ok(DocumentValue::Text(v))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

/// The possible formats for adding document values.
#[derive(Debug)]
pub enum DocumentValueOptions {
    /// A singular document value.
    Single(DocumentValue),

    /// An array of document values.
    Many(Vec<DocumentValue>),
}

impl<'de> Deserialize<'de> for DocumentValueOptions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DocumentValueOptionsVisitor;

        impl<'de> Visitor<'de> for DocumentValueOptionsVisitor {
            type Value = DocumentValueOptions;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "a string, int, float or a list of strings, ints or floats",
                )
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(DocumentValueOptions::Single(DocumentValue::I64(v)))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(DocumentValueOptions::Single(DocumentValue::U64(v)))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(DocumentValueOptions::Single(DocumentValue::F64(v)))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                if let Ok(dt) = tantivy::DateTime::from_str(&v) {
                    return Ok(DocumentValueOptions::Single(DocumentValue::Datetime(
                        dt,
                    )));
                }

                Ok(DocumentValueOptions::Single(DocumentValue::Text(
                    v.to_owned(),
                )))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                if let Ok(dt) = tantivy::DateTime::from_str(&v) {
                    return Ok(DocumentValueOptions::Single(DocumentValue::Datetime(
                        dt,
                    )));
                }

                Ok(DocumentValueOptions::Single(DocumentValue::Text(v)))
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                Vec::deserialize(SeqAccessDeserializer::new(seq))
                    .map(DocumentValueOptions::Many)
            }
        }

        deserializer.deserialize_any(DocumentValueOptionsVisitor)
    }
}

/// A key-value map matching the target index's schema.
#[derive(Debug)]
pub struct DocumentPayload(BTreeMap<String, DocumentValueOptions>);

impl DocumentPayload {
    pub(crate) fn parse_into_document(
        self,
        schema: &Schema,
    ) -> Result<InternalDocument> {
        let mut doc = InternalDocument::new();

        let field = schema.get_field(PRIMARY_KEY).ok_or_else(|| {
            Error::msg(
                "index has no field '_id' and has been invalidated (This is a bug)",
            )
        })?;

        doc.add_u64(field, rand::random::<u64>());
        for (key, values) in self.0 {
            let field = schema.get_field(&key).ok_or_else(|| {
                Error::msg(format!("field {:?} does not exist in schema", &key))
            })?;

            let entry = schema.get_field_entry(field);
            let field_type = entry.field_type();

            match values {
                DocumentValueOptions::Single(value) => {
                    Self::add_value(&key, field, field_type, value, &mut doc)?
                },
                DocumentValueOptions::Many(values) => {
                    for value in values {
                        Self::add_value(&key, field, field_type, value, &mut doc)?;
                    }
                },
            };
        }

        Ok(doc)
    }

    pub(crate) fn get_text_values(
        &self,
        schema: &Schema,
        target_fields: &Vec<Field>,
    ) -> Vec<String> {
        let mut out_fields = vec![];
        for field in target_fields {
            let field_name = schema.get_field_name(*field);

            if let Some(v) = self.0.get(field_name) {
                match v {
                    DocumentValueOptions::Single(value) => {
                        if let Ok(v) = value.clone().try_into() {
                            out_fields.push(v)
                        }
                    },
                    DocumentValueOptions::Many(values) => {
                        for value in values.clone() {
                            if let Ok(v) = value.try_into() {
                                out_fields.push(v)
                            }
                        }
                    },
                };
            }
        }

        out_fields
    }

    fn add_value(
        key: &String,
        field: Field,
        field_type: &FieldType,
        value: DocumentValue,
        doc: &mut InternalDocument,
    ) -> Result<()> {
        match field_type {
            FieldType::U64(_) => doc.add_u64(field, value.try_into()?),
            FieldType::I64(_) => doc.add_i64(field, value.try_into()?),
            FieldType::F64(_) => doc.add_f64(field, value.try_into()?),
            FieldType::Date(_) => {
                let value: DateTime = value.try_into()?;
                doc.add_date(field, &value)
            },
            FieldType::Str(_) => {
                let value: String = value.try_into()?;
                doc.add_text(field, &value)
            },
            FieldType::HierarchicalFacet(_) => {
                let facet: Facet = value.try_into()?;
                let val = FieldValue::new(field, Value::Facet(facet));
                doc.add(val)
            },
            _ => {
                return Err(Error::msg(format!(
                    "byte fields (field: {}) are not supported for document insertion",
                    key,
                )))
            },
        }

        Ok(())
    }
}

impl<'de> Deserialize<'de> for DocumentPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DocumentOptionsVisitor;

        impl<'de> Visitor<'de> for DocumentOptionsVisitor {
            type Value = DocumentPayload;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("A map of key-value pairs or a map of key-values.")
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mapping = BTreeMap::deserialize(MapAccessDeserializer::new(map))?;
                Ok(DocumentPayload(mapping))
            }
        }

        deserializer.deserialize_any(DocumentOptionsVisitor)
    }
}

/// The possible formats for uploading documents.
pub enum DocumentOptions {
    /// A singular document payload.
    Single(DocumentPayload),

    /// An array of documents acting as a bulk insertion.
    Many(Vec<DocumentPayload>),
}

impl<'de> Deserialize<'de> for DocumentOptions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DocumentOptionsVisitor;

        impl<'de> Visitor<'de> for DocumentOptionsVisitor {
            type Value = DocumentOptions;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "A single `DocumentPayload` or a list of `DocumentPayload`s",
                )
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                DocumentPayload::deserialize(MapAccessDeserializer::new(map))
                    .map(DocumentOptions::Single)
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                Vec::deserialize(SeqAccessDeserializer::new(seq))
                    .map(DocumentOptions::Many)
            }
        }

        deserializer.deserialize_any(DocumentOptionsVisitor)
    }
}

/// A individual document returned from the index.
#[derive(Debug, Serialize)]
pub struct DocumentHit {
    /// The document data itself.
    ///
    /// Any STORED fields will be returned.
    pub(crate) doc: tantivy::schema::NamedFieldDocument,

    /// The document id.
    ///
    /// This is a unique 64 bit integer that can be used
    /// to select other similar docs or the document itself.
    ///
    /// This is serialized to a string for language support.
    #[serde(with = "document_id_serializer")]
    pub(crate) document_id: u64,

    /// The computed score of the documents.
    pub(crate) score: Option<Score>,
}

mod document_id_serializer {
    use serde::Serializer;

    pub fn serialize<S>(document_id: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", document_id);
        serializer.serialize_str(&s)
    }
}

#[cfg(test)]
mod test_doc_value {
    use anyhow::Result;

    use super::*;

    #[test]
    fn test_into_raw_values_from_string() -> Result<()> {
        let sample = DocumentValue::Text("124314".into());
        let res: Result<String> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<DateTime> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<u64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<i64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<f64> = sample.clone().try_into();
        assert!(res.is_ok());

        let sample = DocumentValue::Text(format!("{}", Utc::now().timestamp()));
        let res: Result<String> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<DateTime> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<u64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<i64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<f64> = sample.clone().try_into();
        assert!(res.is_ok());

        Ok(())
    }

    #[test]
    fn test_into_raw_values_from_datetime() -> Result<()> {
        let sample = DocumentValue::Datetime(Utc::now());
        let res: Result<String> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<DateTime> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<u64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<i64> = sample.clone().try_into();
        assert!(res.is_err());

        let res: Result<f64> = sample.clone().try_into();
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_into_raw_values_from_u64() -> Result<()> {
        let sample = DocumentValue::U64(45674);
        let res: Result<String> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<DateTime> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<u64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<i64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<f64> = sample.clone().try_into();
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn test_into_raw_values_from_i64() -> Result<()> {
        let sample = DocumentValue::I64(-2354);
        let res: Result<String> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<DateTime> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<u64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<i64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<f64> = sample.clone().try_into();
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn test_into_raw_values_from_f64() -> Result<()> {
        let sample = DocumentValue::F64(234234.234);
        let res: Result<String> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<DateTime> = sample.clone().try_into();
        assert!(res.is_err());

        let res: Result<u64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<i64> = sample.clone().try_into();
        assert!(res.is_ok());

        let res: Result<f64> = sample.clone().try_into();
        assert!(res.is_ok());

        Ok(())
    }
}

#[cfg(test)]
mod test_context_builder {
    use anyhow::Result;

    use super::*;

    #[test]
    fn test_build_context_expect_err() -> Result<()> {
        let dec: IndexDeclaration = serde_json::from_value(serde_json::json!({
            "name": "test",

            // Reader context
            "reader_threads": 1,
            "max_concurrency": 1,

            // Writer context
            "writer_buffer": 64_000_000,
            "writer_threads": 12,

            "storage_type": "memory",
            "fields": {
                "title": {
                    "type": "text",
                    "stored": true
                },
                "description": {
                    "type": "string",
                    "stored": false
                },
                "count": {
                   "type": "u64",
                   "stored": true,
                   "indexed": false,
                   "fast": "single"
                },
            },

            // The query context
            "search_fields": [
                "title",
                "description",
                "count"
            ],
        }))?;

        let res = dec.create_context();
        assert!(res.is_err());
        assert_eq!(
            "the given search fields contain non-indexed fields, fields cannot be searched without being index. Invalid fields: count",
            &res.err().unwrap().to_string(),
        );
        Ok(())
    }

    #[test]
    fn test_build_context_expect_ok() -> Result<()> {
        let dec: IndexDeclaration = serde_json::from_value(serde_json::json!({
            "name": "test",

            // Reader context
            "reader_threads": 1,
            "max_concurrency": 1,

            // Writer context
            "writer_buffer": 64_000_000,
            "writer_threads": 12,

            "storage_type": "memory",
            "fields": {
                "title": {
                    "type": "text",
                    "stored": true
                },
                "description": {
                    "type": "string",
                    "stored": false
                },
                "count": {
                   "type": "u64",
                   "stored": true,
                   "indexed": true,
                   "fast": "single"
                },
            },

            // The query context
            "search_fields": [
                "title",
                "description",
                "count"
            ],
        }))?;

        let res = dec.create_context();
        assert!(res.is_ok());

        Ok(())
    }

    #[test]
    fn test_fields() -> Result<()> {
        let dec: IndexDeclaration = serde_json::from_value(serde_json::json!({
            "name": "test",

            // Reader context
            "reader_threads": 1,
            "max_concurrency": 1,

            // Writer context
            "writer_buffer": 64_000_000,
            "writer_threads": 12,

            "storage_type": "memory",
            "fields": {
                "title": {
                    "type": "text",
                    "stored": true
                },
                "description": {
                    "type": "string",
                    "stored": false
                },
                "count": {
                   "type": "u64",
                   "stored": true,
                   "indexed": true,
                   "fast": "single"
                },
            },

            // The query context
            "search_fields": [
                "title",
                "description",
                "count"
            ],
            "use_fast_fuzzy": true
        }))?;

        let res = dec.create_context();
        assert!(res.is_ok());
        let ctx = res.unwrap();

        assert_eq!(ctx.fuzzy_search_fields().len(), 2);
        assert_eq!(ctx.all_search_fields().len(), 3);

        Ok(())
    }
}
