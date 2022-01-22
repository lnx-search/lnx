use std::iter::FromIterator;

use anyhow::{anyhow, Error, Result};
use hashbrown::{HashMap, HashSet};
use serde::{Serialize, Deserialize};
use tantivy::schema::{Cardinality, FacetOptions, FAST, Field, FieldType, INDEXED, IndexRecordOption, IntOptions, Schema, SchemaBuilder, STORED, TextFieldIndexing, TextOptions};
use tantivy::Score;
use once_cell::sync::OnceCell;

use crate::helpers::Validate;

pub static PRIMARY_KEY: &str = "_id";

fn default_to_true() -> bool {
    true
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaContext {
    /// The index's fields.
    ///
    /// Document entries will be implicitly converted into the index's schema types
    /// if they need to be.
    fields: HashMap<String, FieldDeclaration>,

    /// The fields what are actually searched via tantivy.
    ///
    /// These values need to either be a fast field (ints) or TEXT.
    search_fields: Vec<String>,

    /// A set of fields to boost by a given factor.
    #[serde(default)]
    boost_fields: HashMap<String, Score>,

    #[serde(skip)]
    required_fields: OnceCell<HashSet<String>>,

    #[serde(skip)]
    multi_value_fields: OnceCell<HashSet<String>>,
}

impl Validate for SchemaContext {
    fn validate(&self) -> Result<()> {
        if self.search_fields.is_empty() {
            return Err(Error::msg(
                "at least one indexed field must be given to search.",
            ));
        }

        {
            let mut rejected_fields = vec![];
            for field_name in self.boost_fields.keys() {
                if !self.has_field(field_name) {
                    rejected_fields.push(field_name.to_string());
                }
            }

            if !rejected_fields.is_empty() {
                return Err(anyhow!(
                "key 'boost_fields' contain {} fields that are not defined in the schema: {}",
                rejected_fields.len(),
                rejected_fields.join(", "),
            ));
            }
        }

        {
            let mut rejected_fields = vec![];
            for field_name in self.search_fields.iter() {
                if !self.has_field(field_name) {
                    rejected_fields.push(field_name.to_string());
                }
            }

            if !rejected_fields.is_empty() {
                return Err(anyhow!(
                "key 'search_fields' contain {} fields that are not defined in the schema: {}",
                rejected_fields.len(),
                rejected_fields.join(", "),
            ));
            }
        }

        let required_fields: HashSet<String> = HashSet::from_iter(
            self.fields.iter()
                .filter_map(|(name, info)|
                    if info.is_required() {
                        Some(name)
                    } else {
                        None
                    }
                )
                .cloned()
        );

        let multi_fields: HashSet<String> = HashSet::from_iter(
            self.fields.iter()
                .filter_map(|(name, info)|
                    if info.is_multi() {
                        Some(name)
                    } else {
                        None
                    }
                )
                .cloned()
        );

        let _ = self.required_fields.set(required_fields);
        let _ = self.multi_value_fields.set(multi_fields);

        Ok(())
    }
}

impl SchemaContext {
    pub fn has_field(&self, field_name: &str) -> bool {
        self.fields.contains_key(field_name)
    }

    #[inline]
    pub fn boost_fields(&self) -> &HashMap<String, Score> {
        &self.boost_fields
    }

    #[inline]
    pub fn fields(&self) -> &HashMap<String, FieldDeclaration> {
        &self.fields
    }

    /// Checks and asserts that the fields defined by Tantivy are also the same set of fields
    /// defined in the schema.
    ///
    /// These should never be off unless someone has manually modified the data.
    pub fn assert_existing_schema_matches(&self, existing: &Schema) -> Result<()> {
        let defined = self.as_tantivy_schema();
        let defined_fields: Vec<&str> = defined
            .fields()
            .map(|(f, _)| defined.get_field_name(f))
            .collect();

        let existing_fields: Vec<&str> = existing
            .fields()
            .map(|(f, _)| existing.get_field_name(f))
            .collect();

        let defined_fields_set: HashSet<&str> = HashSet::from_iter(defined_fields.into_iter());
        let existing_fields_set: HashSet<&str> = HashSet::from_iter(existing_fields.into_iter());

        let union: Vec<&str> = defined_fields_set
            .difference(&existing_fields_set)
            .map(|v| *v)
            .collect();

        if !union.is_empty() {
            Err(anyhow!(
                "expected existing schema fields to be inline with defined schema from last save. \
                If you have *not* manually edited the index data then his is a bug and should be reported. \
                Got the following miss-aligned fields: {}",
                union.join(", ")
            ))
        } else {
            Ok(())
        }
    }

    /// Validates all search fields so that they're all indexed.
    ///
    /// If the search fields contain any fields that are not indexed,
    /// the system will list all rejected fields in a Error.
    /// Or if any fields are not text.
    pub fn verify_search_fields(&self, schema: &Schema) -> Result<()> {
        let mut reject = vec![];

        for (_, entry) in schema.fields() {
            let name = entry.name().to_string();
            if !self.search_fields.contains(&name) {
                continue;
            }

            match entry.field_type() {
                FieldType::Str(_) => {},
                _ => {
                    return Err(anyhow!(
                        "search field '{}' is not a text / string field type.",
                        &name,
                    ))
                },
            }

            if !entry.is_indexed() {
                reject.push(name)
            }
        }

        if reject.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "the given search fields contain non-indexed fields, \
                 fields cannot be searched without being index. Invalid fields: {}",
                reject.join(", ")
            ))
        }
    }

    /// Gets all fields that exist in the schema and are marked as search
    /// fields.
    pub fn get_search_fields(&self, schema: &Schema) -> Vec<Field> {
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

            if let FieldType::Str(_) = entry.field_type() {
                search_fields.push(field);
            }
        }

        search_fields
    }

    /// Gets all TEXT and STRING fields that are marked at search fields.
    ///
    /// If the index uses fast-fuzzy this uses the pre-computed fields.
    pub fn get_fuzzy_search_fields(&self, schema: &Schema) -> Vec<Field> {
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

            if let FieldType::Str(_) = entry.field_type() {
                search_fields.push(field);
            }
        }

        search_fields
    }

    /// Generates a new schema from the given fields.
    pub fn as_tantivy_schema(&self) -> tantivy::schema::Schema {
        let mut schema = SchemaBuilder::new();
        schema.add_u64_field(PRIMARY_KEY, FAST | STORED | INDEXED);

        for (field, details) in self.fields.iter() {
            if field == PRIMARY_KEY {
                warn!(
                    "{} is a reserved field name due to being a primary key",
                    PRIMARY_KEY
                );
                continue;
            }

            match details {
                FieldDeclaration::U64 { opts } => {
                    schema.add_u64_field(field, opts.clone());
                },
                FieldDeclaration::I64 { opts } => {
                    schema.add_i64_field(field, opts.clone());
                },
                FieldDeclaration::F64 { opts } => {
                    schema.add_f64_field(field, opts.clone());
                },
                FieldDeclaration::Date { opts } => {
                    schema.add_date_field(field, opts.clone());
                },
                FieldDeclaration::Facet { opts } => {
                    schema.add_facet_field(field, opts.clone());
                },
                FieldDeclaration::Text { opts } => {
                    schema.add_text_field(field, opts.opts_as_text());
                },
                FieldDeclaration::String { opts } => {
                    schema.add_text_field(field, opts.opts_as_string());
                },
            }
        }

        schema.build()
    }
}

/// The base options every field can have.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct BaseFieldOptions {
    /// If the value should be compressed and stored.
    ///
    /// Any value that has stored set to true will have the field
    /// value returned when searching.
    ///
    /// Defaults to true.
    #[serde(default = "default_to_true")]
    stored: bool,

    /// If the field is multi-value.
    #[serde(default)]
    multi: bool,

    /// Is the field required to exist for a document to be valid.
    #[serde(default)]
    required: bool,
}

impl Into<FacetOptions> for BaseFieldOptions {
    fn into(self) -> FacetOptions {
        let mut opts = FacetOptions::default();

        if self.stored {
            opts = opts.set_stored();
        }

        opts
    }
}

impl BaseFieldOptions {
    fn as_raw_opts(&self) -> TextOptions {
        let mut opts = TextOptions::default();

        if self.stored {
            opts = opts.set_stored();
        }

        opts
    }

    fn opts_as_text(&self) -> TextOptions {
        let raw = self.as_raw_opts();
        raw.set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw"))
    }

    fn opts_as_string(&self) -> TextOptions {
        let raw = self.as_raw_opts();
        raw.set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
    }
}

impl Into<TextOptions> for BaseFieldOptions {
    fn into(self) -> TextOptions {
        let mut opts = TextOptions::default();

        if self.stored {
            opts = opts.set_stored();
        }

        opts
    }
}

/// A set of field options that takes into account if a field is
/// multi-value or not in order to determine the fast-field cardinality.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct CalculatedIntOptions {
    /// Should the integer be indexed to be searched?
    #[serde(default)]
    indexed: bool,

    /// Should fieldnorms be used?
    ///
    /// This is only relevant if `indexed = true`.
    /// By default this is `indexed` if left empty.
    fieldnorms: Option<bool>,

    /// Is the field a fast field?.
    ///
    /// Fast fields have a similar lookup time to an array.
    #[serde(default)]
    fast: bool,

    #[serde(flatten)]
    base: BaseFieldOptions,
}

impl Into<IntOptions> for CalculatedIntOptions {
    fn into(self) -> IntOptions {
        let mut opts = IntOptions::default();

        if self.indexed {
            opts = opts.set_indexed();
        }

        if self.base.stored {
            opts = opts.set_stored();
        }

        if self.fieldnorms.unwrap_or(self.indexed) {
            opts = opts.set_fieldnorm();
        }

        if self.fast {
            let cardinality = if self.base.multi {
                Cardinality::MultiValues
            } else {
                Cardinality::SingleValue
            };

            opts = opts.set_fast(cardinality);
        }

        opts
    }
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
    F64 {
        #[serde(flatten)]
        opts: CalculatedIntOptions,
    },

    /// A u64 field with given options.
    U64 {
        #[serde(flatten)]
        opts: CalculatedIntOptions,
    },

    /// A I64 field with given options.
    I64 {
        #[serde(flatten)]
        opts: CalculatedIntOptions,
    },

    /// A Datetime<Utc> field with given options.
    ///
    /// This is treated as a u64 integer timestamp.
    Date {
        #[serde(flatten)]
        opts: CalculatedIntOptions,
    },

    /// A string field with given options.
    ///
    /// This will be tokenized.
    Text {
        #[serde(flatten)]
        opts: BaseFieldOptions,
    },

    /// A string field with given options.
    ///
    /// This wont be tokenized.
    String {
        #[serde(flatten)]
        opts: BaseFieldOptions,
    },

    /// A facet field.
    ///
    /// This is typically represented as a path e.g. `videos/moves/ironman`
    Facet {
        #[serde(flatten)]
        opts: BaseFieldOptions,
    },
}

impl FieldDeclaration {
    #[inline]
    pub fn is_required(&self) -> bool {
        match self {
            FieldDeclaration::F64 { opts } => opts.base.required,
            FieldDeclaration::U64 { opts } => opts.base.required,
            FieldDeclaration::I64 { opts } => opts.base.required,
            FieldDeclaration::Date { opts } => opts.base.required,
            FieldDeclaration::Text { opts } => opts.required,
            FieldDeclaration::String { opts } => opts.required,
            FieldDeclaration::Facet { opts } => opts.required,
        }
    }

    #[inline]
    pub fn is_multi(&self) -> bool {
        match self {
            FieldDeclaration::F64 { opts } => opts.base.multi,
            FieldDeclaration::U64 { opts } => opts.base.multi,
            FieldDeclaration::I64 { opts } => opts.base.multi,
            FieldDeclaration::Date { opts } => opts.base.multi,
            FieldDeclaration::Text { opts } => opts.multi,
            FieldDeclaration::String { opts } => opts.multi,
            FieldDeclaration::Facet { opts } => opts.multi,
        }
    }
}
