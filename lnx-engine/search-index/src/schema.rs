use std::iter::FromIterator;

use anyhow::{anyhow, Error, Result};
use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use tantivy::schema::{
    Cardinality,
    FacetOptions,
    Field,
    FieldType,
    IndexRecordOption,
    NumericOptions,
    Schema,
    SchemaBuilder,
    TextFieldIndexing,
    TextOptions,
    FAST,
    INDEXED,
    STORED,
};
use tantivy::Score;

use crate::helpers::{Calculated, Validate};

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
    /// This can be any indexed fields, these are intern passed to the query parser
    /// as the default set of fields to use. Note that fuzzy search only uses string type
    /// fields however, which means it will ignore non-string fields in the search.
    ///
    /// Term searches are also impervious to this, instead requiring you to explicitly specify
    /// what field you want to search exactly.
    ///
    /// By default this is all indexed fields.
    #[serde(default)]
    search_fields: Vec<String>,

    /// A set of fields to boost by a given factor.
    ///
    /// The score of each field is adjusted so that it is the score of the original
    /// query multiplied by the boost factor.
    #[serde(default)]
    boost_fields: HashMap<String, Score>,

    #[serde(skip)]
    required_fields: HashSet<String>,

    #[serde(skip)]
    multi_value_fields: HashSet<String>,
}

impl Validate for SchemaContext {
    fn validate(&self) -> Result<()> {
        if self.fields.is_empty() {
            return Err(Error::msg("at least one indexed field must be defined."));
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

        // If it is empty we default to the indexed field.
        // So we know they are valid.
        if !self.search_fields.is_empty() {
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

        Ok(())
    }

    fn validate_with_schema(&self, schema: &Schema) -> Result<()> {
        self.verify_search_fields(schema)?;
        self.assert_existing_schema_matches(schema)?;

        Ok(())
    }
}

impl Calculated for SchemaContext {
    fn calculate_once(&mut self) -> Result<()> {
        if self.search_fields.is_empty() {
            self.search_fields = self
                .fields
                .iter()
                .filter_map(
                    |(name, info)| if info.is_indexed() { Some(name) } else { None },
                )
                .cloned()
                .collect();
        }

        self.required_fields = HashSet::from_iter(
            self.fields
                .iter()
                .filter_map(
                    |(name, info)| if info.is_required() { Some(name) } else { None },
                )
                .cloned(),
        );

        self.multi_value_fields = HashSet::from_iter(
            self.fields
                .iter()
                .filter_map(
                    |(name, info)| if info.is_multi() { Some(name) } else { None },
                )
                .cloned(),
        );

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

    #[inline]
    pub fn required_fields(&self) -> &HashSet<String> {
        &self.required_fields
    }

    #[inline]
    pub fn multi_value_fields(&self) -> &HashSet<String> {
        &self.multi_value_fields
    }

    /// Checks and asserts that the fields defined by Tantivy are also the same set of fields
    /// defined in the schema.
    ///
    /// These should never be off unless someone has manually modified the data.
    pub fn assert_existing_schema_matches(&self, existing: &Schema) -> Result<()> {
        let defined = self.as_tantivy_schema();
        let defined_fields = defined.fields().map(|(f, _)| defined.get_field_name(f));

        let existing_fields = existing.fields().map(|(f, _)| existing.get_field_name(f));

        let defined_fields_set: HashSet<&str> = HashSet::from_iter(defined_fields);
        let existing_fields_set: HashSet<&str> = HashSet::from_iter(existing_fields);

        let diff: Vec<&str> = defined_fields_set
            .difference(&existing_fields_set)
            .copied()
            .collect();

        if !diff.is_empty() {
            Err(anyhow!(
                "expected existing schema fields to be inline with defined schema from last save. \
                If you have *not* manually edited the index data then his is a bug and should be reported. \
                Got the following miss-aligned fields: {}",
                diff.join(", ")
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

        if reject.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "the given search fields contain non-indexed fields, \
                 fields cannot be searched without being indexed. Invalid fields: {}",
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

            search_fields.push(field);
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
    pub fn as_tantivy_schema(&self) -> Schema {
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
                    schema.add_u64_field(field, *opts);
                },
                FieldDeclaration::I64 { opts } => {
                    schema.add_i64_field(field, *opts);
                },
                FieldDeclaration::F64 { opts } => {
                    schema.add_f64_field(field, *opts);
                },
                FieldDeclaration::Date { opts } => {
                    schema.add_date_field(field, *opts);
                },
                FieldDeclaration::Facet { opts } => {
                    schema.add_facet_field(field, *opts);
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

impl From<BaseFieldOptions> for FacetOptions {
    fn from(v: BaseFieldOptions) -> Self {
        let mut opts = FacetOptions::default();

        if v.stored {
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
        raw.set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_fieldnorms(true)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
    }

    fn opts_as_string(&self) -> TextOptions {
        let raw = self.as_raw_opts();
        raw.set_indexing_options(
            TextFieldIndexing::default()
                .set_fieldnorms(true)
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::Basic),
        )
    }
}

impl From<BaseFieldOptions> for TextOptions {
    fn from(v: BaseFieldOptions) -> Self {
        let mut opts = TextOptions::default();

        if v.stored {
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

impl From<CalculatedIntOptions> for NumericOptions {
    fn from(v: CalculatedIntOptions) -> Self {
        let mut opts = NumericOptions::default();

        if v.indexed {
            opts = opts.set_indexed();
        }

        if v.base.stored {
            opts = opts.set_stored();
        }

        if v.fieldnorms.unwrap_or(v.indexed) {
            opts = opts.set_fieldnorm();
        }

        if v.fast {
            let cardinality = if v.base.multi {
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

    #[inline]
    pub fn is_indexed(&self) -> bool {
        match self {
            FieldDeclaration::F64 { opts } => opts.indexed,
            FieldDeclaration::U64 { opts } => opts.indexed,
            FieldDeclaration::I64 { opts } => opts.indexed,
            FieldDeclaration::Date { opts } => opts.indexed,
            FieldDeclaration::Text { .. } => true,
            FieldDeclaration::String { .. } => true,
            FieldDeclaration::Facet { .. } => true,
        }
    }
}
