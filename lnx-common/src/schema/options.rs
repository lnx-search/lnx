use serde::{Deserialize, Serialize};
use tantivy::schema::{
    Cardinality,
    FacetOptions,
    IndexRecordOption,
    IntOptions,
    TextFieldIndexing,
    TextOptions,
};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
/// The base options every field can have.
pub struct BaseOptions {
    #[serde(default)]
    /// If the field is multi-value.
    pub multi: bool,

    #[serde(default)]
    /// Is the field required to exist for a document to be valid?
    pub required: bool,
}

impl BaseOptions {
    pub fn as_tantivy_facet_opts(&self) -> FacetOptions {
        FacetOptions::default()
    }
}

// Although not strictly necessary we have this here for consistency.
impl From<BaseOptions> for FacetOptions {
    fn from(_: BaseOptions) -> Self {
        FacetOptions::default()
    }
}

// Although not strictly necessary we have this here for consistency.
impl From<BaseOptions> for TextOptions {
    fn from(_: BaseOptions) -> Self {
        TextOptions::default()
    }
}

impl BaseOptions {
    pub fn as_raw_opts(&self) -> TextOptions {
        TextOptions::default()
    }

    pub fn opts_as_text(&self) -> TextOptions {
        let raw = self.as_raw_opts();
        raw.set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_fieldnorms(true)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
    }

    pub fn opts_as_string(&self) -> TextOptions {
        let raw = self.as_raw_opts();
        raw.set_indexing_options(
            TextFieldIndexing::default()
                .set_fieldnorms(true)
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::Basic),
        )
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
/// A set of field options for bytes fields.
pub struct BytesOptions {
    #[serde(default)]
    /// Should the bytes be indexed to be searched?
    pub indexed: bool,

    #[serde(default)]
    /// Is the field a fast field?.
    ///
    /// Fast fields have a similar lookup time to an array.
    pub fast: bool,

    #[serde(flatten)]
    pub base: BaseOptions,
}

impl BytesOptions {
    pub fn as_tantivy_opts(&self) -> tantivy::schema::BytesOptions {
        let mut opts = tantivy::schema::BytesOptions::default();

        if self.indexed {
            opts = opts.set_indexed();
        }

        if self.fast {
            opts = opts.set_fast();
        }

        opts
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
/// A set of field options for int fields that takes into account if a field is
/// multi-value or not in order to determine the fast-field cardinality.
pub struct CalculatedIntOptions {
    #[serde(default)]
    /// Should the integer be indexed to be searched?
    pub indexed: bool,

    /// Should fieldnorms be used?
    ///
    /// This is only relevant if `indexed = true`.
    /// By default this is `indexed` if left empty.
    pub fieldnorms: Option<bool>,

    #[serde(default)]
    /// Is the field a fast field?.
    ///
    /// Fast fields have a similar lookup time to an array.
    pub fast: bool,

    #[serde(flatten)]
    pub base: BaseOptions,
}

impl CalculatedIntOptions {
    pub fn as_tantivy_opts(&self) -> IntOptions {
        let mut opts = IntOptions::default();

        if self.indexed {
            opts = opts.set_indexed();
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
