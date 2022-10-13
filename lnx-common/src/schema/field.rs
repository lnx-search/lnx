#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    utoipa::ToSchema,
)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Field {
    /// A traditional text field.
    ///
    /// `Text` fields are tokenized and indexed;
    ///
    /// i.e. "Hello, world" becomes `["hello", "world"]` internally.
    Text(TextOptions),

    /// A text field which performs no tokenization.
    ///
    /// `RawStr` fields perform no tokenization or pre-processing, they
    /// are intern, unable to be searched for using the `fast-fuzzy`, `fuzzy` or `normal`
    /// query modes however, they can be used for `term` queries.
    RawStr(BaseOptions),

    /// A hierarchal facet field.
    ///
    /// They are typically represented similarly to a filepath.
    /// For instance, an e-commerce website could have a Facet for `/electronics/tv_and_video/led_tv`.
    ///
    /// A document can be associated to any number of facets.
    /// The hierarchy implicitly implies that a document belonging to a facet also belongs to the ancestor of its facet.
    /// In the example above, `/electronics/tv_and_video/` and `/electronics`.
    Facet(FacetFieldOptions),

    /// A unsigned 64 bit integer field.
    U64(NumericFieldOptions),

    /// A signed 64 bit integer field.
    I64(NumericFieldOptions),

    /// A 64 bit floating point number field.
    F64(NumericFieldOptions),

    /// A JSON object field.
    ///
    /// This field supports any structured JSON data providing the initial structure is a object.
    ///
    /// ✔️ Supported:
    /// ```json
    /// {
    ///     "name": "ChillFish8",
    ///     "bio": "My random data ha ha!",
    ///     "age": 86
    /// }
    /// ```
    ///
    /// ❌ Unsupported:
    /// ```json
    /// [
    ///     "Hello",
    ///     "world",
    ///     "terms"
    /// ]
    /// ```
    Json(BaseOptions),

    /// An arbitrary bytes field.
    ///
    /// Bytes fields can contain a blob of data which can *potentially* be used for
    /// searching, although this is not recommended.
    ///
    /// If returned or uploaded as a JSON object this field expects data to be submitted
    /// in the form of a `base64` encoded string and will be returned as such.
    Bytes(NumericFieldOptions),
}

#[derive(
    Debug,
    Clone,
    Default,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    utoipa::ToSchema,
)]
pub struct BaseOptions {
    #[schema(default = "true", example = json!(true))]
    #[serde(default = "default_to_true")]
    /// Should the field be included in the stored and compressed document.
    ///
    /// If this is `false`, the  field will not be returned as part of the
    /// original document, but can still be used for searching.
    pub stored: bool,

    #[schema(default = "false", example = json!(true))]
    #[serde(default)]
    /// Should the field be a multi-value field.
    ///
    /// This means the field can contain multiple values which are searchable.
    pub multi: bool,

    #[schema(default = "false", example = json!(true))]
    #[serde(default)]
    /// Should the field be mandatory when uploading documents.
    ///
    /// If the field is not mandatory they can be omitted from the uploaded
    /// document and when returned will be populated with a default value.
    ///
    /// The default value is `null` for single value fields and `[]` for multi-value fields.
    pub required: bool,
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    utoipa::ToSchema,
)]
pub struct TextOptions {
    #[serde(flatten)]
    #[schema(inline)]
    pub base: BaseOptions,
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    utoipa::ToSchema,
)]
#[serde(from = "NumericFieldOptionsSchema")]
pub struct NumericFieldOptions {
    #[schema(default = "false", example = json!(false))]
    /// Should the field be indexed.
    ///
    /// This can be used when performing searches or filtering.
    pub indexed: bool,

    #[schema(default = "<indexed>", example = json!(false))]
    /// Should the field norms be enabled.
    ///
    /// This defaults to what ever `indexed` is set to, which
    /// is probably good enough for most people.
    /// If you are unsure if you require this or not, leave as
    /// the default.
    pub field_norms: bool,

    #[schema(default = "false", example = json!(true))]
    /// Should this field be a fast field.
    ///
    /// Fast fields have a similar access time to an array and
    /// are used when sorting and filtering.
    pub fast: bool,

    #[schema(inline)]
    pub base: BaseOptions,
}

impl From<NumericFieldOptionsSchema> for NumericFieldOptions {
    fn from(v: NumericFieldOptionsSchema) -> Self {
        Self {
            indexed: v.indexed,
            field_norms: v.field_norms.unwrap_or(v.indexed),
            fast: v.fast,
            base: v.base,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct NumericFieldOptionsSchema {
    #[serde(default)]
    indexed: bool,

    #[serde(default)]
    field_norms: Option<bool>,

    #[serde(default)]
    fast: bool,

    #[serde(flatten, default)]
    base: BaseOptions,
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    utoipa::ToSchema,
)]
pub struct FacetFieldOptions {
    #[schema(default = "false", example = json!(false))]
    #[serde(default)]
    /// Should the field be included in the stored and compressed document.
    ///
    /// If this is `false`, the  field will not be returned as part of the
    /// original document, but can still be used for searching.
    pub stored: bool,
}

// This is a hack to default to true for serde.
fn default_to_true() -> bool {
    true
}
