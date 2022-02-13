use serde::{Deserialize, Serialize};

use super::options::{BaseOptions, BytesOptions, CalculatedIntOptions};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
/// A declared schema field type.
///
/// Each field has a set of relevant options which can
/// be used to tweak and optimise the index.
pub enum FieldInfo {
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
        opts: BaseOptions,
    },

    /// A string field with given options.
    ///
    /// This wont be tokenized.
    String {
        #[serde(flatten)]
        opts: BaseOptions,
    },

    /// A facet field.
    ///
    /// This is typically represented as a path e.g. `videos/moves/ironman`
    Facet {
        #[serde(flatten)]
        opts: BaseOptions,
    },

    /// A bytes field.
    ///
    /// This is stored as a blob and can potentially be indexed if needed.
    /// This can be submitted as a base64 encoded string for formats that don't
    /// support bytes directly e.g. JSON.
    Bytes {
        #[serde(flatten)]
        opts: BytesOptions,
    },
}

impl FieldInfo {
    #[inline]
    pub fn is_required(&self) -> bool {
        match self {
            FieldInfo::F64 { opts } => opts.base.required,
            FieldInfo::U64 { opts } => opts.base.required,
            FieldInfo::I64 { opts } => opts.base.required,
            FieldInfo::Date { opts } => opts.base.required,
            FieldInfo::Text { opts } => opts.required,
            FieldInfo::String { opts } => opts.required,
            FieldInfo::Facet { opts } => opts.required,
            FieldInfo::Bytes { opts } => opts.base.required,
        }
    }

    #[inline]
    pub fn is_multi(&self) -> bool {
        match self {
            FieldInfo::F64 { opts } => opts.base.multi,
            FieldInfo::U64 { opts } => opts.base.multi,
            FieldInfo::I64 { opts } => opts.base.multi,
            FieldInfo::Date { opts } => opts.base.multi,
            FieldInfo::Text { opts } => opts.multi,
            FieldInfo::String { opts } => opts.multi,
            FieldInfo::Facet { opts } => opts.multi,
            FieldInfo::Bytes { opts } => opts.base.multi,
        }
    }

    #[inline]
    pub fn is_indexed(&self) -> bool {
        match self {
            FieldInfo::F64 { opts } => opts.indexed,
            FieldInfo::U64 { opts } => opts.indexed,
            FieldInfo::I64 { opts } => opts.indexed,
            FieldInfo::Date { opts } => opts.indexed,
            FieldInfo::Text { .. } => true,
            FieldInfo::String { .. } => true,
            FieldInfo::Facet { .. } => true,
            FieldInfo::Bytes { opts } => opts.indexed,
        }
    }
}
