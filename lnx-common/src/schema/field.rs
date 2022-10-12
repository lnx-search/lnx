

#[derive(
    Debug,
    Clone,
    validator::Validate,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    utoipa::ToSchema,
)]
pub struct Field {

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
#[serde(tag = "type")]
pub enum FieldType {
    Text {

    },
    RawStr {

    },
    U64 {

    },
    I64 {

    },
    F64 {

    },
    Json {

    },
    Bytes {

    },
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
#[serde(from = "BaseOptions")]
pub struct BaseOptions {
    pub indexed: bool,
    pub field_norms: bool,
    pub fast: bool,
}

impl From<BaseOptionsSchema> for BaseOptions {
    fn from(v: BaseOptionsSchema) -> Self {
        Self {
            indexed: v.indexed,
            field_norms: v.field_norms.unwrap_or(v.indexed),
            fast: v.fast,
        }
    }
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
struct BaseOptionsSchema {
    #[serde(default)]
    stored: bool,

    #[serde(default)]
    multi: bool,

    #[serde(default)]
    required: bool,
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
    pub indexed: bool,
    pub field_norms: bool,
    pub fast: bool,
}

impl From<NumericFieldOptionsSchema> for NumericFieldOptions {
    fn from(v: NumericFieldOptionsSchema) -> Self {
        Self {
            indexed: v.indexed,
            field_norms: v.field_norms.unwrap_or(v.indexed),
            fast: v.fast,
        }
    }
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
struct NumericFieldOptionsSchema {
    #[serde(default)]
    indexed: bool,

    #[serde(default)]
    field_norms: Option<bool>,

    #[serde(default)]
    fast: bool,
}
