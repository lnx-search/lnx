use std::borrow::Cow;
use lnx_common::schema::FieldInfo;

pub trait AsCqlType {
    fn as_cql_type(&self) -> Cow<'static, str>;
}

impl AsCqlType for FieldInfo {
    fn as_cql_type(&self) -> Cow<'static, str> {
        if self.store_as_blob() {
            return Cow::Borrowed("blob")
        }

        let column_type = match self {
            FieldInfo::F64 { .. } => "double",
            FieldInfo::U64 { .. } => "bigint",
            FieldInfo::I64 { .. } => "bigint",
            FieldInfo::Date { .. } => "timestamp",
            FieldInfo::Text { .. } => "text",
            FieldInfo::String { .. } => "text",
            FieldInfo::Facet { .. } => "text",
            FieldInfo::Bytes { .. } => "blob",
        };

        if self.is_multi() {
            Cow::Owned(format!("set<{}>", column_type))
        } else {
            Cow::Borrowed(column_type)
        }
    }
}
