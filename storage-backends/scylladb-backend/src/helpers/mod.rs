pub mod doc;
pub mod segments;
pub mod as_cql_type;
// mod value;

pub fn format_column(v: impl AsRef<str>) -> String {
    format!("field_{}", v.as_ref())
}
