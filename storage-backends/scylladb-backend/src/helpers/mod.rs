pub mod doc;
pub mod segments;

pub fn format_column(v: impl AsRef<str>) -> String {
    format!("field_{}", v.as_ref())
}