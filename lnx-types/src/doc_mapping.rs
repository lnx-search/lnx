use std::collections::BTreeMap;

/// The document schema defining the doc mapping of documents to be ingested
pub struct DocMapping(BTreeMap<String, DocField>);

/// Represents a single document field and it's desired type/schema.
pub struct DocField {

}