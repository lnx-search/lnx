use ahash::AHasher;
use std::hash::{Hasher, Hash};

use tantivy::schema::{NamedFieldDocument, Value};

use crate::correction::correct_sentence;


pub(crate) fn hash<T: Hash>(v: &T) -> u64 {
    let mut hasher = AHasher::default();
    v.hash(&mut hasher);
    hasher.finish()
}

pub fn correct_doc_fields(
    doc: &mut NamedFieldDocument,
    indexed_text_fields: &Vec<String>,
) {
    let mut changes = vec![];

    for target in indexed_text_fields {
        let id = hash(target);

        let maybe_values = doc.0.get(target);
        if let Some(values) = maybe_values {
            for val in values {
                if let Value::Str(data) = val {
                    let corrected = correct_sentence(data);
                    changes.push((format!("_{}", id), vec![Value::Str(corrected)]));
                }
            }
        };
    }

    for (k, v) in changes {
        doc.0.insert(k, v);
    }
}