use std::hash::{Hash, Hasher};

use ahash::AHasher;

use crate::correction::correct_sentence;
use crate::structures;
use crate::structures::DocumentValue;

pub(crate) fn hash<T: Hash>(v: &T) -> u64 {
    let mut hasher = AHasher::default();
    v.hash(&mut hasher);
    hasher.finish()
}

pub fn correct_doc_fields(doc: &mut structures::Document, indexed_text_fields: &Vec<String>) {
    let mut changes = vec![];

    for target in indexed_text_fields {
        let id = hash(target);

        let maybe_values = doc.0.get(target);
        if let Some(values) = maybe_values {
            for val in values {
                if let DocumentValue::Text(data) = val {
                    let corrected = correct_sentence(data, 1);
                    changes.push((format!("_{}", id), vec![DocumentValue::Text(corrected)]));
                }
            }
        };
    }

    for (k, v) in changes {
        doc.0.insert(k, v);
    }
}
