use std::borrow::Cow;
use smallvec::SmallVec;
use tantivy::schema::Field;

use lnx_schema::indexing::IndexingSchema;
use lnx_document::view_access::{DocViewTransform, MapAccess, ValueDeserializer, ValueTransform};

type Fields<'block> = SmallVec<[FieldValue<'block>; 8]>;

/// A doc view transformer for producing documents which
/// can be easily indexed.
pub struct DocIndexingTransformer;

impl<'block> DocViewTransform<'block> for DocIndexingTransformer {
    type Output = IndexingDoc<'block>;

    fn transform<A: MapAccess<'block>>(&mut self, mut access: A) -> anyhow::Result<Self::Output> {
        let mut fields = SmallVec::with_capacity(access.size_hint());

        let mut current_key = KeyBuilder::default();
        let mut transformer = ValueTransformer { current_key: &mut current_key };

        // We dont actually use the output type here as we use the transformer
        // more to walk through the nested structure rather than create new types.
        while let Some((key, _)) = access.next_entry(&mut transformer)? {

            // We know it's a top level doc.
            let key = if current_key.num_parts() == 0 {
                Cow::Borrowed(key)
            } else {
                current_key.push_part(key);
                Cow::Owned(current_key.to_key())
            };

            let value = FieldValue {
                key,
                foo: &(),
            };

            fields.push()
        }

        Ok(IndexingDoc { fields })
    }
}


/// A document which can be indexed by tantivy.
pub struct IndexingDoc<'block> {
    fields: SmallVec<[FieldValue<'block>; 8]>
}

pub struct FieldValue<'block> {
    key: Cow<'block, str>,
    foo: &'block (),
}


pub struct ValueTransformer<'block, 'a: 'block> {
    current_key: &'a mut KeyBuilder<'block>,
    fields: &'a mut Fields<'block>
}

// We dont actually use the output type here as we use the transformer
// more to walk through the nested structure rather than create new types.
impl<'block, 'a: 'block> ValueTransform<'block> for ValueTransformer<'block, 'a> {
    type Output = ();

    fn transform<D>(
        &mut self,
        deserializer: D,
    ) -> anyhow::Result<Self::Output>
    where
        D: ValueDeserializer<'block>
    {

        Ok(())
    }
}



#[derive(Default)]
/// A structure for incrementally building nested keys
/// as their flattened counter parts.
pub struct KeyBuilder<'block> {
    length: usize,
    parts: SmallVec<[&'block str; 4]>
}

impl<'block> KeyBuilder<'block> {
    /// Resets the entire temporary key.
    fn reset(&mut self) {
        self.length = 0;
        self.parts.clear();
    }

    /// Pops the last-added key part.
    fn pop_parts(&mut self) {
        if let Some(part) = self.parts.pop() {
            self.length -= part.len();
        }
    }

    /// Appends a new part to the *front* of the key.
    fn push_part(&mut self, s: &'block str) {
        self.length += s.len();
        self.parts.push(s);
    }

    /// Returns the length of the string if it was fully built.
    fn string_length(&self) -> usize {
        self.length + (self.parts.len() - 1)
    }

    /// Returns the number of parts in the temporary key.
    fn num_parts(&self) -> usize {
        self.parts.len()
    }

    /// Builds a new key from the internal set of parts.
    fn to_key(&self) -> String {
        let num_parts = self.parts.len();
        let mut s = String::with_capacity(self.length);

        for (i, part) in self.parts.iter().rev().enumerate() {
            s.push_str(part);

            if i < (num_parts - 1) {
                s.push('.');
            }
        }

        s
    }
}