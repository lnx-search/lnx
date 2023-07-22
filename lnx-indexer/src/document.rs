use std::borrow::Cow;

use smallvec::SmallVec;

type Fields<'block> = SmallVec<[FieldValue<'block>; 8]>;

pub struct FieldValue<'block> {
    key: Cow<'block, str>,
    foo: &'block (),
}

#[derive(Default)]
/// A structure for incrementally building nested keys
/// as their flattened counter parts.
pub struct KeyBuilder<'block> {
    length: usize,
    parts: SmallVec<[&'block str; 4]>,
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
