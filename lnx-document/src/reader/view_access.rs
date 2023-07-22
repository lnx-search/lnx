use std::net::Ipv6Addr;

use anyhow::bail;

use super::ArchivedFieldType;
use crate::{DateTime, DocumentView, Step};

/// A type which can transform a view into a new type from a deserializer.
pub trait DocViewTransform<'block> {
    /// The transformed type.
    type Output;

    /// Attempts to deserialize the type from a given map access.
    fn transform<A>(&mut self, access: A) -> anyhow::Result<Self::Output>
    where A: MapAccess<'block>;
}

/// A transform which can create a new type from a single document value.
pub trait ValueTransform<'block>
where
    Self: Sized + 'block,
{
    /// The transformed type.
    type Output;

    /// Attempts to deserialize the type from a given deserializer.
    fn transform<D>(
        &mut self,
        deserializer: D,
    ) -> anyhow::Result<Self::Output>
    where D: ValueDeserializer<'block>;
}

/// A deserializer which supports a given visitor.
pub trait ValueDeserializer<'block> {
    /// Attempts to deserialize the visitors output type.
    fn deserialize_any<V: ValueVisitor<'block>>(
        self,
        visitor: V,
    ) -> anyhow::Result<V::Output>
    where V: ValueVisitor<'block>;
}

/// A visitor type which handles multiple possible types.
pub trait ValueVisitor<'block> {
    /// The output value.
    type Output;

    #[inline]
    /// Called when the value is null.
    fn visit_null(&mut self) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret null value")
    }

    #[inline]
    /// Called when the value is a u64.
    fn visit_u64(&mut self, _val: u64) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret u64 value")
    }

    #[inline]
    /// Called when the value is a i64.
    fn visit_i64(&mut self, _val: i64) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret i64 value")
    }

    #[inline]
    /// Called when the value is a f64.
    fn visit_f64(&mut self, _val: f64) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret f64 value")
    }

    #[inline]
    /// Called when the value is a bool.
    fn visit_bool(&mut self, _val: bool) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret bool value")
    }

    #[inline]
    /// Called when the value is a ip.
    fn visit_ip(&mut self, _val: Ipv6Addr) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret ip address value")
    }

    #[inline]
    /// Called when the value is a datetime.
    fn visit_datetime(&mut self, _val: DateTime) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret datetime value")
    }

    #[inline]
    /// Called when the value is a str.
    fn visit_str(&mut self, _val: &'block str) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret str value")
    }

    #[inline]
    /// Called when the value is a bytes.
    fn visit_bytes(&mut self, _val: &'block [u8]) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret bytes value")
    }

    #[inline]
    /// Called when the value is a facet.
    fn visit_facet(&mut self, _val: &'block str) -> anyhow::Result<Self::Output> {
        bail!("Visitor cannot interpret facet value")
    }

    #[inline]
    /// Called when the value is a sequence.
    fn visit_seq<A>(&mut self, _access: A) -> anyhow::Result<Self::Output>
    where
        A: SeqAccess<'block>,
    {
        bail!("Visitor cannot interpret array value")
    }

    #[inline]
    /// Called when the value is a map.
    fn visit_map<A>(&mut self, _access: A) -> anyhow::Result<Self::Output>
    where
        A: MapAccess<'block>,
    {
        bail!("Visitor cannot interpret map value")
    }
}

/// A type which supports accessing a sequence of values.
pub trait SeqAccess<'block> {
    /// The approximate size of the sequence.
    ///
    /// This is a hint only, the sequence can always be longer or shorter
    /// than the value returned.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next entry in the sequence.
    fn next_element<T: ValueTransform<'block> + 'block>(
        &mut self,
        transformer: &mut T,
    ) -> anyhow::Result<Option<T::Output>>;
}

pub trait MapAccess<'block> {
    /// The approximate size of the map.
    ///
    /// This is a hint only, the map can always be longer or shorter
    /// than the value returned.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next entry in the map.
    fn next_entry<T: ValueTransform<'block> + 'block>(
        &mut self,
        transformer: &mut T,
    ) -> anyhow::Result<Option<(&'block str, T::Output)>>;
}

/// The core document view deserializer.
///
/// This allows you to effectively transpose the document view to other structures
/// without allocating the original data.
pub struct DocViewDeserializer<'block> {
    /// We use a raw pointer here because it is much easier to read and understand
    /// what is going on than the 3 lifetimes which existed in some parts of this code.
    state: *mut State,
    view: DocumentView<'block>,
}

impl<'block> DocViewDeserializer<'block> {
    /// Creates a new doc view deserializer.
    pub fn new(view: DocumentView<'block>) -> Self {
        let mut state = State {
            num_steps: view.doc.layout.len(),
            step_cursor: 0,
            type_cursors: Default::default(),
        };

        Self {
            state: (&mut state) as *mut _,
            view,
        }
    }
}

impl<'block> MapAccess<'block> for DocViewDeserializer<'block> {
    #[inline]
    fn size_hint(&self) -> usize {
        self.view.len()
    }

    fn next_entry<T: ValueTransform<'block> + 'block>(
        &mut self,
        transformer: &mut T,
    ) -> anyhow::Result<Option<(&'block str, T::Output)>> {
        unsafe { (*self.state).inc_step() };
        let step_idx = unsafe { (*self.state).step() };

        // Our data is finished.
        if step_idx >= unsafe { (*self.state).num_steps() } {
            return Ok(None);
        }

        let step: &rkyv::Archived<Step> = &self.view.doc.layout[step_idx];

        assert_ne!(
            step.field_id,
            u16::MAX,
            "Invalid doc layout, top level object cannot have array elements as part of the data (Indicated by the u16::MAX ID)"
        );

        let key = self.view.block.get_field(step.field_id);

        let deserializer = DocValueDeserializer {
            state: self.state,
            step,
            view: self.view,
        };
        let value = transformer.transform(deserializer)?;

        Ok(Some((key, value)))
    }
}

pub struct DocValueDeserializer<'block> {
    /// We use a raw pointer here because it is much easier to read and understand
    /// what is going on than the 3 lifetimes which existed in some parts of this code.
    state: *mut State,
    step: &'block rkyv::Archived<Step>,
    view: DocumentView<'block>,
}

impl<'block> ValueDeserializer<'block> for DocValueDeserializer<'block> {
    fn deserialize_any<V: ValueVisitor<'block>>(
        self,
        mut visitor: V,
    ) -> anyhow::Result<V::Output> {
        if !matches!(
            self.step.field_type,
            ArchivedFieldType::Array | ArchivedFieldType::Object
        ) {
            assert_eq!(
                self.step.field_length,
                1,
                "Field length for object values which are not collections should be single values."
            );
        }

        match self.step.field_type {
            ArchivedFieldType::String => {
                let cursor = unsafe { (*self.state).inc_string() };
                let v: &str = self.view.block.strings[cursor].as_ref();
                visitor.visit_str(v)
            },
            ArchivedFieldType::Bytes => {
                let cursor = unsafe { (*self.state).inc_bytes() };
                let v: &[u8] = self.view.block.bytes[cursor].as_ref();
                visitor.visit_bytes(v)
            },
            ArchivedFieldType::Bool => {
                let cursor = unsafe { (*self.state).inc_bool() };
                let v: bool = self.view.block.bools[cursor];
                visitor.visit_bool(v)
            },
            ArchivedFieldType::U64 => {
                let cursor = unsafe { (*self.state).inc_u64() };
                let v: u64 = self.view.block.u64s[cursor];
                visitor.visit_u64(v)
            },
            ArchivedFieldType::I64 => {
                let cursor = unsafe { (*self.state).inc_i64() };
                let v: i64 = self.view.block.i64s[cursor];
                visitor.visit_i64(v)
            },
            ArchivedFieldType::F64 => {
                let cursor = unsafe { (*self.state).inc_f64() };
                let v: f64 = self.view.block.f64s[cursor];
                visitor.visit_f64(v)
            },
            ArchivedFieldType::Null => visitor.visit_null(),
            ArchivedFieldType::IpAddr => {
                let cursor = unsafe { (*self.state).inc_ip() };
                let v = self.view.block.ips[cursor];
                visitor.visit_ip(v.as_ipv6())
            },
            ArchivedFieldType::DateTime => {
                let cursor = unsafe { (*self.state).inc_i64() };
                let v: i64 = self.view.block.i64s[cursor];
                let dt = DateTime::from_micros(v).expect("This is infallible");
                visitor.visit_datetime(dt)
            },
            ArchivedFieldType::Facet => {
                let cursor = unsafe { (*self.state).inc_string() };
                let v: &str = self.view.block.strings[cursor].as_ref();
                visitor.visit_facet(v)
            },
            ArchivedFieldType::Array => {
                let access = DocArrayDeserializer {
                    sequence_state: SequenceState::new(
                        self.state,
                        self.step,
                        self.view,
                        self.step.field_length,
                    ),
                    view: self.view,
                };
                visitor.visit_seq(access)
            },
            ArchivedFieldType::Object => {
                let access = DocMapDeserializer {
                    state: self.state,
                    size: self.step.field_length as usize,
                    cursor: 0,
                    view: self.view,
                };
                visitor.visit_map(access)
            },
        }
    }
}

pub struct DocArrayDeserializer<'block> {
    /// The state specific to the sequence handling.
    sequence_state: SequenceState<'block>,
    view: DocumentView<'block>,
}

impl<'block> SeqAccess<'block> for DocArrayDeserializer<'block> {
    #[inline]
    fn size_hint(&self) -> usize {
        self.sequence_state.size
    }

    fn next_element<T: ValueTransform<'block>>(
        &mut self,
        transformer: &mut T,
    ) -> anyhow::Result<Option<T::Output>> {
        if self.sequence_state.is_complete() {
            return Ok(None);
        }
        self.sequence_state.maybe_advance_cursors();

        let deserializer = DocValueDeserializer {
            state: self.sequence_state.state,
            step: self.sequence_state.step,
            view: self.view,
        };

        transformer.transform(deserializer).map(Some)
    }
}

pub struct DocMapDeserializer<'block> {
    /// We use a raw pointer here because it is much easier to read and understand
    /// what is going on than the 3 lifetimes which existed in some parts of this code.
    state: *mut State,
    size: usize,
    cursor: usize,
    view: DocumentView<'block>,
}

impl<'block> MapAccess<'block> for DocMapDeserializer<'block> {
    #[inline]
    fn size_hint(&self) -> usize {
        self.size
    }

    fn next_entry<T: ValueTransform<'block>>(
        &mut self,
        transformer: &mut T,
    ) -> anyhow::Result<Option<(&'block str, T::Output)>> {
        if self.cursor >= self.size {
            return Ok(None);
        }

        unsafe { (*self.state).inc_step() };
        let step_idx = unsafe { (*self.state).step() };
        let step: &rkyv::Archived<Step> = &self.view.doc.layout[step_idx];

        debug_assert_eq!(
            step.field_length, 1,
            "Field value lengths for objects should always be 1.",
        );

        let deserializer = DocValueDeserializer {
            state: self.state,
            step,
            view: self.view,
        };

        let key = self.view.block.get_field(step.field_id);
        let value = transformer.transform(deserializer)?;

        Ok(Some((key, value)))
    }
}

#[derive(Default)]
struct State {
    num_steps: usize,
    step_cursor: usize,
    type_cursors: TypeCursors,
}

impl State {
    /// Returns the total number of layout steps in the doc.
    fn num_steps(&self) -> usize {
        self.num_steps
    }

    /// Returns the position of the step cursor.
    fn step(&self) -> usize {
        self.step_cursor
    }

    /// Increments the current step cursor.
    fn inc_step(&mut self) {
        self.step_cursor += 1;
    }

    /// Increments the string value cursor and returns the *previous* value before
    /// it was incremented.
    fn inc_string(&mut self) -> usize {
        let before = self.type_cursors.strings;
        self.type_cursors.strings += 1;
        before
    }

    /// Increments the u64 value cursor and returns the *previous* value before
    /// it was incremented.
    fn inc_u64(&mut self) -> usize {
        let before = self.type_cursors.u64s;
        self.type_cursors.u64s += 1;
        before
    }

    /// Increments the i64 value cursor and returns the *previous* value before
    /// it was incremented.
    fn inc_i64(&mut self) -> usize {
        let before = self.type_cursors.i64s;
        self.type_cursors.i64s += 1;
        before
    }

    /// Increments the f64 value cursor and returns the *previous* value before
    /// it was incremented.
    fn inc_f64(&mut self) -> usize {
        let before = self.type_cursors.f64s;
        self.type_cursors.f64s += 1;
        before
    }

    /// Increments the ip value cursor and returns the *previous* value before
    /// it was incremented.
    fn inc_ip(&mut self) -> usize {
        let before = self.type_cursors.ips;
        self.type_cursors.ips += 1;
        before
    }

    /// Increments the string value cursor and returns the *previous* value before
    /// it was incremented.
    fn inc_bool(&mut self) -> usize {
        let before = self.type_cursors.bools;
        self.type_cursors.bools += 1;
        before
    }

    /// Increments the string value cursor and returns the *previous* value before
    /// it was incremented.
    fn inc_bytes(&mut self) -> usize {
        let before = self.type_cursors.bytes;
        self.type_cursors.bytes += 1;
        before
    }
}

struct SequenceState<'block> {
    /// We use a raw pointer here because it is much easier to read and understand
    /// what is going on than the 3 lifetimes which existed in some parts of this code.
    state: *mut State,
    /// The number of unique step entries.
    size: usize,
    /// The current position of the step entries.
    cursor: usize,
    /// The number of values that are part of a given step.
    temporary_value_size: usize,
    /// The current position of the cursor going through the values for the given step.
    temporary_value_cursor: usize,
    /// The current step context.
    step: &'block rkyv::Archived<Step>,
    view: DocumentView<'block>,
}

impl<'block> SequenceState<'block> {
    fn new(
        state: *mut State,
        step: &'block rkyv::Archived<Step>,
        view: DocumentView<'block>,
        field_length: u16,
    ) -> Self {
        Self {
            state,
            size: field_length as usize,
            cursor: 0,
            temporary_value_size: 0,
            temporary_value_cursor: 0,
            step,
            view,
        }
    }

    /// Returns if the sequence is complete.
    fn is_complete(&self) -> bool {
        self.cursor >= self.size
    }

    /// Potentially advances the cursors within the state.
    ///
    /// If the sequence is already complete this is a no-op.
    fn maybe_advance_cursors(&mut self) {
        if self.is_complete() {
            return;
        }

        // Our step values are exhausted, let's advance our cursor.
        if self.temporary_value_cursor >= self.temporary_value_size {
            unsafe { (*self.state).inc_step() };
            let step_idx = unsafe { (*self.state).step() };
            self.step = &self.view.doc.layout[step_idx];

            // Adjust the cursor positions
            self.temporary_value_cursor = 0;
            self.temporary_value_size = self.step.field_length as usize;
            self.cursor += 1;
        }
    }
}

#[derive(Copy, Clone, Default)]
struct TypeCursors {
    strings: usize,
    u64s: usize,
    i64s: usize,
    f64s: usize,
    ips: usize,
    bools: usize,
    bytes: usize,
}
