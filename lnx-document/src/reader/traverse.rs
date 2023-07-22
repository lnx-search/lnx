use std::net::Ipv6Addr;

use crate::{ArchivedFieldType, DateTime, DocumentView, Step};

/// A type which can be walked through a document view.
pub trait ViewWalker<'block> {
    /// The error produced by the view walker.
    type Err;

    /// Called when the walker visits a null value.
    fn visit_null(&mut self, is_last: bool) -> Result<(), Self::Err>;

    /// Called when the walker visits a str value.
    fn visit_str(&mut self, is_last: bool, val: &'block str) -> Result<(), Self::Err>;

    /// Called when the walker visits a bytes value.
    fn visit_bytes(&mut self, is_last: bool, val: &'block [u8])
        -> Result<(), Self::Err>;

    /// Called when the walker visits a nool value.
    fn visit_bool(&mut self, is_last: bool, val: bool) -> Result<(), Self::Err>;

    /// Called when the walker visits a u64 value.
    fn visit_u64(&mut self, is_last: bool, val: u64) -> Result<(), Self::Err>;

    /// Called when the walker visits an i64 value.
    fn visit_i64(&mut self, is_last: bool, val: i64) -> Result<(), Self::Err>;

    /// Called when the walker visits an f64 value.
    fn visit_f64(&mut self, is_last: bool, val: f64) -> Result<(), Self::Err>;

    /// Called when the walker visits an ip address value.
    fn visit_ip(&mut self, is_last: bool, val: Ipv6Addr) -> Result<(), Self::Err>;

    /// Called when the walker visits a datetime value.
    fn visit_date(&mut self, is_last: bool, val: DateTime) -> Result<(), Self::Err>;

    /// Called when the walker visits a facet value.
    fn visit_facet(&mut self, is_last: bool, val: &'block str) -> Result<(), Self::Err>;

    /// A object key.
    fn visit_map_key(&mut self, key: &'block str) -> Result<(), Self::Err>;

    /// Called when the walker starts a new array.
    fn start_array(&mut self, size_hint: usize) -> Result<(), Self::Err>;

    /// Called when the walker ends the current array structure.
    fn end_array(&mut self, is_last: bool) -> Result<(), Self::Err>;

    /// Called when the walker starts a new map.
    fn start_map(&mut self, size_hint: usize) -> Result<(), Self::Err>;

    /// Called when the walker ends the current map structure.
    fn end_map(&mut self, is_last: bool) -> Result<(), Self::Err>;
}

/// Largely copied from serde, this macro just
/// returns an error as is without the additional `into()`
/// logic of the `?` operator.
macro_rules! tri {
    ($res:expr) => {{
        match $res {
            Ok(val) => val,
            Err(e) => return Err(e),
        }
    }};
}

/// A system that can traverse the compact document format
/// allowing systems to work with the documents in a more
/// user friendly way without having to worry about
/// any cursors or references.
pub(crate) struct DocViewTraverser<'block, W>
where
    W: ViewWalker<'block>,
{
    /// The internal walker.
    pub(crate) walker: W,
    /// The document view.
    pub(crate) view: DocumentView<'block>,
}

impl<'block, W> DocViewTraverser<'block, W>
where
    W: ViewWalker<'block>,
{
    /// Serializes the view to a JSON formatted value in a given writer.
    pub(crate) fn traverse(mut self) -> Result<(), W::Err> {
        tri!(self.walker.start_map(self.view.len()));

        let mut cursors = TypeCursors::default();

        let mut i = 0;
        let mut step_idx = 0;
        while step_idx < self.view.doc.layout.len() {
            let step: &rkyv::Archived<Step> = &self.view.doc.layout[step_idx];

            assert_ne!(
                step.field_id,
                u16::MAX,
                "Invalid doc layout, top level object cannot have array elements as part of the data (Indicated by the u16::MAX ID)"
            );

            let is_last = i >= (self.view.doc.len as usize - 1);
            tri!(self.walk_map_field(is_last, &mut step_idx, &mut cursors, step));

            step_idx += 1;
            i += 1;
        }

        tri!(self.walker.end_map(true));

        Ok(())
    }

    fn walk_map_field(
        &mut self,
        is_parent_last: bool,
        step_idx: &mut usize,
        cursors: &mut TypeCursors,
        step: &rkyv::Archived<Step>,
    ) -> Result<(), W::Err> {
        debug_assert_ne!(step.field_id, u16::MAX, "Object field should not be an array sub element. This is a bug.\n{step_idx}\n {step:?}");

        let key: &str = self.view.block.field_mapping[step.field_id as usize].as_ref();
        tri!(self.walker.visit_map_key(key));

        if !matches!(
            step.field_type,
            ArchivedFieldType::Array | ArchivedFieldType::Object
        ) {
            assert_eq!(step.field_length, 1, "Field length for object values which are not collections should be single values.");
        }

        match step.field_type {
            ArchivedFieldType::Null => tri!(self.walker.visit_null(is_parent_last)),
            ArchivedFieldType::String => {
                let v: &str = self.view.block.strings[cursors.strings].as_ref();

                tri!(self.walker.visit_str(is_parent_last, v));

                cursors.strings += 1;
            },
            ArchivedFieldType::Bytes => {
                let v: &[u8] = self.view.block.bytes[cursors.bytes].as_ref();

                tri!(self.walker.visit_bytes(is_parent_last, v));

                cursors.bytes += 1;
            },
            ArchivedFieldType::Bool => {
                let v: bool = self.view.block.bools[cursors.bools];

                tri!(self.walker.visit_bool(is_parent_last, v));

                cursors.bools += 1;
            },
            ArchivedFieldType::U64 => {
                let v: u64 = self.view.block.u64s[cursors.u64s];

                tri!(self.walker.visit_u64(is_parent_last, v));

                cursors.u64s += 1;
            },
            ArchivedFieldType::I64 => {
                let v: i64 = self.view.block.i64s[cursors.i64s];

                tri!(self.walker.visit_i64(is_parent_last, v));

                cursors.i64s += 1;
            },
            ArchivedFieldType::F64 => {
                let v: f64 = self.view.block.f64s[cursors.f64s];

                tri!(self.walker.visit_f64(is_parent_last, v));

                cursors.f64s += 1;
            },
            ArchivedFieldType::IpAddr => {
                let ip = self.view.block.ips[cursors.ips];

                tri!(self.walker.visit_ip(is_parent_last, ip.as_ipv6()));

                cursors.ips += 1;
            },
            ArchivedFieldType::DateTime => {
                let v = &self.view.block.i64s[cursors.i64s];
                let dt = DateTime::from_micros(*v).expect("This is infallible");

                tri!(self.walker.visit_date(is_parent_last, dt));

                cursors.i64s += 1;
            },
            ArchivedFieldType::Facet => {
                let v: &str = self.view.block.strings[cursors.strings].as_ref();

                tri!(self.walker.visit_facet(is_parent_last, v));

                cursors.strings += 1;
            },
            ArchivedFieldType::Array => {
                let collection_length = step.field_length as usize;

                tri!(self.walker.start_array(collection_length));

                for i in 0..collection_length {
                    (*step_idx) += 1;

                    let step = &self.view.doc.layout[*step_idx];

                    let is_last = i >= (collection_length - 1);
                    self.walk_array_element(is_last, step_idx, cursors, step)?;
                }

                tri!(self.walker.end_array(is_parent_last));
            },
            ArchivedFieldType::Object => {
                let collection_length = step.field_length as usize;

                tri!(self.walker.start_map(collection_length));

                for i in 0..collection_length {
                    (*step_idx) += 1;

                    let step = &self.view.doc.layout[*step_idx];

                    let is_last = i >= (collection_length - 1);
                    self.walk_map_field(is_last, step_idx, cursors, step)?;
                }

                tri!(self.walker.end_map(is_parent_last));
            },
        }

        Ok(())
    }

    #[inline]
    fn walk_array_element(
        &mut self,
        is_parent_last: bool,
        step_idx: &mut usize,
        cursors: &mut TypeCursors,
        step: &rkyv::Archived<Step>,
    ) -> Result<(), W::Err> {
        assert_eq!(
            step.field_id,
            u16::MAX,
            "Got non-array element step. This likely means the layout was read incorrectly. This is a bug."
        );

        match step.field_type {
            ArchivedFieldType::Null => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let is_last = i >= (num_entries - 1);
                    tri!(self.walker.visit_null(is_parent_last && is_last));
                }
            },
            ArchivedFieldType::String => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: &str = self.view.block.strings[cursors.strings].as_ref();

                    let is_last = i >= (num_entries - 1);
                    tri!(self.walker.visit_str(is_parent_last && is_last, v));

                    cursors.strings += 1;
                }
            },
            ArchivedFieldType::Bytes => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: &[u8] = self.view.block.bytes[cursors.bytes].as_ref();

                    let is_last = i >= (num_entries - 1);
                    tri!(self.walker.visit_bytes(is_parent_last && is_last, v));

                    cursors.bytes += 1;
                }
            },
            ArchivedFieldType::Bool => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: bool = self.view.block.bools[cursors.bools];

                    let is_last = i >= (num_entries - 1);
                    tri!(self.walker.visit_bool(is_parent_last && is_last, v));

                    cursors.bools += 1;
                }
            },
            ArchivedFieldType::U64 => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: u64 = self.view.block.u64s[cursors.u64s];

                    let is_last = i >= (num_entries - 1);
                    tri!(self.walker.visit_u64(is_parent_last && is_last, v));

                    cursors.u64s += 1;
                }
            },
            ArchivedFieldType::I64 => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: i64 = self.view.block.i64s[cursors.i64s];

                    let is_last = i >= (num_entries - 1);
                    tri!(self.walker.visit_i64(is_parent_last && is_last, v));

                    cursors.i64s += 1;
                }
            },
            ArchivedFieldType::F64 => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: f64 = self.view.block.f64s[cursors.f64s];

                    let is_last = i >= (num_entries - 1);
                    tri!(self.walker.visit_f64(is_parent_last && is_last, v));

                    cursors.f64s += 1;
                }
            },
            ArchivedFieldType::IpAddr => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let ip = self.view.block.ips[cursors.ips];

                    let is_last = i >= (num_entries - 1);
                    tri!(self
                        .walker
                        .visit_ip(is_parent_last && is_last, ip.as_ipv6()));

                    cursors.ips += 1;
                }
            },
            ArchivedFieldType::DateTime => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: i64 = self.view.block.i64s[cursors.i64s];
                    let dt = DateTime::from_micros(v).expect("This is infallible");

                    let is_last = i >= (num_entries - 1);
                    tri!(self.walker.visit_date(is_parent_last && is_last, dt));

                    cursors.i64s += 1;
                }
            },
            ArchivedFieldType::Facet => {
                let num_entries = step.field_length as usize;
                for i in 0..num_entries {
                    let v: &str = self.view.block.strings[cursors.strings].as_ref();

                    let is_last = i >= (num_entries - 1);
                    tri!(self.walker.visit_facet(is_parent_last && is_last, v));

                    cursors.strings += 1;
                }
            },
            ArchivedFieldType::Array => {
                let collection_length = step.field_length as usize;

                tri!(self.walker.start_array(collection_length));

                for i in 0..collection_length {
                    (*step_idx) += 1;

                    let step = &self.view.doc.layout[*step_idx];
                    let is_last = i >= (collection_length - 1);

                    self.walk_array_element(is_last, step_idx, cursors, step)?;
                }

                tri!(self.walker.end_array(is_parent_last));
            },
            ArchivedFieldType::Object => {
                let collection_length = step.field_length as usize;

                tri!(self.walker.start_map(collection_length));

                for i in 0..collection_length {
                    (*step_idx) += 1;

                    let step = &self.view.doc.layout[*step_idx];
                    let is_last = i >= (collection_length - 1);

                    self.walk_map_field(is_last, step_idx, cursors, step)?;
                }

                tri!(self.walker.end_map(is_parent_last));
            },
        }

        Ok(())
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
