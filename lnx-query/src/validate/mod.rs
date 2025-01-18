use std::borrow::Cow;

use poem_openapi::{Enum, Object};
use serde_derive::Serialize;

pub mod distinct;
pub mod select_fields;
pub mod table;

#[derive(Debug, Copy, Clone, Enum, Serialize, Eq, PartialEq)]
/// The unique code of the validation error that occurred.
pub enum ErrorCode {
    #[serde(rename = "ERR_UNKNOWN_TABLE")]
    /// The table provided by user input does not exist within lnx's table catalog.
    ///
    /// **Help:**
    ///
    /// Although rare, it is possible that there is a small delay of 1-2 seconds between creating
    /// the table and when it is accessible via the API, in this situation retrying the error
    /// should resolve this error.
    UnknownTable,
    #[serde(rename = "ERR_UNKNOWN_FIELD")]
    /// The field provided by user input does not exist for the specified table.
    ///
    /// **Help:**
    ///
    /// Double check that the table you're attempted to select data from contains
    /// the field(s) you are specifying.
    UnknownField,
    #[serde(rename = "ERR_FIELD_NOT_STORED")]
    /// The field provided by user input exists, but is not stored.
    ///
    /// This means that the data is only indexed which is a _lossy_ conversion and therefore
    /// lnx cannot retrieve the original value.
    ///
    /// **Help:**
    ///
    /// All fields are `stored: true` by default, this means you have explicitly disabled
    /// storing the original content for this field. You will need to re-enable this option
    /// by creating a new table and re-inserting your data, lnx cannot do this for you automatically.
    FieldIsNotStored,
    /// The field provided by user input exists, but is not backed by a columnar index.
    ///
    /// This means lnx does not have fast random access to the values within this field
    /// which prevents it from being used in things like sorting, distinct queries, aggregations, etc...
    ///
    /// **Help:**
    ///
    /// All fields are `columnar: true` by default, this means you have explicitly disabled
    /// the columnar index on the original content for this field. You will need to re-enable this option
    /// by editing the table settings, this will cause lnx to re-index the table from scratch
    /// which has a significant performance impact.
    FieldIsNotFast,
    #[serde(rename = "ERR_BAD_WILDCARD")]
    /// A wildcard was provided by the user input alongside other explicitly declared fields
    /// which is not allowed.
    ///
    /// **Help:**
    ///
    /// Use _either_ the wildcard (`*`) _or_ explicitly declare the fields as they are mutually
    /// exclusive because the wildcard will implicitly pull in the fields being explicitly declared.
    WildcardNotAllowed,
    #[serde(rename = "ERR_DUPLICATE_FIELD")]
    /// The same field has been provided twice.
    ///
    /// **Help:**
    ///
    /// Remove on of the duplicate values so there is all field names are unique.
    DuplicateField,
    #[serde(rename = "ERR_MISSING_SELECT_FIELDS")]
    /// The query provided is missing at least one field to return.
    ///
    /// ```json5
    /// { $select: [], ... }  // This doesn't work!
    /// ```
    ///
    /// **Help:**
    ///
    /// You can pass either individual fields, or pass a `*` as the single value within the array.
    /// For example, either of these patterns work:
    ///
    /// ```json5
    /// { $select: ["*"], ... }  // Returns all fields
    /// { $select: ["a", "b"], ... }  // Returns only fields "a" and "b".
    /// ```
    MissingSelectFields,
}

#[derive(Debug, Clone, Object, Serialize)]
/// A validation error that occurs when the syntax of a query is valid,
/// but some part of the provided parameters are invalid.
pub struct ValidationError {
    /// The general error code for the validation error.
    pub code: ErrorCode,
    /// The error description.
    pub message: String,
    /// A possible quick fix for the error you are experiencing.
    pub help: Option<String>,
    /// Where the error is located within the query.
    ///
    /// For example: `$where.$any[1].$fuzzy.$fields[0]` would mean the
    /// value located within the `where` clause, in the _second_ nested query parameter
    /// within the `$any` query, within the `$fuzzy` query's _first_ search field.
    pub location: String,
}

#[derive(Default)]
/// The current context as the system validates the query.
pub(crate) struct ValidatorContext {
    /// The current position where the validator got to.
    location: Vec<JsonNode>,
}

impl ValidatorContext {
    /// Pops the current location node from the stack.
    pub fn pop_location(&mut self) {
        self.location.pop();
    }

    /// Adds a new location node.
    pub fn push_location(&mut self, loc: impl Into<JsonNode>) {
        self.location.push(loc.into())
    }

    /// Build a new validation error.
    pub fn build_error(
        &self,
        code: ErrorCode,
        message: String,
        help: Option<String>,
    ) -> ValidationError {
        ValidationError {
            code,
            message,
            help,
            location: self.render_location(),
        }
    }

    fn render_location(&self) -> String {
        use std::fmt::Write;

        let mut nodes = self.location.iter().peekable();
        let mut location = String::new();

        while let Some(node) = nodes.next() {
            match node {
                JsonNode::Key(key) => write!(location, "{key}").unwrap(),
                JsonNode::Index(index) => write!(location, "[{index}]").unwrap(),
            };

            if let Some(next) = nodes.peek() {
                if matches!(next, JsonNode::Key(_)) {
                    location.push('.');
                }
            }
        }

        location
    }
}

pub(crate) enum JsonNode {
    Key(Cow<'static, str>),
    Index(usize),
}

impl From<&'static str> for JsonNode {
    fn from(value: &'static str) -> Self {
        Self::Key(Cow::Borrowed(value))
    }
}

impl From<String> for JsonNode {
    fn from(value: String) -> Self {
        Self::Key(Cow::Owned(value))
    }
}

impl From<usize> for JsonNode {
    fn from(value: usize) -> Self {
        Self::Index(value)
    }
}
