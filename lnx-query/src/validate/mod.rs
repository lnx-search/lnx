use std::borrow::Cow;

use poem_openapi::{Enum, Object};
use serde_derive::Serialize;

pub mod distinct;
pub mod query_parts;
pub mod select_fields;
pub mod sort;
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
    #[serde(rename = "ERR_FIELD_NOT_INDEXED")]
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
    FieldIsNotIndexed,
    #[serde(rename = "ERR_FIELD_NOT_COLUMNAR")]
    /// The field provided by user input exists, but is not backed by a columnar index.
    ///
    /// This means lnx does not have fast random access to the values within this field
    /// which prevents it from being used in things like sorting, distinct queries, aggregations, etc...
    ///
    /// **Help:**
    ///
    /// All fields are `columnar: true` by default, this means you have explicitly disabled
    /// the columnar index on the original content for this field. You will need to re-enable this option
    /// by creating a new table and re-inserting your data, lnx cannot do this for you automatically.
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
    /// The select statement provided is missing at least one field to return.
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
    #[serde(rename = "ERR_MISSING_QUERY_FIELDS")]
    /// The where clause query provided is missing at least one field to return.
    ///
    /// ```json5
    /// { $fuzzy: "hello world", $fields: [], ... }  // This doesn't work!
    /// ```
    ///
    /// **Help:**
    ///
    /// When you add a query, you must tell lnx what fields to search within, some fields
    /// may not be valid to use depending on what indexes the query requires.
    ///
    /// ```json5
    /// { $fuzzy: "hello world", $fields: ["title"], ... }  // Performs a fuzzy search on field `title`.
    /// ```
    MissingQueryFields,
    #[serde(rename = "ERR_MISSING_SORT_FIELDS")]
    /// The query provided has explicitly declared the `$sort` clause but
    /// has not provided any rules to sort the document by.
    ///
    /// ```json5
    /// { $sort: [], ... }  // This doesn't work!
    /// ```
    ///
    /// **Help:**
    ///
    /// You can pass either a single sort rule as an object, or an array of sort rules
    /// providing there is at least one rule within the array.
    ///
    /// The `$score` variable is available to sort by score explicitly, or you can set `$score: null`
    /// to disable sorting completely.
    /// Disabling sorting is not recommended if you need repeatable results as there
    /// is no consistent ordering in this event.
    ///
    /// ```json5
    /// // Sorts by score in ascending order
    /// { $sort: { $by: "$score", $order: "asc" }, ... }  
    /// // Sorts documents first by age in ascending order and then splits
    /// // any ties by sorting by $score in descending order.
    /// { $sort: [{ $by: "age", $order: "asc" }, { $by: "$score", $order: "desc" }], ... }  
    /// // Or to disable sorting and simply taking the first available results (non-predictable results.)
    /// { $sort: null, ... }
    /// ```
    MissingSortFields,
    #[serde(rename = "ERR_BAD_REGEX")]
    /// The provided regex pattern is not a valid regex.
    ///
    /// **Help:**
    ///
    /// lnx uses the regex syntax from the regex crate which can be found here:
    /// https://docs.rs/regex/latest/regex/#syntax
    InvalidRegexPattern,
    #[serde(rename = "ERR_BAD_BOOST")]
    /// The provided boost multiplier is incorrect.
    ///
    /// **Help:**
    ///
    /// The boost value is a _multiplier_ which is applied to the score which cannot be less than 0.0.
    InvalidBoostMultiplier,
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
