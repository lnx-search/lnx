use serde::Deserialize;
use utoipa::ToSchema;
use crate::query_structure::Occur;

#[derive(Debug, Default, Clone, ToSchema, Deserialize)]
pub struct FuzzyQueryContext {
    #[schema(example = "Ironman 4")]
    #[serde(rename = "$value")]
    /// The actual query value.
    ///
    /// This can be a single word or a whole sentence.
    pub value: String,

    #[serde(default, rename = "$transposition_costs_two")]
    /// Should swapping two characters around within a word in order
    /// to match it to another word have a edit distance of `2` instead of `1` (default.)
    ///
    /// If you are unsure what this is or if you need it, you probably do not want to change
    /// this from the default (`false`).
    pub transposition_costs_two: bool,

    #[serde(default, rename = "$word_occurrence")]
    /// The occurrence rule for the produced query.
    ///
    /// When you provide a query value, it is tokenized into individual words using
    /// the schema field's set tokenizer (by default this splits on whitespace and punctuation.)
    ///
    /// When these tokens are combined as part of the query, the `word_occurrence` is used to
    /// describe whether all of the words, some of the words or none of the words should appear
    /// in the document. By default this is set to `should` so some words may be missing and
    /// still match a given document.
    pub word_occurrence: Occur,
}

impl From<String> for FuzzyQueryContext {
    fn from(value: String) -> Self {
        Self {
            value,
            ..Default::default()
        }
    }
}