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

    #[serde(default, rename = "$edit_distance_bounds")]
    pub edit_distance_bounds: EditDistanceBounds,
}

impl From<String> for FuzzyQueryContext {
    fn from(value: String) -> Self {
        Self {
            value,
            ..Default::default()
        }
    }
}


#[derive(Debug, Clone, ToSchema, Deserialize)]
/// The word length bounds associated with the maximum edit distance.
///
/// Configuring the maximum edit distance for each word is key in order
/// to create good relevancy. Often short words are not spelt wrong, or
/// are not spelt as badly.
///
/// Because of this, having a edit distance of `2` for short words like `hello`
/// is probably not a good idea, as it makes the algorithm slightly *too* lenient
/// as with an edit distance of `2`, the term `hello` would also match `hollow` which
/// is very unlikely to actually be what the user wanted.
///
/// By default the system has the following values:
///
/// - Words which are shorter than `5` characters long have a max distance of `0` (no mistakes).
/// - Words which are shorter than `8` characters long have a max distance of `1` (1 typo).
/// - Words which are longer than or equal to `8` characters long have a max distance of `2` (2 typos).
pub struct EditDistanceBounds {
    #[schema(default = json!(5), example = json!(5))]
    #[serde(default = "EditDistanceBounds::default_min_length_d1")]
    /// The minimum length of a word to have a max edit distance of `1`.
    pub distance_of_1_bound: usize,

    #[schema(default = json!(8), example = json!(8))]
    #[serde(default = "EditDistanceBounds::default_min_length_d2")]
    /// The minimum length of a word to have a max edit distance of `2`.
    pub distance_of_2_bound: usize,
}

impl Default for EditDistanceBounds {
    fn default() -> Self {
        Self {
            distance_of_1_bound: Self::default_min_length_d1(),
            distance_of_2_bound: Self::default_min_length_d2(),
        }
    }
}

impl EditDistanceBounds {
    pub fn default_min_length_d1() -> usize {
        5
    }

    pub fn default_min_length_d2() -> usize {
        8
    }
}