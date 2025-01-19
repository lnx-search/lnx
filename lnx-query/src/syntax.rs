use std::collections::BTreeMap;

use poem_openapi::types::{Example, MaybeUndefined};
use poem_openapi::{Enum, Object, Union};
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct SelectQuery {
    #[serde(rename = "$select")]
    #[oai(rename = "$select")]
    /// The fields to select and return when fetching query results.
    ///
    /// A wild card can be used to select all fields: `$select: ["*"]`.
    pub select_fields: Vec<String>,
    #[serde(default, rename = "$distinct")]
    #[oai(default, rename = "$distinct")]
    /// Equivalent in SQL to `SELECT DISTINCT`, lnx will de-duplicate documents with the
    /// same column values.
    pub distinct: bool,
    #[serde(rename = "$from")]
    #[oai(rename = "$from")]
    /// The table(s) to search and retrieve documents from.
    pub from_tables: String,
    #[serde(default, rename = "$where")]
    #[oai(default, rename = "$where")]
    /// The filtering and matching criteria for the search.
    pub where_clause: MaybeUndefined<WhereClause>,
    #[serde(default, rename = "$parallel")]
    #[oai(default, rename = "$parallel")]
    /// Execute the query across multiple cores if available.
    ///
    /// This is a useful option for analytical workloads.
    pub parallel: bool,
    #[serde(default, rename = "$sort")]
    #[oai(default, rename = "$sort")]
    /// Rules for customising how results are sorted.
    pub sort_by: MaybeUndefined<OneOrManySortBy>,
    #[serde(default = "default_limit")]
    #[oai(default = "default_limit")]
    /// Limits the number of results returned.
    pub limit: usize,
    #[serde(default)]
    #[oai(default)]
    /// The number of results to skip before selecting the returned documents.
    ///
    /// This can be used for pagination however, deep pagination over thousands of documents
    /// may have a performance impact.
    pub offset: usize,
}

fn default_limit() -> usize {
    100
}

#[derive(Debug, Union, Serialize, Deserialize)]
enum OneOrManyTables {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Union, Serialize, Deserialize)]
/// A query clause used to filter and match documents.
///
/// *This is an optional keyword - default behaviour is to match all documents.*
///
/// This clause will calculate a score for each document which describes
/// how strong of a match the document was relative to the query.
/// The score is accessible via the `$score` variable and is the default
/// value used when sorting.
///
/// lnx supports several query types and combinations giving you a great deal of flexibility,
/// most query types have a "simple" and "advanced" syntax that can be used depending on how
/// much control you want over how lnx matches documents. A "simple" syntax is often just the
/// query string and no additional fields.
///
/// lnx supports the following query types:
///
/// #### Text searching & matching
///
/// - _**$fuzzy**_ - BM25 search with typo-tolerance, a certain number of mistakes are allowed
///     for a term depending on the term's length. This can be configured via the query
///     or via the table config.
/// - _**$fulltext**_ - Standard BM25 full-text search.
/// - _**$phrase**_ - Matches a specific sequence of words.
/// - _**$prefix**_ - Matches any term which starts with the provided characters.
/// - _**$morelikethis**_ - Matches documents similar to the results from a provided sub-query or inline payload.
/// - _**$regex**_ - Matches terms using a regex pattern.
///
/// #### Nested expressions
///
/// - _**$all**_ - Match all inner expressions in order to be considered a match.
/// - _**$any**_ - Match at least on of the inner expressions in order to be considered a match.
/// - _**$atleast**_ - Require at least `N` subqueries to match.  TODO: Implement
/// - _**$any**_ - Match at least on of the inner expressions in order to be considered a match.
/// - _**$not**_ - Negates the match result of an inner query.
/// - _**$parse**_ - Parses a query where clause from the provided string with customisable behaviour.
///
/// #### Filtering expressions
///
/// These are query types that can only be applied to fields which are marked as `columnar: true`
/// or `indexed: true` (i.e. strings.)
///
/// _💡 TIP: All fields are `columnar: true` by default unless you have explicitly
/// disabled columnar storage for specific fields._
///
/// - _**$exists**_ - Match any document that has a non-null value present for a set of fields.
/// - _**$range**_ - Match documents with values that lay within the specified range bounds.
/// - _**$eq**_ - Match documents that match the provided value _exactly_ for  a given field.
/// - _**$neq**_ - Match documents that do _not_ match the _exact_ provided value and  a given field.
///     _This is an alias for `$not: { $eq: <value> }.`_
/// - _**$lt**_ - An alias for `$range: { $lt: <value> }`.
/// - _**$lte**_ - An alias for `$range: { $lte: <value> }`.
/// - _**$gt**_ - An alias for `$range: { $gt: <value> }`.
/// - _**$gte**_ - An alias for `$range: { $gte: <value> }`.
///
pub enum WhereClause {
    All(AllExpr),
    Any(AnyExpr),
    AtLeast(AtLeastExpr),
    Not(NotExpr),
    Exists(ExistsExpr),
    Fuzzy(FuzzyExpr),
    FullText(FullTextExpr),
    Phrase(PhraseExpr),
    Prefix(PrefixExpr),
    MoreLikeThis(MoreLikeThisExpr),
    Regex(RegexExpr),
    TextParser(TextParserExpr),
    Eq(EqExpr),
    Neq(NeqExpr),
    Range(RangeExpr),
    Lt(LtExpr),
    Lte(LteExpr),
    Gt(GtExpr),
    Gte(GteExpr),
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct AllExpr {
    #[serde(rename = "$all")]
    #[oai(rename = "$all")]
    /// The document must match against all the inner clauses
    /// in order to be considered a match.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> {
    /// |>     $all: [
    /// |>         { $eq: "example",  $fields: ["source"] },
    /// |>         { $gt: 1736198475, $fields: ["created_at"] },
    /// |>     ]
    /// |> }
    ///
    /// >>> { "id": 1, "source": "example", "created_at": 1736199475 }
    /// >>> { "id": 2, "source": "example", "created_at": 1736199575 }
    /// ```
    pub ctx: Vec<WhereClause>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct AnyExpr {
    #[serde(rename = "$any")]
    #[oai(rename = "$any")]
    /// The document can match against any of the inner clauses
    /// in order to be considered a match.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> {
    /// |>     $any: [
    /// |>         { $eq: "example",  $fields: ["source"] },
    /// |>         { $gt: 1736198475, $fields: ["created_at"] },
    /// |>     ]
    /// |> }
    ///
    /// >>> { "id": 1, "source": "example", "created_at": 1736199475 }
    /// >>> { "id": 2, "source": "example", "created_at": 1736199575 }
    /// >>> { "id": 3, "source": "webpage", "created_at": 1736199575 }
    /// >>> { "id": 4, "source": "example", "created_at": 1636199575 }
    /// ```
    pub ctx: Vec<WhereClause>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct AtLeastExpr {
    #[serde(rename = "$atleast")]
    #[oai(rename = "$atleast")]
    /// Require that the document match _at least_ `$threshold` number of
    /// queries out of the nested sub queries.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> {
    /// |>     $atleast: [
    /// |>         { $gt: 1736198475, $fields: ["created_at"] },
    /// |>         { $eq: "tim", $fields: ["name"] },
    /// |>         { $eq: "bob", $fields: ["name"] },
    /// |>     ],
    /// |>     $threshold: 2,
    /// |> }
    ///
    /// >>> { "id": 1, "name": "tim", "created_at": 1736198476 }
    /// >>> { "id": 2, "name": "bob", "created_at": 1736198476 }
    /// ```
    pub ctx: Vec<WhereClause>,
    #[serde(rename = "$threshold")]
    #[oai(rename = "$threshold")]
    /// The minimum number of inner queries that should match in order
    /// for the document to still be included in results.
    ///
    /// This value cannot be higher than the number of sub-queries.
    pub threshold: usize,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct NotExpr {
    #[serde(rename = "$not")]
    #[oai(rename = "$not")]
    /// Negates the match result of the inner query.
    ///
    /// I.e. Match becomes no-match, and no-match become match.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $not: { $gt: 1736198475, $fields: ["created_at"] } }
    ///
    /// >>> { "id": 4, "source": "example", "created_at": 1636199575 }
    /// ```
    pub ctx: Box<WhereClause>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct ExistsExpr {
    #[serde(rename = "$exists")]
    #[oai(rename = "$exists")]
    /// Matches documents with non-null values in specified fields.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $exists: ["source"] }
    ///
    /// >>> { "id": 1, "source": "example", "created_at": 1736199475 }
    /// >>> { "id": 2, "source": "example", "created_at": 1736199575 }
    /// >>> { "id": 3, "source": "webpage", "created_at": 1736199575 }
    /// >>> { "id": 4, "source": "example", "created_at": 1636199575 }
    /// ```
    pub fields: Vec<String>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct FuzzyExpr {
    #[serde(rename = "$fuzzy")]
    #[oai(rename = "$fuzzy")]
    /// Matches documents with BM25 scoring while being forgiving of spelling mistakes
    /// within the input.
    ///
    /// This is especially useful for user-facing search where it is common to miss-type one or two
    /// characters in a word.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $fuzzy: "John grishame", $fields: ["author"] }
    ///
    /// >>> { "id": 1, "author": "John Grisham" }
    /// >>> { "id": 2, "author": "John Love" }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided search text.
    pub fields: Vec<String>,
    #[serde(rename = "$boost")]
    #[oai(rename = "$boost")]
    /// The boost factor to multiply the resulting score by.
    ///
    /// This is applied **after thresholding**.
    pub boost: f32,
    #[serde(default = "default_true", rename = "$prefixlast")]
    #[oai(default = "default_true", rename = "$prefixlast")]
    /// If `true`, the last term of the `$search` text will be treated as a prefix matching term.
    ///
    /// This means given the text _"hello wor"_, the term _"wor"_ will
    /// match _"word"_, _"worldly"_, _"world"_, etc...
    pub prefix_last_term: bool,
    #[serde(default = "FuzzyExpr::default_one_typo_threshold", rename = "$onetypo")]
    #[oai(default = "FuzzyExpr::default_one_typo_threshold", rename = "$onetypo")]
    /// Specifies the _minimum_ length of a term for it to be allowed upto **1** typo.
    pub one_typo_threshold: usize,
    #[serde(default = "FuzzyExpr::default_two_typo_threshold", rename = "$twotypo")]
    #[oai(default = "FuzzyExpr::default_two_typo_threshold", rename = "$twotypo")]
    /// Specifies the _minimum_ length of a term for it to be allowed upto **2** typos.
    pub two_typo_threshold: usize,
}

impl FuzzyExpr {
    fn default_one_typo_threshold() -> usize {
        5
    }

    fn default_two_typo_threshold() -> usize {
        8
    }
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct FullTextExpr {
    #[serde(rename = "$fulltext")]
    #[oai(rename = "$fulltext")]
    /// Matches and scores documents using the BM26 full-text search.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $fulltext: "old and men", $fields: ["title"] }
    ///
    /// >>> { "id": 1, "title": "The Old Man and the Sea" }
    /// >>> { "id": 2, "title": "Of Mice and Men" }
    /// >>> { "id": 3, "title": "Frankenstein" }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided search text.
    pub fields: Vec<String>,
    #[serde(rename = "$boost")]
    #[oai(rename = "$boost")]
    /// The boost factor to multiply the resulting score by.
    ///
    /// This is applied **after thresholding**.
    pub boost: f32,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct PhraseExpr {
    #[serde(rename = "$phrase")]
    #[oai(rename = "$phrase")]
    /// Matches a set of terms respecting their positions and adjacency exactly.
    ///
    /// For example `hello world` will match `"hello world bob"` but not `"hello bob world"`.
    ///
    /// _💡 TIP: By using the advanced syntax, you can add "slop" allowance which can
    /// make the matching behaviour more forgiving with positioning of terms._
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $phrase: "and men", $fields: ["title"] }
    ///
    /// >>> { "id": 2, "title": "Of Mice and Men" }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided phrase.
    pub fields: Vec<String>,
    #[serde(rename = "$boost")]
    #[oai(rename = "$boost")]
    /// The boost factor to multiply the resulting score by.
    ///
    /// This is applied **after thresholding**.
    pub boost: f32,
    #[serde(rename = "$slop")]
    #[oai(rename = "$slop")]
    /// Slop allowed for the phrase.
    ///
    /// The query will match if its terms are separated by slop terms at most.
    /// The slop can be considered a budget between all terms.
    ///
    /// E.g. _"A B C"_ with slop 1 allows _"A X B C"_, _"A B X C"_, but not _"A X B X C"_.
    ///
    /// Transposition costs 2, e.g. “A B” with slop 1 will not match “B A” but it would with
    /// slop 2 Transposition is not a special case, in the example above A is moved 1 position
    /// and B is moved 1 position, so the slop is 2.
    ///
    /// As a result slop works in both directions, so the order of the terms may be changed as
    /// long as they respect the slop.
    ///
    /// By default, the slop is `0` meaning query terms need to be adjacent.
    pub slop: usize,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct PrefixExpr {
    #[serde(rename = "$prefix")]
    #[oai(rename = "$prefix")]
    /// Match a term of phrase by prefix.
    ///
    /// Inputs like `hel` will match `"hello"`, `"help"`, etc...
    ///
    /// Optionally passing multiple terms i.e. `hello worl` will be counted as a _prefix phrase_
    /// query and match `"hello world"` but not `"hello bob world"` or `"worl hello"`
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $phrase: "frank", $fields: ["title"] }
    ///
    /// >>> { "id": 2, "title": "Frankenstein" }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided prefix.
    pub fields: Vec<String>,
    #[serde(rename = "$boost")]
    #[oai(rename = "$boost")]
    /// The boost factor to multiply the resulting score by.
    ///
    /// This is applied **after thresholding**.
    pub boost: f32,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct MoreLikeThisExpr {
    #[serde(rename = "$morelikethis")]
    #[oai(rename = "$morelikethis")]
    /// Match similar documents.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> {
    /// |>     $morelikethis: {
    /// |>         $from: {
    /// |>             $docs: [
    /// |>                 { "author": "John Grisham", "title": "Ford County" },
    /// |>                 { "author": "John Grisham", "title": "The Racketeer" },
    /// |>             ]
    /// |>         },
    /// |>         $stopwords: ["and", "or", "then", "the"],
    /// |>     },
    /// |> }
    ///
    /// >>> { "id": 2, "title": "The Last Juror" ,   "author": "John Grisham" }
    /// >>> { "id": 1, "title": "The Judge's List",  "author": "John Grisham" }
    /// >>> { "id": 4, "title": "The Street Lawyer", "author": "John Grisham" }
    /// ```
    pub ctx: MoreLikeThisBounds,
    #[serde(rename = "$boost")]
    #[oai(rename = "$boost")]
    /// The boost factor to multiply the resulting score by.
    ///
    /// This is applied **after thresholding**.
    pub boost: f32,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct MoreLikeThisBounds {
    #[serde(rename = "$from")]
    #[oai(rename = "$from")]
    /// The source documents.
    ///
    /// These documents are what the query compares against when considering if
    /// a document is similar or not.
    pub from: MoreLikeThisFrom,
    #[serde(default, rename = "$stopwords")]
    #[oai(default, rename = "$stopwords")]
    /// Words/terms to ignore from the source documents.
    pub stop_words: Vec<String>,
    #[serde(rename = "$docfrequency")]
    #[oai(rename = "$docfrequency")]
    /// Filter out terms that appear in too many documents or too few.
    pub doc_frequency: Option<MoreLikeThisDocFreq>,
}

#[derive(Debug, Union, Serialize, Deserialize)]
pub enum MoreLikeThisFrom {
    Query(MoreLikeThisFromQuery),
    Docs(MoreLikeThisFromDocs),
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct MoreLikeThisFromQuery {
    #[serde(rename = "$query")]
    #[oai(rename = "$query")]
    /// The sub-query to execute to retrieve the documents.
    ///
    /// The content of these documents will determine the matching criteria.
    pub query: Box<SelectQuery>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct MoreLikeThisFromDocs {
    #[serde(rename = "$docs")]
    #[oai(rename = "$docs")]
    /// A set of document objects mapping `fields => values`.
    ///
    /// The content of these documents will determine the matching criteria.
    pub docs: Vec<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct MoreLikeThisDocFreq {
    #[serde(rename = "$min")]
    #[oai(rename = "$min")]
    /// The minimum doc frequency a term must have in order to be considered
    /// matching criteria.
    pub min: Option<usize>,
    #[serde(rename = "$max")]
    #[oai(rename = "$max")]
    /// The maximum doc frequency a term must have in order to be considered
    /// matching criteria.
    pub max: Option<usize>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct RegexExpr {
    #[serde(rename = "$regex")]
    #[oai(rename = "$regex")]
    /// Matches terms using the provided regex pattern.
    ///
    /// Regex syntax can be found here: https://docs.rs/regex/latest/regex/#syntax
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $regex: "m[ea]n", $fields: ["title"] }
    ///
    /// >>> { "id": 1, "title": "The Old Man and the Sea" }
    /// >>> { "id": 2, "title": "Of Mice and Men" }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided regex pattern.
    pub fields: Vec<String>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct TextParserExpr {
    #[serde(rename = "$parse")]
    #[oai(rename = "$parse")]
    /// A customisable text parser allowing users to create their own queries
    /// via the text syntax.
    ///
    /// Parsing rules can be enabled/disabled to fit your use case.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $parse: "\"Jason Borne\" +wallpaper -ai", $fields: ["name"] }
    ///
    /// >>> { "id": 5, "name": "Jason Borne printable wallpaper" }
    /// >>> { "id": 4, "name": "floral wallpaper" }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided search query.
    pub fields: Vec<String>,
    #[serde(rename = "$boost")]
    #[oai(rename = "$boost")]
    /// The boost factor to multiply the resulting score by.
    ///
    /// This is applied **after thresholding**.
    pub boost: f32,
    #[serde(rename = "$options")]
    #[oai(rename = "$options")]
    /// The options used to change how the parser behaves.
    pub parse_options: TextParserConfig,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct TextParserConfig {
    #[serde(default = "default_true", rename = "$boolean")]
    #[oai(default = "default_true", rename = "$boolean")]
    /// If `true`, allows uses to use the boolean operators `AND` & `OR` (case-sensitive) for
    /// combining parts of the query into unions and intersections.
    ///
    /// `AND` takes precedence over `OR`, so that `a AND b OR c` is interpreted as `(a AND b) OR c`.
    pub boolean_ops_enabled: bool,
    #[serde(default = "default_true", rename = "$negative")]
    #[oai(default = "default_true", rename = "$negative")]
    /// If `true`, allows users to mark a query term as _excluded_ meaning it must _not_ appear
    /// within the document matched.
    ///
    /// The syntax for this is `-<term>` and is useful for disambiguating a query.
    /// For example: `apple -fruit`.
    pub negative_ops_enabled: bool,
    #[serde(default = "default_true", rename = "$must")]
    #[oai(default = "default_true", rename = "$must")]
    /// If `true`, allows users to mark a query term as _required_.
    /// The syntax for this is `+<term>`
    pub must_ops_enabled: bool,
    #[serde(default = "default_true", rename = "$phrase")]
    #[oai(default = "default_true", rename = "$phrase")]
    /// If `true`, allows users to match a set of words exactly and respecting the positions.
    ///
    /// The syntax for phrase queries are `"<sentence>"`: `"hello world"` will match `"hello world bob"`
    /// but not `"world hello"`.
    ///
    /// If the target field does have `positions: true` set, this will be silently parsed
    /// as a standard text query.
    ///
    /// _💡 TIP: All fields are `positions: true` by default unless you have explicitly
    /// disabled positions for specific fields._
    pub phrase_ops_enabled: bool,
    #[serde(default, rename = "$slop")]
    #[oai(default, rename = "$slop")]
    /// If `true`, allows users to specify slop in phrase queries using the `~<slop>`
    /// syntax. Which allows to set the phrase’s matching distance in words.
    ///
    /// For example `"big wolf"~1` will return documents containing the phrase `"big bad wolf"`.
    ///
    /// Required `$phrase` to be enabled otherwise this has no affect.
    pub slop_ops_enabled: bool,
    #[serde(default = "default_true", rename = "$range")]
    #[oai(default = "default_true", rename = "$range")]
    /// If `true`, allows users to filter documents by a columnar field by selecting
    /// any values that lie within the parsed range bounds.
    ///
    /// For example: `title:[a TO c}` will find all documents whose title contains a word
    /// lexicographically between a and c (inclusive lower bound, exclusive upper bound).
    /// Inclusive bounds are `[]`, exclusive are `{}`
    pub range_ops_enabled: bool,
    #[serde(default, rename = "$in")]
    #[oai(default, rename = "$in")]
    /// If `true` allows inputs to match a field against a set of literals.
    ///
    /// For example: `title: IN [a b cd]` is equivalent to `title:a OR title:b OR title:c`.
    pub set_ops_enabled: bool,
    #[serde(default = "default_true", rename = "$parsewildcard")]
    #[oai(default = "default_true", rename = "$parsewildcard")]
    /// If `true` a plain `*` will match all documents.
    pub parse_wildcards: bool,
    #[serde(default = "default_true", rename = "$boost")]
    #[oai(default = "default_true", rename = "$boost")]
    /// If `true`, users can use boost ops using the `^<boostfactor>` syntax.
    ///
    /// For instance, "SRE"^2.0 OR devops^0.4 will boost documents containing SRE
    /// instead of devops. Negative boosts are not allowed.
    pub boost_ops_enabled: bool,
    #[serde(default, rename = "$fuzzyterms")]
    #[oai(default, rename = "$fuzzyterms")]
    /// If `true`, tokenized terms use fuzzy matching with typo tolerance.
    pub fuzzy_terms: bool,
    #[serde(default = "default_true", rename = "$strict")]
    #[oai(default = "default_true", rename = "$strict")]
    /// If the parser should reject the query if it cannot fully
    /// parse the input or if it should silently ignore errors.
    pub strict: bool,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct RangeExpr {
    #[serde(rename = "$range")]
    #[oai(rename = "$range")]
    /// Selects documents which have values that lay within the specified range bounds.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $range: { $gte: 5, $lt: 500 }, $fields: ["likes"] }
    ///
    /// >>> { "id": 1, "name": "Bob", "likes": 16 }
    /// >>> { "id": 7, "name": "Tim", "likes": 200 }
    /// ```
    pub ctx: RangeBounds,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided range bounds.
    pub fields: Vec<String>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct RangeBounds {
    #[serde(rename = "$lt")]
    #[oai(rename = "$lt")]
    /// Matches values which are _less than_ the provided value.
    ///
    /// This keyword is mutually exclusive to `$lte`.
    pub lt: MaybeUndefined<String>,
    #[serde(rename = "$lte")]
    #[oai(rename = "$lte")]
    /// Matches values which are _less than or equal to_ the provided value.
    ///
    /// This keyword is mutually exclusive to `$lt`.
    pub lte: MaybeUndefined<String>,
    #[serde(rename = "$gt")]
    #[oai(rename = "$gt")]
    /// Matches values which are _greater than_ the provided value.
    ///
    /// This keyword is mutually exclusive to `$gt`.
    pub gt: MaybeUndefined<String>,
    #[serde(rename = "$gte")]
    #[oai(rename = "$gte")]
    /// Matches values which are _greater than or equal to_ the provided value.
    ///
    /// This keyword is mutually exclusive to `$gte`.
    pub gte: MaybeUndefined<String>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct EqExpr {
    #[serde(rename = "$eq")]
    #[oai(rename = "$eq")]
    /// Matches values which are _equal to_ the provided value.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $eq: "tim", $fields: ["name"] }
    ///
    /// >>> { "id": 7, "name": "Tim" }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided value.
    pub fields: Vec<String>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct NeqExpr {
    #[serde(rename = "$neq")]
    #[oai(rename = "$neq")]
    /// Matches values which are _not equal to_ the provided value.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $eq: "tim", $fields: ["name"] }
    ///
    /// >>> { "id": 7, "name": "Bob" }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided value.
    pub fields: Vec<String>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct LtExpr {
    #[serde(rename = "$lt")]
    #[oai(rename = "$lt")]
    /// Matches values which are _less than_ the provided value.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $lt: 100, $fields: ["likes"] }
    ///
    /// >>> { "id": 7, "name": "Bob",     "likes": 15 }
    /// >>> { "id": 3, "name": "Pickles", "likes": 39 }
    /// >>> { "id": 2, "name": "Timmy",   "likes": 99 }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided value.
    pub fields: Vec<String>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct LteExpr {
    #[serde(rename = "$lte")]
    #[oai(rename = "$lte")]
    /// Matches values which are _less than or equal to_ the provided value.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $lt: 100, $fields: ["likes"] }
    ///
    /// >>> { "id": 7, "name": "Bob",      "likes": 15 }
    /// >>> { "id": 3, "name": "Pickles",  "likes": 39 }
    /// >>> { "id": 4, "name": "Veronica", "likes": 100 }
    /// >>> { "id": 2, "name": "Timmy",    "likes": 99 }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided value.
    pub fields: Vec<String>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct GtExpr {
    #[serde(rename = "$gt")]
    #[oai(rename = "$gt")]
    /// Matches values which are _greater than_ the provided value.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $gt: 50, $fields: ["likes"] }
    ///
    /// >>> { "id": 4, "name": "Veronica", "likes": 100 }
    /// >>> { "id": 2, "name": "Timmy",    "likes": 99 }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided value.
    pub fields: Vec<String>,
}

#[derive(Debug, Object, Serialize, Deserialize)]
pub struct GteExpr {
    #[serde(rename = "$gte")]
    #[oai(rename = "$gte")]
    /// Matches values which are _greater than or equal to_ the provided value.
    ///
    /// #### Example:
    ///
    /// ```json5
    /// |> { $gte: 39, $fields: ["likes"] }
    ///
    /// >>> { "id": 3, "name": "Pickles",  "likes": 39 }
    /// >>> { "id": 4, "name": "Veronica", "likes": 100 }
    /// >>> { "id": 2, "name": "Timmy",    "likes": 99 }
    /// ```
    pub ctx: String,
    #[serde(rename = "$fields")]
    #[oai(rename = "$fields")]
    /// The fields to attempt to match with the provided value.
    pub fields: Vec<String>,
}

#[derive(Debug, Union, Serialize, Deserialize)]
/// Describes how a document should be sorted.
pub enum OneOrManySortBy {
    /// Sort the documents by one field/variable.
    One(SortBy),
    /// Sort the documents by multiple fields/variables.
    ///
    /// The documents will be sorted in lexicographical order
    /// relative to the order the clauses are defined as.
    ///
    /// For example:
    ///
    /// ```json5
    /// $sort: [
    ///     { $by: "rating", $order: "desc" },
    ///     { $by: "age", $order: "asc" },
    ///     { $by: "score", $order: "desc" },
    /// ]
    /// ```
    ///
    /// Will sort the results by `rating` in _descending order_ first,
    /// then any "equal" ratings will be sorted by `age` in _ascending order_.
    /// Finally, any remaining equally sorted documents will be sorted by `score` in _descending order_.
    Many(Vec<SortBy>),
}

#[derive(Debug, Object, Serialize, Deserialize)]
#[oai(example = true)]
/// Describes how a document should be sorted.
pub struct SortBy {
    #[serde(rename = "$by")]
    #[oai(rename = "$by")]
    /// The field or variable to sort the document results by.
    ///
    /// This defaults to `$score` which is a magic variable using the
    /// score of the query produced by the `$where` clause.
    pub by: String,
    #[serde(default, rename = "$order")]
    #[oai(default, rename = "$order")]
    /// The order to return the sorted results by.
    pub order: Order,
}

impl Example for SortBy {
    fn example() -> Self {
        Self {
            by: "$score".to_string(),
            order: Order::Desc,
        }
    }
}

#[derive(Debug, Default, Enum, Serialize, Deserialize)]
#[oai(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
/// The order to return the sorted results by.
pub enum Order {
    #[default]
    /// Order results in descending order (highest to lowest.)
    Desc,
    /// Order results in ascending order (lowest to highest.)
    Asc,
}

fn default_true() -> bool {
    true
}
