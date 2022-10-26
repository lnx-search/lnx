use utoipa::ToSchema;
use serde::Deserialize;


#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ValueSelector<V> {
    Single(V),
    Multi(Vec<V>),
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum Selector<D: Into<Q>, Q> {
    #[schema(example = "Hello, world")]
    /// A raw query value which assumes all defaults.
    /// 
    /// Example query structure:
    /// 
    /// ```json
    /// {
    ///     // This assumes the default settings so our
    ///     // produced query will use the default occur selector
    ///     // and default search fields.
    ///     "text": "Hello, world"
    /// }
    /// ```
    Direct(D),

    #[schema(example = json!({"$must_not": "Hello, world", "$fields": ["title", "body"]}))]
    /// A single query part which has adjustable settings.
    /// 
    /// Example query structure:
    /// 
    /// ```json
    /// {
    ///     "text": {
    ///         // This changes our query to match only documents
    ///         // that *do not* match the text.
    ///         "$must_not": "Hello, world",
    ///         "$fields": ["title", "body"]
    ///     }
    /// }
    /// ```
    Single(Q),

    #[schema(example = json!([123, 456, 789]))]
    /// A set of queries that all use the default settings.
    /// 
    /// This is largely just a convenience API for queries
    /// which may want multiple values that could match. I.e. `term` queries.
    /// 
    /// Example query structure:
    /// 
    /// ```json
    /// {
    ///     "term": [
    ///         123,
    ///         456,
    ///         789
    ///     ]
    /// }
    /// ```
    /// 
    /// This is equivalent to the more verbose structure:
    /// 
    /// ```json
    /// {
    ///     "$all": [
    ///         {"term": 123},
    ///         {"term": 456},
    ///         {"term": 789},
    ///     ] 
    /// }
    /// ```
    MultiDirect(Vec<D>),

    #[schema(
        example = json!([
            {"$must": "world", "$fields": ["title"]}, 
            {"$must_not": "hello"},
        ])
    )]
    /// A set of queries to perform each with their own individual settings.
    /// 
    /// ```json
    /// {
    ///     "term": [
    ///         {"$must_not": "Hello"},
    ///         {"$must": "world"}
    ///     ]
    /// }
    /// ```
    Multi(Vec<Q>)
}
