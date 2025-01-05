A select query allows you to retrieve data from a table.

It uses a JSON5-format with a SQL-like structure and set of keywords.
Keywords are prefixed with `$` to indicate it is a keyword rather than a
arbitrary value.

### Basic structure

All queries have a core structure similar to SQL, meaning we define
the fields to retrieve, what table to select from and the querying conditions.

Key fields can be double-quoted or have no quotes at all.

```json5
{
    // Alternatively you can use the wildcard "*".
    // $select: "*",
    $select: ["id", "title", "description", "categories"],
    $from: "books",
    $where: {
        $fuzzy: "John Grishm",
        // $fields: ["title", "description"],   // To search specific fields
    },
    // Sorting can be configured to adjust how the system sorts the final results
    // the $score value is a magic variable computed by the query conditions.
    // the default $order is always `desc`.
    $sort: { $by: "$score", $order: "desc" },
    $limit: 100,
    $offset: 10,
}
```

### Selecting fields - `$select`

*This is a required keyword.*

The `$select` keyword expects one of the following syntaxes:

_**Select all fields**_
```json5
{ $select: "*", ... }  
```

_**Select specific fields**_
```json5
{ $select: ["title", "description"], ... }  
```

### Specifying the table - `$from`

*This is a required keyword.*

You can query one or more tables, currently, when providing multiple tables, every table
must have the fields being selected and filtered by within their schema.
When using a wildcard `$select`, the schemas of the tables must match exactly.

This behaves like `UNION ALL` in SQL.

The `$from` keyword expects one of the following syntaxes:

_**Query single table**_
```json5
{ $from: "books", ... }  
```

_**Query multiple tables**_
```json5
{ $from: ["books", "movies"], ... }  
```

### Filtering & scoring documents - `$where`

*This is an optional keyword - default behaviour is to match all documents.*

lnx supports several query types and combinations giving you a great deal of flexibility,
most query types have a "simple" and "advanced" syntax that can be used depending on how
much control you want over how lnx matches documents. A "simple" syntax is often just the
query string and no additional fields.

lnx supports the following query types:

#### Text searching & matching

- _**$fuzzy**_ - BM25 search with typo-tolerance, a certain number of mistakes are allowed
                 for a term depending on the term's length. This can be configured via the query
                 or via the table config.
- _**$fulltext**_ - Standard BM25 full-text search.
- _**$phrase**_ - Matches a specific sequence of words.
- _**$prefix**_ - Matches any term which starts with the provided characters.
- _**$morelikethis**_ - Matches documents similar to the results from a provided sub-query or inline payload.
- _**$regex**_ - Matches terms using a regex pattern.

#### Nested expressions

- _**$all**_ - Match all inner expressions in order to be considered a match.
- _**$any**_ - Match at least on of the inner expressions in order to be considered a match.
- _**$parser**_ - Parses a query where clause from the provided string with customisable behaviour.

#### Columnar filtering

These are query types that can only be applied to fields which are marked as `columnar: true`.

_💡 TIP: All fields are `columnar: true` by default unless you have explicitly
disabled columnar storage for specific fields._

- _**$exists**_ - Match any document that has a non-null value present for a set of fields.
- _**$range**_ - Match documents with values that lay within the specified range bounds.
- _**$eq**_ - Match documents that match  the provided value _exactly_ for  a given field.
- _**$neq**_ - Match documents that do _not_ match the _exact_ provided value and  a given field.
- _**$lt**_ - An alias for `$range: { $lt: <value> }`.
- _**$lte**_ - An alias for `$range: { $lte: <value> }`.
- _**$gt**_ - An alias for `$range: { $gt: <value> }`.
- _**$gte**_ - An alias for `$range: { $gte: <value> }`.

### Sorting results - `$sort`

*This is an optional keyword - default behaviour is to sort by score in descending order.*



### Limiting results returned - `$limit`

*This is an optional keyword - default behaviour is to return the top 100 documents*

### Offset results returned - `$offset`

*This is an optional keyword - default behaviour is to have no offset*