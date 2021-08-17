# Searching An index
Once you've added documents to you're ready to start
searching!

lnx provides you with 3 major ways to query the index:
- (`mode=normal`) The tantivy query parse system, this is not typo tolerant
but is very powerful for custom user queries, think log searches.
- (`mode=fuzzy`, DEFAULT) A fuzzy query, this ignores the custom query 
system that the standard query parser would otherwise handle,
but intern is typo tolerant.
- (`more=more-like-this`) Unlike the previous two options this takes a document
reference address and produces documents similar to the given one.
This is super useful for things like books etc... Wanting related items.

### Required Endpoints
```
GET /indexes/:index_name/search
```

**Endpoint Query Parameters**
- `query`* - A string query which is used to search documents.
- `ref_document`** - A document id in the form of <segment>-<id>.
- `mode` - The specified mode of the query. Out of `normal`, `fuzzy`, `more-like-this`.
- `limit` - Limit the amount of results to return (Default: `20`)
- `offset` - The offset to skip `n` documents before returning any. 
- `order_by` - The field to order content by. This must be a field existing in 
the schema.

`*` = This is required for `normal` and `fuzzy` mode queries but not `more-like-this`.</br>
`**` = This is required for `more-like-this` queries but not `normal` and `fuzzy`.

### Example Search Query
The bellow query searches out `movies` index with the query `hello, world`
this will be matched in the `fuzzy` mode due to it being the default if not
specified, we then limit it to `10` items.
```
GET /indexes/movies/search?limit=10&query=hello, world
```

Our response can look something like this: