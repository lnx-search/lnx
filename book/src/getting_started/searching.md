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
GET /indexes
```