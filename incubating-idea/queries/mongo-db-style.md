# Mongo DB style queries

Part of me likes the mongo DB way of marking operators
and database ops with the `$` prefix, although this
is probably not the best for everyone, I'm curious what others think.

Alternatively look at the [standard-style.md](standard-style.md) for a 
version without the `$` prefix.


## Basic Queries 

```json5
{
  "$fuzzy": "Hell o world",
  // "$text": "name:bob",
  // "term": "bob",
  // "$regex": "bob+"
  "$limit": 20,
  "$offset": 5
}
```

## Specify Fields (Version 1)

```json5
{
  "$fuzzy": {
      "$query": "Hello world",
      "$fields": ["description", "title"]
  },
  "$limit": 20,
  "$offset": 5
}
```

Term queries are a bit ugly however when you want to specify different values for different fields.
```json5
{
  "$terms": {
      "$values": ["hello", "world"],
      "$field": "description"
  },
  "$limit": 20,
  "$offset": 5
}
```

## Specify Fields (Version 2 - More inline with Mongo)
This effectively just does how Mongo's select queries work,
here each field and query **must** match in order for a document
to be selected.

```json5
{
  "description": {
      "$fuzzy": "Hello world",
  },
  "title": {
      "$fuzzy": "Hello world",
  },
  "$limit": 20,
  "$offset": 5
}
```

Term queries are definitely nicer with this approach:
```json5
{
  "description": {
      "$in": ["hello", "world"],
  },
  "title": {
      "$in": ["bob", "ross"],
  },
  "$limit": 20,
  "$offset": 5
}
```

## Or statements (Version 1)
Personally I'm not a fan of calling it `or`, I think `any` is a much better fit and reads nicer:
```json5
{
  "$any": [
    {
      "$fuzzy": {
          "$query": "Hello world",
          "$fields": ["description", "title"]
      },      
    },
    {        
      "$terms": {
          "$values": ["bob", "ross"],
          "$field": "description"
      },
    }
  ],
  "$limit": 20,
  "$offset": 5
}
```

## OR statements (inline with mongo version)
```json5
{
  "$or": [
    {
      "description": {
          "$in": ["hello", "world"],
      }        
    },
    {      
      "title": {
          "$in": ["bob", "ross"],
      },  
    }
  ],
  "$limit": 20,
  "$offset": 5
}
```

