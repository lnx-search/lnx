# Adding / Removing Documents

Documents can only be added or removed, not updated.
This is preferred to be done explicitly hence no update endpoint.

### Endpoints Required
Adds one or more documents to the given index.
This can either return immediately or wait for all operations 
to be submitted depending on the `wait` query parameter.
```
POST /indexes/:index_name/documents?wait=<bool>
```

Deletes any documents matching the set of given terms. (Requires a JSON body.)
```
DELETE /indexes/:index_name/documents
```


## Adding a document
Adding a document is relatively simple, you can either add a single
document represented as a JSON object or you can submit a array of object.

Every document is checked for the required fields, if any docs are missing
fields the *entire* request is rejected.

```
POST /indexes/:index_name/documents?wait=<bool>
```

If `wait=true` (recommended) the system will wait for all operations (one operation per doc)
to be submitted before returning. The writer has a 20 operation buffer limit, this can be
useful for client backpressure waiting for when to send more documents or not.

Not waiting can quickly add a lot of load on the server which may be un-desirable.

#### Representing Data Types
*Most* data types can be represented as raw 

#### Example Body (Singular)
```js
{
    "title": {
        // both `string` and `text` fields use the `text` data type representation.
        "type": "text",   
        "value": "Hello, World",
    },
    "some_data": {
        "type": "bytes",
        // Bytes are encoded as a standard base64 string.
        "value": "eW91IGRpZCB0aGUgZWFzdGVyIGVnZywgaGF2ZSBhIGNvb2tpZQ=="
    },
    "some_datetime": {
        "type": "date",
        // Datetime fields are handled as a Timezone naive UTC timestamp.
        "value": 2308762358023
    },
}
```

#### Example Body (Multiple)
```js
[
    {
        "title": {
            "type": "text",   
            "value": "Hello, World",
        },
        "some_data": {
            "type": "bytes",
            "value": "eW91IGRpZCB0aGUgZWFzdGVyIGVnZywgaGF2ZSBhIGNvb2tpZQ=="
        },
        "some_datetime": {
            "type": "date",
            "value": 2308762358023
        },
    },
    {
        "title": {
            "type": "text",   
            "value": "Hello, World 2",
        },
        "some_data": {
            "type": "bytes",
            "value": "eW91IGRpZCB0aGUgZWFzdGVyIGVnZywgaGF2ZSBhIGNvb2tpZQ=="
        },
        "some_datetime": {
            "type": "date",
            "value": 2308762358023
        },
    },
]
```