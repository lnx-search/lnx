[
    {
        "query": {
            "fuzzy": [
                {"$should": "Hello, world!", "$fields": [], "$field": ""},
                {"$must": "Hello, world!"},
                {"$must_not": "Hello, world!"}
            ],
            "text": {

            },
            "$all": [
                { "text": { "$should": "my-text" } }
            ],
            "$any": [],
            "$none": [],
        }
    }
]

