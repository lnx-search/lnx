# Creating An Index
Creating an index is fairly simple, once made lnx will it as a persistent config so that you 
dont need to continuously keep redefining it.

### Endpoints Required
```
POST /indexes
```

### Gotcha Moments
Creating an index can create quite a few gotcha moments so its important to understand the behaviour
of the system before ramming your head into a wall wondering why things arent updating or being
destroyed.

- This system is append / delete only, to update an index it must be deleted and re-made, all 
data is lost in the process. This is because the index is schema-full and requires fields exist
if define (This may change in future releases.)
- Creating indexes are **VERY HEAVY** operations, not only are you acquiring a global lock across
all indexes stopping anything else completing operations but you are also spawning and creating
several threads and thread pools (more on this later.) They should not be made randomly during 
usage and should not be made via some user input *THIS WILL CAUSE SERIOUS ISSUES OTHERWISE*.
- Deleting indexes clear all data stored, this means any previously uploaded documents will be
removed.
- The `max_concurrency` determines the amount of readers proportionally, and 
the `reader_threads` determines the amount of threads each reader gets, 
therefore you spawn at least `max_concurrency` x `reader_threads` + `writer_threads` + `1` (for writer thread)

### Support Field Data Types
- `string` This is similar to `text` but wont be indexed. (Supports `stored` bool)
- `text` This is similar to `string` but will be indexed. (Supports `stored` bool)
- `bytes` A arbitrary bytes field with given (Supports [BytesOptions](/getting_started/creating_a_index.html#bytes-options))
- `f64` A 64 bit floating point integer field. (Supports [IntOptions](/getting_started/creating_a_index.html#int-options))
- `i64` A 64 bit signed integer field. (Supports [IntOptions](/getting_started/creating_a_index.html#int-options))
- `u64` A 64 bit unsigned point integer field. (Supports [IntOptions](/getting_started/creating_a_index.html#int-options))
- `date` A UTC datetime field, stored as a u64 integer. (Supports [IntOptions](/getting_started/creating_a_index.html#int-options))


#### Int Options
If a field is *in italic* it's optional and not required.

- `indexed`: bool - Whether or not to index this field. 
Setting an integer as indexed will generate a posting list for each value taken by the integer.

- *`fast`: 'single' or 'multi'* - Set the field as a fast field with a
given cardinality which makes it either a single-value or multi-value field. 
Fast fields are designed for random access.
Access time are similar to a random lookup in an array.
If more than one value is associated to a fast field, only the last one is kept.

- `stored`: bool - Set the field as stored.
Only the fields that are set as *stored* are persisted into the store.

#### Bytes Options
If a field is *in italic* it's optional and not required.

- `indexed`: bool - Whether or not to index this field. 
Setting an integer as indexed will generate a posting list for each value taken by the integer.

- *`fast`: bool* - Set the field as a single-valued fast field. 
Fast fields are designed for random access.
Access time are similar to a random lookup in an array.
If more than one value is associated to a fast field, only the last one is kept.

- `stored`: bool - Set the field as stored.
Only the fields that are set as *stored* are persisted into the store.

### The Request Payload
The `/indexes` endpoint expects a POST request with a `application/json` body, and example 
payload looks like:

```js
{
    "name": "my-index",  // The name of the index, must not contain '/'

    "writer_buffer": 6000000,  // The buffer size in bytes for the writer to use.
    "writer_threads": 1,    // The number of threads for the index writer to use.

    "reader_threads": 1,   // The number of threads for each reader to use. 
    
    "max_concurrency": 10,  // The maximum amount of searches / readers at one time.
    "search_fields": [   // The list of fields that are used when searching.
        "title" 
    ],
    
    // The backend to use, choices of 'memory', 'tempdir', and 'filesystem'
    "storage_type": {   
        "mode": "memory",
        // if we were using 'filesystem' then we would need to give a path.
        // "path": "/store",
    },  
    
    // The actual fields for this schema.
    // NOTE: not every field defined is used to search.
    "fields": {   
        "title": {
            // We defined the field type as `text` this means it will be indexed.
            "type": "text",   
          
            // This will be stored, compressed and returned when returning data.
            "stored": true,   
        },
        "description": {
           "type": "text",  
           "stored": true,
        },
        "id": {
            // Types supported include: u64, i64, f64, date, text, string, bytes
            "type": "u64",    
    
            // Optionally if true this will be indexed (Only for int fields and date)
            "indexed": true,  
            "stored": true,   
  
            // Integer fields can be defined as fast fields, this allows us to apply
            // some additional optimisations by telling tantivy if it's a single value
            // or multi-value field.
            "fast": "single", 
        },    
    },
    
    // Boost fields are what we can use to adjust how much weight we give to
    // specific fields to adjust how much bias there is to each field.
    "boost_fields": {   
      "title": 2.0,     
      "description": 0.8,
    }
}
```

### Success!
Providing the given payload data is valid you have successfully made an index.

This can error if you assign more threads than your system can reasonably assign.

#### Performance Note
More threads != faster, you should test and benchmark different ratios of reader threads
and concurrency as that will have the biggest affect on performance.