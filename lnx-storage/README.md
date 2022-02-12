# lnx storage layer

This implements the base storage system responsible for managing documents, metadata and
the secondary index storage.

### The `DocStore` trait
This trait should provide the persistent storage for the raw documents that
have been added. Each document is tagged with a created at timestamp which must be
persisted along with the document.

### The `ChangeLogStore` trait
This implements the required logic for tracking changes made the index data.

### The `EngineStore` trait
This trait should provide the persistent storage for metadata handling for things like
current nodes, schemas, etc...

### The `MetaStore` trait
This trait is responsible for storing index specific additional data 
e.g. stop words, current index stamp, etc...

