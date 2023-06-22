# lnx replication

This is an eventually consistent replication system built upon `datacake`, there are a few ways this is used:

### Metadata replication
Metadata (i.e. Settings, schema changes, etc...) are replicated and stored as is by `datacake` and it's KV layer.

This is because there isn't a lot of optimisation or special handling to be done, unlike the doc store system.

### Document replication
The replication system does a few things:

- Acts as a WAL before documents get indexed
- Replicates the documents across the nodes
- Manages and merges blocks of documents for better compression