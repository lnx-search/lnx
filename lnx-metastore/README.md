# lnx Metastore

This is where all important metadata is stored in a read-optimised fashion.

Internally this is a wrapper around an LMDB instance and uses `rkyv` to (de)serialize metadata entries.


### Naming conventions

When creating a new database (sub-metastore) please follow the convention of prefixing with
```
lnx_metastore__
```

The default metastore database is `lnx_metastore__default`.