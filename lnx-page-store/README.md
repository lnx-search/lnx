# lnx-page-store

A traditional RDMS style storage system backed by pages of data with a few tweaks.

The main goal of this project is to provide fast and predictable storage for the lnx system backed by io_uring and
provides a few unique features:

- All operations are asynchronous
- Only requires 2 background threads to operate
- Produces contiguous slices of memory from reads

## Features 

- Encryption at rest
- Asynchronous IO
- Atomic bulk operations
- Minimal write amplification

## Development

#### Running Miri tests

```shell
cargo +nightly miri nextest run -p lnx-page-store --all-features
```

#### Running Base tests

Remove the `test-huge-pages` feature if your OS does not support huge pages (or have it enabled.)
```shell
cargo +nightly nextest run -p lnx-page-store --features test-huge-pages
```


## Durability & Consistency Behavior

The storage implementation puts heavy emphasis on ensuring data durability and consistency, and as such, a lot of
behaviour is mirrored from Postgres which we use a good target, so if we're not sure about how we should act under
a certain condition, we normally mirror Postgres.

This means that there are few specific behaviours:

- An error on `fsync` calls forces the file into a read-only mode and the system will enter a recovery mode,
  otherwise we risk data loss if we allow the user to continue interacting with the store without first recovering.
- During recovery, in the very rare edge case that a fsync call failed and an error is returned to the user,
  but the contents of the WAL still ended up on stable storage, it is possible for store to apply the operations
  of WAL entries that were part of a transaction that was reported to the user as failed.

