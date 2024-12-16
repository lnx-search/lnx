# lnx-fs

An object-storage list abstraction over a file system.

This is optimized for writing and reading data _fast_ without relying on the file system cache.

The system is completely asynchronous using Direct IO and io_uring (sorry non-linux people!) backed
by the `glommio` runtime. 

It works by having immutable "tablets" where all writes are sequentially appended to the end of an active tablet,
once that tablet reaches a certain size, it is closed and a new tablet is created.
This allows the system to write very small blobs _very_ quickly which is good in lnx's use case where
some writes might only be one or two documents.

The downside to this approach is it uses a bit more disk space that strictly necessary, and relies on a periodic
GC job to compact tablets and cleanup dead blobs.

Ensuring durability of objects is largely down to the metastore, which is a SQLite database, files are only
registered in the metastore once the blob itself has been successfully flushed to disk.

## Buckets

Like S3, the system has the concept of buckets, they allow you to completely isolate two set of files or blobs which
can allow for mounting files on multiple disks, etc... 

The downside is too many buckets leads to the system not running efficiently because writes are too sparse across
the buckets. It is only recommended to have a couple buckets at most.

## Config Persistence

The system itself is self-contains, and each bucket has its own set of config values which can be set and unset
at runtime which will persist between restarts. When config values are unset they fall back to sane defaults.

## Example

```rust
use std::path::PathBuf;
use lnx_fs::{VirtualFileSystem, RuntimeOptions, FileSystemError, Body, Bytes};

#[tokio::main]
async fn main() -> Result<(), FileSystemError> {
    let options = RuntimeOptions::builder()
        .num_threads(2)
        .build();

    let mount_point = PathBuf::from_str("/tmp/my-fs/").unwrap();
    let service = VirtualFileSystem::mount(mount_point, options).await?;
    service.create_bucket("demo").await?;
    
    let bucket = service.bucket("demo").unwrap();
    
    let body = Body::complete(Bytes::from_static(b"Hello, World!"));
    bucket.write("example.txt", body.clone()).await?;
    
    let incoming = bucket.read("example.txt").await?;
    let data = incoming.collect().await?;
    assert_eq!(data, body);
}
```

## Internals

lnx FS internally acts like an LSM tree specifically for blobs, the API it presents is something similar to that
of S3 but with some additional features and limitations. 

The system itself makes use of io_uring and DIRECT_IO via `glommio`, which is why there is no file cache being
hit which may or may not impact your reads.

### Reserved paths

Internally lnx FS uses its own file store to hold onto various bits of metadata and signals, to avoid
accidental corruption of this data, the system will write internal files under the prefix:

```
__lnx_fs/
```

The system will not allow you to write a file under this prefix yourself.


### Metastore state

Internally, metadata of files is always kept in memory since it is assumed that the amount of memory this takes
up is very small relative to the amount of data in the blobs themselves. 

Each tablet appends a footer to the end of each blob it writes to a tablet along with a set of magic bytes
that allow recovery from zero by scanning from the end of the tablet and moving towards the start. 

Alongside this footer, the writers will asynchronously write to a more compact metadata file in `tablet_metadata/`
that contains a set of MSGPACK buffers with the same data as the blob footer in the main tablet file.
This is intended to be used as the main source of persistent under normal operations as it is faster
to scan and read after a restart.
The metadata file is laid out like so:

```
<buffer_len_u32><crc32_checksum_u32><mmsgpack_buffer>
```

#### Metadata file corruption

The metadata files are written asynchronously and only occasionally flushed to disk, this means it is possible
for data to be missing from these metadata files. 

When the system first starts up, it reads all the content from each metadata file and then works out 
what tablets it needs to partially, or entirely re-scan in order to recover the original state.

*Once the first write is completed and flushed in the main tablet file, the data is **always** recoverable.*

#### Checkpoint file layout

The actual formatting of `.ckpt` files is a `MSGPACK` format with the layout:

```
{
    "events": [
        {
            "created_at": u64,
            "transaction_id": string | null,
            "data": {
                "Create": {
                    "file_path": string,
                    "data_range": {
                        "start": u64,
                        "end": u64
                    }
                }
            }
        },
        {
            "created_at": u64,
            "transaction_id": string | null,
            "data": {
                "Delete": {
                    "file_path": string
                }
            }
        },
        {
            "created_at": u64,
            "transaction_id": string | null,
            "data": {
                "Rename": {
                    "from_path": string,
                    "to_path": string
                }
            }
        },
    ],
    "observed_writer_position": u64,
}
```

### Operations

The system also has only two "core" operations, `write` and `read`, the rest of the operations
are pseudo operations

- **Write** -> Writes an arbitrary non-zero length buffer as a given file, the file event kind is `Create`.
- **Read** -> Reads a blob with the given file path from within the virtual file system.
- **Delete** -> Writes new entry in the tablet but with a zero-length buffer, the file event kind is `Delete`.
- **Rename** -> Writes a new entry in the tablet but with zero-length buffer, the file event kind is `Rename`.

NOTE: File events are attached to each operation written to the tablet, this is how the system knows how
to apply operations even if they all look like zero-length writes.


#### Bulk Transactions

One of the key differences between this system and S3, is that it allows writing and deleting multiple files
in bulk as part of an all-or-nothing operation. 

But there is a problem, as we mentioned earlier, every blob is given a footer when it is first written to disk,
and our transactions are mostly just around metadata (we still need to write the blob to disk before we commit)
but our writers have no way of knowing once a file is committed or rolled back, and they don't want to wait or
try and overwrite failed transactions, that would be too sensitive to abrupt system failure.

So the solution is each time you create a bulk transaction, the system will allocate a `Ulid` called the
`transaction_id` which is persisted in the footer alongside each blob's metadata.

When a `transaction_id` is present in the blob, the system will expect a signal file to exist under:

```
__lnx_fs/transactions/<transaction_id>.commit
```

To indicate that the transaction was a success. If this file is missing, the transaction will be considered
a failure and the files will be assumed dead.

Writing the commit signal file is the last fallible step in the transaction.