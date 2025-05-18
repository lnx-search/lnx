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

## Page Tagging

As well as the data stored in the pages themselves, the system allows the user to store upto `32` bytes worth of
`tag` space, which is stored together will the rest of the pages, making bulk loading of this data quick.

* This data is stored within pages themselves and are therefore encrypted if enabled.

## File Layout

The page store consists of "Page Files", which are isolated parts of the store, each file is made up of the following:

- Allocation Table Bitset
- Page Operations Log (POL)
- Encoded pages

### Core concept

One of the primary challenges faced with this store is that we need to be able to write multiple pages (in same file)
in bulk and as part of an all-or-nothing transaction. However, we also want to avoid solutions like a separate WAL file
or separate metadata files in order to avoid additional flushes and write amplification associated with them.

The solution in this case is the Page Operations Log (POL), which stores a small amount of metadata of the most recent
operations in a log-like format near the start of the file. This is then read in conjunction with the allocation table
to work out what operations should be applied to the store or aborted (in the case of transactions.)

Every so often a "checkpoint" will be performed on the log resulting in the allocation table being updated and the
generation marker being updated to reflect the number of operations seen by the allocation table.
Once this step is complete the POL can be overwritten with new entry for the cycle to restart.

The advantage of this approach is it allows us to still perform multiple operations together as part of a single operation
while still being crash safe.

## Page Layout 

The storage system divides a fixed size file into `N` pages of equal size,
each page is `8192` bytes in length.

Of that specified size, some bytes are reserved:

#### Header 

- Bytes `0..2` are version flags which indicate to the store what encoding the pages have.
  * Version `0x01` uses `layout_v1` encoding which is _not_ encoded.
  * Version `0x02` is not currently used but is reserved for `layout_enc_v1` encoding with AES encryption
    applied after serialization.
- Bytes `2..8` reserved.

#### Footer
- Bytes `-96..` are reserved for metadata.
  * `v0x02` **only**, Bytes `-40..` are used within this reversed space.

#### Versions

##### V1 (`0x01`)

V1 page format consists of a metadata header serialized with `rkyv` followed by the data bytes of the page,
this layout has no additional overhead/reserved space but does not perform any encryption of additional integrity checks.

##### V1 Encrypted (`0x02`)

This uses the same internal layout as [V1](#v1-0x01) but with the addition of being encrypted
at rest using the [XChaCha20Polly1305](https://docs.rs/chacha20poly1305/latest/chacha20poly1305/index.html)
algorithm.

Unlike V1, this system is encrypted and has additional data integrity checks but at the cost of having an additional
`40` bytes overhead per page for reserved space.
