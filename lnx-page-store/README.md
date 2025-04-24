# lnx-page-store

A traditional RDMS style storage system backed by pages of data with a few tweaks.

The main goal of this project is to provide fast and predictable storage for the lnx system backed by io_uring and
provides a few unique features:

- All operations are asynchronous
- Only requires 2 background threads to operate
- Produces contiguous slices of memory from reads

### Page Spec

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
- `v0x02` **only**, Bytes `-40..` are reserved for encryption metadata.


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