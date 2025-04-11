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

- Bytes `0..2` are version flags which indicate to the store what encoding the pages have.
  * Version `0x01` uses `layout_v1` encoding which is _not_ encoded.
  * Version `0x02` is not currently used but is reserved for `layout_enc_v1` encoding with AES encryption
    applied after serialization.
- Bytes `2..8` reserved.
- 
- `v0x02` **only**, Bytes `-128..` are reserved for encryption metadata.
- `0` to `7` padding bytes may be present at the end in order to align the data to blocks of `8`.
  * This is applied _after_ any reserved bytes for specific versions.

