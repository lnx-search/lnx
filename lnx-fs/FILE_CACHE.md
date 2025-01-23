# `lnx-fs` file cache

Since `lnx-fs` uses direct IO on all its disk operations, it uses its own internal cache for commonly read 
blocks of files.

## History 

There is a little bit of history with this system, during the rewrite this cache was originally a `moka` _LFU_ cache
where each 32KB block of a file was a separate cache entry, which worked for the most part and allowed the system
to hit over 20GB/s on cached sections of files, but it had some issue both in generic use cases, and how tantivy
expects the access files:

- Memory in cache could not be aligned (which meant anything using rkyv has to be re-allocated)
- We could not merge cacged pages of data that are next to each other into one contiguous blob
  which means the readers must always do another copy of data if they require that behaviour, which
  increases the memory pressure on the system.
    * This occurs for all term dictionary reads, columnar files and uspell indexes.
- It is hard to truly track how much memory is in use and the cache metrics because once we give a `bytes::Bytes`
  object back to the reader, it is out of our control.

## New Design

The new system is designed a lot closer to that of a traditional buffer pool in a database, although we have to
work around some particular constraints like the fact that we should merge the pages into single blobs
when they are next to one another... But more on this later.

In particular, the system is switching to allocating virtual address space using **mmap**.
This may seem a little bit weird, since tantivy itself uses mmap for its core directory, but we chose to write
our own FS layer so we have direct IO and avoid issues like the TLB shutdowns, and signal handling on SIGBUS
errors (which is not really a big issue on hardware mounted drives, but network mounted block storage can be prone to this.)

The main difference here is we are only allocating anonymous memory, and still do the disk IO using direct IO, which
technically still involves an additional memory copy unlike mapping the file directly, but gives us far greater control
over how and when things are cached.

Currently, the layout is in the form of a moka LFU cache holding onto the currently active files and the chunks
that are in the cache, then each file has its own allocated "block" of virtual memory which is mostly just empty
space, overtime if the file is read often, this will build up into a more populated set of pages.

![new design](/assets/lnx-fs-file-cache.svg)
