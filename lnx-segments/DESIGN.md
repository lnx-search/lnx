# lnx segments

Segments are the core backbone of how lnx stores its data. Each segment is in essence, a new tantivy directory + metadata
bundled into a single file. Each file is designed to be completely independent of anything else or any other segments,
effectively allowing you to take any segments from any existing instance, and drop them into a new instance without
loosing data.

These single files can be read with a special directory implementation so that to tantivy, it is a directory. 

Once a segment file has been written, the contents of it are immutable, with one exception; deleted entries are
appended to the end of the file using fixed width rows. These act as tombstones which are later handled by mergers.

## Segment structure
![segment structure](/assets/segment-structure.png)

### File Layout
Files are laid out in the segment in no particular order currently, although this may be subject to change.

Files are joined together as continuous stream of bytes and the relative positions of where each file lies within
the segment are added to the metadata.

The following files are ignored from the original tantivy index:
- `.tantivy-*.lock`
- `.tmp*`

![file layout](/assets/segment-file-layout.png)

### Metadata
Metadata is currently a serialized view of the `Metadata` struct, which intern contains each file path stored within
the segment and the positions for where that file lies.

A unique `HLCTimestamp` is also stored within the metadata which is the segment's unique ID, along with a `index_name`
field which stores the index name that the segment belongs to.

## Producing Segments
Segments are produced by first creating a standard tantivy index which is managed by the `lnx-storage` crate.
The files produced by tantivy are then fed into the `Exporter`, which reads each file in the index and adds
them to the single file sequentially. 

*Note: Files are read separately so that there is room for optimisations on linux 
with the aio file system, see the `aio` directory to see this.*

A new segment is produced for every commit in the system or when 4GB (approx) have been written, this is done to
avoid segments becoming excessively large and hard to manage. 

![segment producing](/assets/segment-producing.png)

## Combining Segments
Because segments are produced on every commit, this can lead to lots of small segments being produced which leads to
too many files being opened at once, and the system being relatively inefficient. 

To mitigate this issue, two or more segments can be combined into a new segment, effectively turning several indexes 
into one larger index.

It's worth noting that the combiner does not invoke tantivy's merging system for the index files, instead they are simply
just joined together as if you copied two folders over into a new folder. Merging of the full index data itself is
handled by the merging system which is a separate handler.

![segment combining](/assets/segment-combining.png)

