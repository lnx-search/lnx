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

![segment structure](/assets/segment-file-layout.png)

### Metadata
Metadata is currently a serialized view of the `Metadata` struct, which intern contains each file path stored within
the segment and the positions for where that file lies.

A unique `HLCTimestamp` is also stored within the metadata which is the segment's unique ID, along with a `index_name`
field which stores the index name that the segment belongs to.
