# lnx Document

A core standalone crate for storing dynamic JSON-like data in binary formats.

This system has the core document value types which are used within lnx, and the block format.

### Format
You may notice that documents themselves are just an array of layouts which describe the format
of the document rather than containing the values directly.

This is because documents are stored in blocks within lnx, and all values are grouped and merged together
in order to aid compression and avoid padding overhead (more on this later.)

The size on disk is around the same size as JSON data is which although not the compact format, allows for total
zero-copy deserialization meaning the doc store performs no allocations when returning docs to the user.
(This is excluding the decompression allocation if a block is not in the cache.)

This approach has several advantages:
- Removes any overhead from the process of taking the data on disk to returning the data to the user.
- The true memory usage used by the doc store cache can be calculated simply by taking the length of the buffer.
- Merge operations for combining sparse blocks into dense blocks become significantly more efficient both memory
  and CPU wise.