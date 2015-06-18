# compressedMaps
compressedMaps library contains a set of concurrent and non-concurrent primitive hash maps.

Both types of maps are using variable length encoding to save as much memory as possible. We also store data in the large blocks
(at least 4k long) thus minimizing the number of objects we create and reducing the GC load. Testing has shown that having
an object per bucket (even if a bucket contains more than one entry) is too expensive in terms of garbage collection.

All maps support fill factors greater than 1 and less than 16 (this is a soft limit) - it allows you to store more than one entry
in the bucket thus enabling delta compression for the keys, which are stored in the sorted order.

We do not impose any limits on the map size (so it is long, not int), though the underlying array definitely can not grow beyond
the usual Java limit of 2G values, so we start increasing fill factor after the underlying map grows to 2G.

We also allow users to specify the memory block recycling limit - the map will keep the blocks after they are no longer used
and then reuse them when a new block should be allocated. This option may reduce your application GC load on the assumption
that memory blocks are kept alive long enough to be promoted in the old heap generation.

The entry point for all map users is info.javaperformance.compressedmaps.CompressedMapsFactory class, which provides
factory methods for all maps implemented in the library.
