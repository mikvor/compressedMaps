/*
 * (C) Copyright 2015 Mikhail Vorontsov ( http://java-performance.info/ ) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *      Mikhail Vorontsov
 */

package info.javaperformance.malloc;

/**
 * This class contains logic for packing/unpacking bucket information into a {@code long} value.
 *  A {@code long} contains:
 *  bits  0 - 19: offset in a block (bound by MAX_BLOCK_SIZE=1M)
 *  bits 20 - 27: chain length, can't be zero, FF is used as an indicator that block length is encoded at the start of the bucket
 *  bits 28 - 31: reserved for now, could be used to store encoding scheme or anything else
 *  bits 32 - 63: chain id (full int range)
 *
 *  There are 2 forbidden encodings: 0 and 1 (you can never have them due to non zero length requirement)
 */
public class LongBucketEncoding {
    private static final int MAX_BLOCK_SIZE_BITS = 20;
    private static final int MAX_BLOCK_SIZE = 1 << MAX_BLOCK_SIZE_BITS;
    /** Maximal length of a bucket you can have encoded in {@code long} */
    public static final int MAX_ENCODED_LENGTH = 0xFF;

    /**
     * Empty cell, convenient value because of memory is set to zeroes by default and we have cheaper operations
     * if one arg is a constant zero
     */
    public static final long EMPTY = 0;
    /** Relocated cell */
    public static final long RELOCATED = 1;

    /**
     * This method is used to keep the number of blocks under control by gradually increasing the new block
     * size after reaching certain threshold in the blocks storage. This is needed to ensure that we don't impact GC too much.
     *
     * The formula is 4 kilobyte blocks until we reach 16.384 blocks in the storage and when add another 4K for each
     * 16.384 blocks in the storage.
     * It can be efficiently calculated by shifts: 16384 = 2^14, 4096 = 2^12.
     *
     * @param blocksActive Current block storage size
     * @return The size of the next block
     */
    public static int getBlockSize( final int blocksActive )
    {
        //16K blocks - 4K
        //32K blocks - 8K
        //and so on. Limit block size by 1M
        return Math.min(((blocksActive >> 14) + 1) << 12, MAX_BLOCK_SIZE);
    }

    /**
     * Get chain offset in the block
     * @param bucket Bucket
     * @return Chain offset
     */
    public static int getOffset( final long bucket )
    {
        return (int) (bucket & ( MAX_BLOCK_SIZE - 1 ) ); //20 bits at most, matches to MAX_BLOCK_SIZE
    }

    /**
     * Extract block index from a bucket
     * @param bucket Bucket
     * @return Block index
     */
    public static int getBlockIndex( final long bucket )
    {
        return (int) (bucket >> 32);
    }

    /**
     * Get block length by bucket
     * @param bucket Bucket
     * @return Block length, 0xFF in case when you should read it from the start of the bucket instead
     */
    public static int getBlockLength( final long bucket )
    {
        return (int) ( ( bucket >> MAX_BLOCK_SIZE_BITS ) & MAX_ENCODED_LENGTH );
    }

    /**
     * Pack all parts into a long bitmap
     * @param blockIdx Block index (full long)
     * @param offset Offset in a block (20 bits)
     * @param length Bucket length (zero forbidden, up to 254 values, 0xFF - read from bucket)
     * @return A packed long
     */
    public static long pack( final int blockIdx, final int offset, final int length )
    {
        return ( ( (long)blockIdx ) << 32 ) | offset | ( length << MAX_BLOCK_SIZE_BITS );
    }

}
