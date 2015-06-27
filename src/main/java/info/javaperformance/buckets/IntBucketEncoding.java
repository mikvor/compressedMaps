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

package info.javaperformance.buckets;

/**
 * This class contains logic for packing/unpacking bucket information into a {@code int} value.
 *  An {@code int} contains:
 *  bits  0 - 11: offset in a block (all blocks are 4K long in this mode)
 *  bits 12 - 14: chain length, can't be zero, 7 is used as an indicator that block length is encoded at the start of the bucket
 *  bits 15 - 31: chain id (up to 128K chains)
 *
 *  There is a forbidden encoding: 0 (you can never have it due to non zero length requirement)
 */
public class IntBucketEncoding {
    private static final int BLOCK_SIZE_BITS = 12;
    private static final int LENGTH_BITS = 3;
    private static final int BLOCK_ID_OFFSET = BLOCK_SIZE_BITS + LENGTH_BITS;
    public static final int MAX_BLOCK_SIZE = 1 << BLOCK_SIZE_BITS;
    /** Maximal length of a bucket you can have encoded in {@code int} */
    public static final int MAX_ENCODED_LENGTH = ( 1 << LENGTH_BITS ) - 1;
    /** Maximal supported block index */
    public static final int MAX_BLOCK_INDEX = ( 1 << ( 32 - BLOCK_SIZE_BITS - LENGTH_BITS ) ) - 1;

    /**
     * Empty cell, convenient value because of memory is set to zeroes by default and we have cheaper operations
     * if one arg is a constant zero
     */
    public static final int EMPTY = 0;

    /**
     * Get block size - always 4K in this mode
     *
     * @return The size of the next block
     */
    public static int getBlockSize()
    {
        return MAX_BLOCK_SIZE;
    }

    /**
     * Get chain offset in the block
     * @param bucket Bucket
     * @return Chain offset
     */
    public static int getOffset( final int bucket )
    {
        return bucket & ( MAX_BLOCK_SIZE - 1 );
    }

    /**
     * Extract block index from a bucket
     * @param bucket Bucket
     * @return Block index
     */
    public static int getBlockIndex( final int bucket )
    {
        return bucket >>> BLOCK_ID_OFFSET;
    }

    /**
     * Get block length by bucket
     * @param bucket Bucket
     * @return Block length, {@code MAX_ENCODED_LENGTH} in case when you should read it from the start of the bucket instead
     */
    public static int getBlockLength( final int bucket )
    {
        return ( bucket >> BLOCK_SIZE_BITS ) & MAX_ENCODED_LENGTH;
    }

    /**
     * Pack all parts into an int bitmap
     * @param blockIdx Block index (up to 128K)
     * @param offset Offset in a block (12 bits)
     * @param length Bucket length (zero forbidden, up to {@code MAX_ENCODED_LENGTH - 1} values, {@code MAX_ENCODED_LENGTH} - read from bucket)
     * @return A packed int
     */
    public static int pack( final int blockIdx, final int offset, final int length )
    {
        return ( blockIdx << BLOCK_ID_OFFSET ) | offset | ( length << BLOCK_SIZE_BITS );
    }

}
