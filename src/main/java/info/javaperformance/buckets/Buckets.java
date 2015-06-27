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
 * An abstraction for bucket storage for the single threaded maps.
 */
public class Buckets
{
    private IBuckets m_data;

    public Buckets( final int size, final boolean isLong )
    {
        m_data = isLong ? new LongBucket( size ) : new IntBucket( size );
    }

    public boolean isLong()
    {
        return m_data.isLong();
    }

    /**
     * Set a packed bucket. You can only use this method for setting a packed value
     * previously obtained from another bucket.
     * @param index Index
     * @param value Packed bucket value
     */
    public void set( final int index, final long value )
    {
        m_data.set( index, value );
    }

    public void set( final int index, final int blockIdx, final int offset, final int length )
    {
        m_data.set( index, blockIdx, offset, length );
    }

    public long get( final int index )
    {
        return m_data.get( index );
    }

    public boolean select( final int index )
    {
        return m_data.select( index );
    }

    public int length()
    {
        return m_data.length();
    }

    public int getOffset()
    {
        return m_data.getOffset();
    }

    public int getBlockIndex()
    {
        return m_data.getBlockIndex();
    }

    public int getBlockLength()
    {
        return m_data.getBlockLength();
    }

    public long getBucket()
    {
        return m_data.getBucket();
    }

    public long emptyBucket()
    {
        return 0; //always zero
    }

    public int maxEncodedLength()
    {
        return m_data.maxEncodedLength();
    }

    /**
     * Calculate the allocated block size based on the allocator capacity
     * @param allocatorMapSize Allocator size
     * @return A block size for the newly allocated blocks
     */
    public int getBlockSize( final int allocatorMapSize )
    {
        return m_data.getBlockSize( allocatorMapSize );
    }

    ////////////////////////////////////////////////////////////
    //  Actual implementations
    ////////////////////////////////////////////////////////////

    private interface IBuckets
    {
        /**
         * @return true if {@code long[]} is used, false for {@code int[]}
         */
        public boolean isLong();

        /**
         * Set a bucket at a given index
         * @param index Index
         * @param value Bucket value (already packed)
         */
        public void set( final int index, final long value );

        /**
         * Set a bucket from the components
         * @param index Index
         * @param blockIdx Block index
         * @param offset Offset in a block
         * @param length Number of pairs in a bucket
         */
        public void set( final int index, final int blockIdx, final int offset, final int length );

        /**
         * Obtain a packed bucket
         * @param index Index
         * @return A bucket
         */
        public long get( final int index );

        /**
         * Select a bucket at a given index as an active one
         * @param index Index
         * @return True for non-empty buckets, false for empty ones
         */
        public boolean select( final int index );

        /**
         * @return Table size
         */
        public int length();

        /**
         * @return Offset from previously {@code select}-ed bucket
         */
        public int getOffset();

        /**
         * @return Block index from previously {@code select}-ed bucket
         */
        public int getBlockIndex();

        /**
         * @return Number of pairs in a bucket from previously {@code select}-ed bucket
         */
        public int getBlockLength();

        /**
         * @return Previously {@code select}-ed bucket in a packed form
         */
        public long getBucket();

        /**
         * @return Currently supported max bucket length which could be stored in a bucket
         */
        public int maxEncodedLength();

        /**
         * Calculate the allocated block size based on the allocator capacity
         * @param allocatorMapSize Allocator size
         * @return A block size for the newly allocated blocks
         */
        public int getBlockSize( final int allocatorMapSize );
    }

    private class IntBucket implements IBuckets
    {
        private final int[] m_data;
        private int m_bucket;

        public IntBucket( final int size )
        {
            m_data = new int[ size ];
        }

        @Override
        public boolean isLong() {
            return false;
        }

        @Override
        public void set( final int index, final long value ) {
            m_data[ index ] = ( int ) value;
        }

        @Override
        public void set( final int index, final int blockIdx, final int offset, final int length ) {
            if ( blockIdx > IntBucketEncoding.MAX_BLOCK_INDEX || offset >= IntBucketEncoding.MAX_BLOCK_SIZE )
            {
                /*
                 Migrate to a long map. We need the same size map, but all values must be repacked.
                 Special care must be paid to length. We have limit=7 for int, but it is 255 for long.
                 Anything with length=7 must be converted into length=255 so that we should still extract
                 the length from the bucket after migration.
                */
                final LongBucket longBucket = new LongBucket( m_data.length );
                for ( int i = 0; i < m_data.length; ++i )
                    if ( select( i ) )
                    {
                        final int blockLength = getBlockLength();
                        longBucket.set( i, getBlockIndex(), getOffset(),
                                blockLength != maxEncodedLength() ? blockLength : longBucket.maxEncodedLength() );
                    }

                //set the current field
                longBucket.set( index, blockIdx, offset,
                        length < IntBucketEncoding.MAX_ENCODED_LENGTH ? length : LongBucketEncoding.MAX_ENCODED_LENGTH );
                //and get rid of these buckets
                Buckets.this.m_data = longBucket;
            }
            else
                m_data[ index ] = IntBucketEncoding.pack( blockIdx, offset, length );
        }

        @Override
        public long get( final int index ) {
            return m_data[ index ];
        }

        @Override
        public boolean select( final int index ) {
            m_bucket = m_data[ index ];
            return m_bucket != IntBucketEncoding.EMPTY;
        }

        @Override
        public int length() {
            return m_data.length;
        }

        @Override
        public int getOffset() {
            return IntBucketEncoding.getOffset( m_bucket );
        }

        @Override
        public int getBlockIndex() {
            return IntBucketEncoding.getBlockIndex( m_bucket );
        }

        @Override
        public int getBlockLength() {
            return IntBucketEncoding.getBlockLength( m_bucket );
        }

        @Override
        public long getBucket() {
            return m_bucket;
        }

        @Override
        public int maxEncodedLength() {
            return IntBucketEncoding.MAX_ENCODED_LENGTH;
        }

        @Override
        public int getBlockSize( final int allocatorMapSize ) {
            return IntBucketEncoding.getBlockSize();
        }
    }

    private class LongBucket implements IBuckets
    {
        private final long[] m_data;
        private long m_bucket;

        public LongBucket( final int size )
        {
            m_data = new long[ size ];
        }

        @Override
        public boolean isLong() {
            return true;
        }

        @Override
        public void set( final int index, final long value ) {
            m_data[ index ] = value;
        }

        @Override
        public void set( final int index, final int blockIdx, final int offset, final int length ) {
            m_data[ index ] = LongBucketEncoding.pack( blockIdx, offset, length );
        }

        @Override
        public long get( final int index ) {
            return m_data[ index ];
        }

        @Override
        public boolean select( final int index ) {
            m_bucket = m_data[ index ];
            return m_bucket != LongBucketEncoding.EMPTY;
        }

        @Override
        public int length() {
            return m_data.length;
        }

        @Override
        public int getOffset() {
            return LongBucketEncoding.getOffset( m_bucket );
        }

        @Override
        public int getBlockIndex() {
            return LongBucketEncoding.getBlockIndex( m_bucket );
        }

        @Override
        public int getBlockLength() {
            return LongBucketEncoding.getBlockLength( m_bucket );
        }

        @Override
        public long getBucket() {
            return m_bucket;
        }

        @Override
        public int maxEncodedLength() {
            return LongBucketEncoding.MAX_ENCODED_LENGTH;
        }

        @Override
        public int getBlockSize( final int allocatorMapSize ) {
            return LongBucketEncoding.getBlockSize( allocatorMapSize );
        }
    }

}
