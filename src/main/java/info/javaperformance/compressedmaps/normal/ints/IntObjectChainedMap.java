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

package info.javaperformance.compressedmaps.normal.ints;

import info.javaperformance.buckets.Buckets;
import info.javaperformance.malloc.SingleThreadedBlock;
import info.javaperformance.malloc.SingleThreadedBlockAllocator;
import info.javaperformance.serializers.*;
import info.javaperformance.tools.Primes;
import info.javaperformance.tools.Tools;

import java.util.Objects;

import static info.javaperformance.tools.VarLen.readUnsignedInt;
import static info.javaperformance.tools.VarLen.writeUnsignedInt;

/**
 * A simple single threaded compressed map. It uses {@code int[]} instead of {@code long[]} for buckets
 * before it allocates 128K blocks (or chain length exceeds 4K), so that the smaller maps can benefit from the even smaller map footprint.
 */
public class IntObjectChainedMap<V> implements IIntObjectMap<V>{
    private  final V NO_VALUE = null ;

    /*
    We store multiple reusable objects here. They are needed to avoid unnecessary object allocations.
     */
    private final Iterator<V> m_iter;
    private final ByteArray m_bar1 = new ByteArray();
    private final ByteArray m_bar2 = new ByteArray();
    private final UpdateResult<V> m_updateResult = new UpdateResult<>();
    private final Writer<V> m_writer;

    /** Key serializer */
    private final IIntSerializer m_keySerializer;
    /** Value serializer */
    private final IObjectSerializer<V> m_valueSerializer;
    /** Original fill factor */
    private final float m_fillFactor;
    /**
     * We check if we have to rehash only after any bucket length exceeds this value. This allows us to avoid
     * costly contended fields access.
     */
    private final int m_iFillFactor;
    /** Bucket table */
    private Buckets m_data;
    /**
     * Map size. We are not limited by int because of possibility to have fill factors greater than 1.
     */
    private long m_size;

    /**
     * Map size threshold - next rehashing happens after we exceed the threshold.
     * This field is {@code long} because we can use fill factors > 1.
     */
    private long m_threshold;

    /** Memory blocks are allocated and tracked here */
    private final SingleThreadedBlockAllocator m_blockAllocator;


    /**
     * Create a map with a given size, fill factor and key/value serializers
     * @param size Expected map size
     * @param fillFactor Map fill factor. Fill factors over 1.0 are supported and preferred for this map. This implementation
     *                   puts a soft limit of 16 for the fill factors. Such fill factors make buckets too long, which
     *                   increases the access/update costs, but the difference in the compression is getting smaller
     *                   and smaller.
     *                   Using fill factors below 1 is not prohibited, but you may end up with worse memory consumption
     *                   than {@code HashMap} can provide you.
     * @param keySerializer Serializer for keys
     * @param valueSerializer Serializer for values
     * @param blockCacheLimit The limit on the amount of memory blocks we try to reuse in order to reduce the GC load.
     *                        Increase it over the default (32K) if you want nearly no GC impact after the map size will stabilize.
     *
     * @throws NullPointerException If {@code keySerializer == null} or {@code valueSerializer == null}
     * @throws IllegalArgumentException If {@code fillFactor > 16} or {@code fillFactor <= 0.01} or {@code blockCacheLimit < 0}
     */
    public IntObjectChainedMap( final long size, final float fillFactor,
                               final IIntSerializer keySerializer, final IObjectSerializer<V> valueSerializer,
                               final long blockCacheLimit )
    {
        Objects.requireNonNull( keySerializer, "Key serializer must be provided!" );
        Objects.requireNonNull( valueSerializer, "Value serializer must be provided!" );
        if ( fillFactor > 16 )
            throw new IllegalArgumentException( "Fill factors higher than 16 are not supported!" );
        if ( fillFactor <= 0.01 )
            throw new IllegalArgumentException( "Fill factor must be greater than 0.01 and less or equal to 16!" );
        if ( blockCacheLimit < 0 )
            throw new IllegalArgumentException( "BlockCacheLimit can not be negative!" );

        m_keySerializer = keySerializer;
        m_valueSerializer = valueSerializer;
        m_blockAllocator = new SingleThreadedBlockAllocator( blockCacheLimit );
        m_fillFactor = fillFactor;
        m_iFillFactor = ( int ) Math.ceil( m_fillFactor );
        final long requestedCapacity = ( long ) Math.ceil( size / fillFactor );
        final int newCapacity = requestedCapacity >= Primes.getMaxIntPrime() ? Primes.getMaxIntPrime() : Primes.findNextPrime( requestedCapacity );
        //this threshold adjustment is needed on tiny initial size / large fill factor combinations so that we do not
        //rehash table without table size increase in future.
        m_data = new Buckets( newCapacity, false );
        //disable resizing if the initial size is already large enough
        if ( newCapacity == Primes.getMaxIntPrime() )
            m_threshold = Long.MAX_VALUE;
        else {
            long threshold = size;
            while ( Primes.findNextPrime( ( int ) Math.ceil( threshold * 2 / fillFactor ) ) == newCapacity )
                threshold *= 2;
            m_threshold = threshold;
        }
        //optimizations
        m_iter = new Iterator<>( m_keySerializer, m_valueSerializer );
        m_writer = new Writer<>( m_keySerializer, m_valueSerializer );
    }

    public V get( final int key )
    {
        if ( !m_data.select( getIndex( key, m_data.length() ) ) )
            return NO_VALUE;

        return m_iter.reset( getByteArray( getBlockByIndex( m_data.getBlockIndex() ), m_data.getOffset() ), m_data ).findKey( key, NO_VALUE );
    }

    public V put( final int key, final V value )
    {
        final int idx = getIndex( key, m_data.length() );
        //copy/update the chain
        final UpdateResult<V> res = addToChain( idx, key, value );
        final V ret = res.retValue; //must be saved in case of rehash
        changeSize( res.sizeChange );
        return ret;
    }

    /**
     * Write a single entry bucket
     * @param output Use this block for output (it has enough space)
     * @param key Key
     * @param value Value
     * @param idx Bucket to use
     */
    private void singleEntry( final SingleThreadedBlock output, final int key, final V value, final int idx )
    {
        final int startPos = output.pos;
        final ByteArray bar = getByteArray( output );
        output.increaseEntries(); //allocate block prior to writing
        m_writer.reset( bar ).writePair( key, value );
        output.pos = bar.position();
        m_data.set( idx, output.getIndex(), startPos, 1 );
    }

    private void singleEntryRehash( final SingleThreadedBlock output, final Iterator<V> inputIter, final int idx )
    {
        final int startPos = output.pos;
        final ByteArray bar = getByteArray( output );
        output.increaseEntries(); //allocate block prior to writing
        m_writer.reset( bar ).transferPair( inputIter );
        output.pos = bar.position();
        m_data.set( idx, output.getIndex(), startPos, 1 );
    }

    /**
     * Add key/value to a given chain. A chain is locked during the operation, so it can be safely updated.
     * The result is written to m_data[index]
     * @param index Bucket index
     * @param key Key
     * @param value Value
     * @return A new chain and an old value
     */
    private UpdateResult<V> addToChain( final int index, final int key, final V value )
    {
        if ( !m_data.select( index ) ) {
            singleEntry( getBlock( m_keySerializer.getMaxLength() + m_valueSerializer.getMaxLength( value ) + 1 ), key, value, index );
            return m_updateResult.set( NO_VALUE, 1 );
        }

        final SingleThreadedBlock inputBlock = getBlockByIndex( m_data.getBlockIndex() );
        final int inputStartOffset = m_data.getOffset();

        final ByteArray input = getByteArray( inputBlock, inputStartOffset );
        final Iterator<V> iter = m_iter.reset( input, m_data );
        if ( iter.getElems() > m_data.maxEncodedLength() - 2 ) //could grow to 255+, which should be stored in the bucket
            return addToChainSlow( index, iter, inputBlock, inputStartOffset, key, value );

        //calculate the chain length (it helps us to better fill data blocks)
        while ( iter.hasNext() )
            iter.skip();
        final int chainLength = input.position() - inputStartOffset;
        input.position( inputStartOffset );
        iter.reset( input, m_data );

        //2* is a safety net here due to possibility that a value may take longer in the delta form compared to original form
        final SingleThreadedBlock outputBlock = getBlock( chainLength +  2 * m_keySerializer.getMaxLength() +  m_valueSerializer.getMaxLength( value ) + 1 );
        final int startOutputPos = outputBlock.pos;
        final ByteArray baOutput = getByteArray2( outputBlock );

        inputBlock.decreaseEntries(); //release the input block, it may be held by this method for a little longer
        outputBlock.increaseEntries(); //allocate block
        final Writer<V> writer = m_writer.reset( baOutput );

        V retValue = NO_VALUE;
        boolean inserted = false, updated = false;

        while ( iter.hasNext() )
        {
            iter.advance( false );
            if ( iter.getKey() < key )
                writer.transferPair( iter );
            else if ( iter.getKey() == key )
            {
                inserted = true;
                updated = true;
                retValue = iter.readValue();
                writer.writePair( key, value );
            }
            else
            {
                if ( !inserted )
                {
                    inserted = true;
                    writer.writePair( key, value );
                }
                writer.transferPair( iter );
            }
        }
        if ( !inserted ) //all keys are smaller
            writer.writePair( key, value );

        outputBlock.pos = baOutput.position();
        final int sizeChange = updated ? 0 : 1;
        m_data.set( index, outputBlock.getIndex(), startOutputPos, iter.getElems() + sizeChange );
        return m_updateResult.set( retValue, sizeChange );
    }

    private UpdateResult<V> addToChainRehash( final int index, final Iterator<V> inputIter )
    {
        if ( !m_data.select( index ) ) {
            singleEntryRehash( getBlock( m_keySerializer.getMaxLength() + inputIter.getValueLength() + 1 ), inputIter, index );
            return m_updateResult.set( NO_VALUE, 1 );
        }

        final SingleThreadedBlock inputBlock = getBlockByIndex( m_data.getBlockIndex() );
        final int inputStartOffset = m_data.getOffset();

        final ByteArray input = getByteArray( inputBlock, inputStartOffset );
        final Iterator<V> iter = m_iter.reset( input, m_data );
        if ( iter.getElems() > m_data.maxEncodedLength() - 2 ) //could grow to 255+, which should be stored in the bucket
            return addToChainSlow( index, iter, inputBlock, inputStartOffset, inputIter.getKey(), inputIter.readValue() ); //read a value for less common cases

        //calculate the chain length (it helps us to better fill data blocks)
        while ( iter.hasNext() )
            iter.skip();
        final int chainLength = input.position() - inputStartOffset;
        input.position( inputStartOffset );
        iter.reset( input, m_data );

        //2* is a safety net here due to possibility that a value may take longer in the delta form compared to original form
        final SingleThreadedBlock outputBlock = getBlock( chainLength +  2 * m_keySerializer.getMaxLength() + inputIter.getValueLength() + 1 );
        final int startOutputPos = outputBlock.pos;
        final ByteArray baOutput = getByteArray2( outputBlock );

        inputBlock.decreaseEntries(); //release the input block, it may be held by this method for a little longer
        outputBlock.increaseEntries(); //allocate block
        final Writer<V> writer = m_writer.reset( baOutput );

        V retValue = NO_VALUE;
        boolean inserted = false, updated = false;

        while ( iter.hasNext() )
        {
            iter.advance( false );
            if ( iter.getKey() < inputIter.getKey() )
                writer.transferPair( iter );
            else if ( iter.getKey() == inputIter.getKey() )
            {
                inserted = true;
                updated = true;
                retValue = iter.readValue();
                writer.transferPair( inputIter );
            }
            else
            {
                if ( !inserted )
                {
                    inserted = true;
                    writer.transferPair( inputIter );
                }
                writer.transferPair( iter );
            }
        }
        if ( !inserted ) //all keys are smaller
            writer.transferPair( inputIter );

        outputBlock.pos = baOutput.position();
        final int sizeChange = updated ? 0 : 1;
        m_data.set( index, outputBlock.getIndex(), startOutputPos, iter.getElems() + sizeChange );
        return m_updateResult.set( retValue, sizeChange );
    }

    /**
     * This is a special version of previous method which deals with chains which require storing the chain length
     * prior to the chain ( compared to being encoded in the buckets ).
     * @param index Key bucket
     * @param iter Input iterator
     * @param inputBlock Input block
     * @param inputStartOffset Start offset for the iterator (prior to len)
     * @param key Key
     * @param value Value
     * @return A new chain and an old value
     */
    private UpdateResult<V> addToChainSlow( final int index, final Iterator<V> iter,
                                         final SingleThreadedBlock inputBlock,
                                         final int inputStartOffset, final int key, final V value )
    {
        boolean hasKey = false;
        V retValue = NO_VALUE;
        while ( iter.hasNext() ) //look up a key and then fast forward to the end of chain to find out its length
        {
            iter.advance( false );
            if ( iter.getKey() == key )
            {
                hasKey = true;
                retValue = iter.readValue();
                while ( iter.hasNext() )
                    iter.skip();
            }
            else if ( iter.getKey() > key )
            {
                iter.skipValue();
                while ( iter.hasNext() )
                    iter.skip();
            }
            else
                iter.skipValue();
        }

        final int chainLength = iter.getBuf().position() - inputStartOffset;
        final int elems = hasKey ? iter.getElems() : iter.getElems() + 1;
        final SingleThreadedBlock outputBlock = getBlock( chainLength +  2 * m_keySerializer.getMaxLength() +  m_valueSerializer.getMaxLength( value ) + 2 ); //2 for transition from header to chain length
        final int startOutputPos = outputBlock.pos;
        final ByteArray output = getByteArray2( outputBlock );

        inputBlock.decreaseEntries();
        outputBlock.increaseEntries(); //allocate block
        //here we write the correct number of elements upfront
        final Writer<V> writer = m_writer.reset( output, elems );
        boolean inserted = false;

        //fully reset the iterator (position on the bucket length)
        iter.getBuf().position( inputStartOffset );
        iter.reset( iter.getBuf(), m_data );

        while ( iter.hasNext() )
        {
            iter.advance( false );
            if ( iter.getKey() < key )
                writer.transferPair( iter );
            else if ( iter.getKey() == key )
            {
                inserted = true;
                retValue = iter.readValue();
                writer.writePair( key, value );
            }
            else
            {
                if ( !inserted )
                {
                    inserted = true;
                    writer.writePair( key, value );
                }
                writer.transferPair( iter );
            }
        }
        if ( !inserted ) //all keys are smaller
            writer.writePair( key, value );

        outputBlock.pos = output.position();
        m_data.set( index, outputBlock.getIndex(), startOutputPos, m_data.maxEncodedLength() );
        return m_updateResult.set( retValue, hasKey ? 0 : 1 );
    }

    public V remove( final int key )
    {
        final int idx = getIndex( key, m_data.length() );
        if ( !m_data.select( idx ) )
            return NO_VALUE;

        final UpdateResult<V> res = removeKey( key, idx );
        m_size += res.sizeChange;
        return res.retValue;
    }

    /**
     * Remove a given key from a chain.
     * 3 special cases are supported:
     * 1) key not found - same chain is returned
     * 2) removal from a 1-entry long chain - null chain is returned with a valid retValue
     * 3) removal of the last element in the chain - in most cases only the chain length should be updated
     * @param key Key to remove
     * @param idx Key bucket
     * @return Updated or original chain
     */
    private UpdateResult<V> removeKey( final int key, final int idx )
    {
        final SingleThreadedBlock inputBlock = getBlockByIndex( m_data.getBlockIndex() );
        final int inputStartOffset = m_data.getOffset();

        final ByteArray input = getByteArray( inputBlock, inputStartOffset );
        final Iterator<V> iter = m_iter.reset( input, m_data );

        boolean hasKey = false;
        V retValue = NO_VALUE;
        while ( iter.hasNext() )
        {
            iter.advance( false );
            if ( iter.getKey() == key )
            {
                hasKey = true;
                retValue = iter.readValue();
                break;
            }
            else if ( iter.getKey() > key )
                break;
            else
                iter.skipValue();
        }
        if ( !hasKey )
            return m_updateResult.set( NO_VALUE, 0 );

        //special case 1 - chain removal
        if ( iter.getElems() == 1 ) {
            inputBlock.decreaseEntries();
            m_data.set( idx, m_data.emptyBucket() );
            return m_updateResult.set( retValue, -1 );
        }

        //special case 2 - removal of the last element and length is not too big - only bucket info changes
        //we can not check for equality - once we have reached maxlen there will be a length field at the start of the record
        if ( !iter.hasNext() && iter.getElems() < m_data.maxEncodedLength() )
        {
            m_data.set( idx, inputBlock.getIndex(), inputStartOffset, iter.getElems() - 1 );
            return m_updateResult.set( retValue, -1 );
        }

        //Reuse the same block for removed chain. There are some risks that it will keep more blocks active, but on
        //the other hand it will definitely reduce the number of discarded blocks => reduce GC.

        input.position( inputStartOffset );
        iter.reset( input, m_data );

        final ByteArray output = getByteArray2( inputBlock );
        output.position( inputStartOffset );
        final Writer<V> writer = m_writer.reset( output, iter.getElems() <= m_data.maxEncodedLength() ? 0 : iter.getElems() - 1 );
        while ( iter.hasNext() )
        {
            iter.advance( false );
            if ( iter.getKey() != key )
                writer.transferPair( iter );
            else
                iter.skipValue();
        }

        m_data.set( idx, inputBlock.getIndex(), inputStartOffset,
                                iter.getElems() <= m_data.maxEncodedLength() ? iter.getElems() - 1 : m_data.maxEncodedLength() );
        return m_updateResult.set( retValue, -1 );
    }

    /**
     * Get map size. Note that this method does not lock the map to calculate the result, so it can return slightly incorrect
     * values (including negative ones).
     * Calling this method to frequently may cause the performance degradation of your code.
     * @return The approximate current map size (temporarily may be negative)
     */
    public long size() {
        return m_size;
    }

    /**
     * Rehash the table.
     * @param old Old bucket table
     */
    private void rehash( final Buckets old )
    {
        final ByteArray barLocal = new ByteArray();
        final Iterator<V> iterLocal = new Iterator<>( m_keySerializer, m_valueSerializer );

        for ( int i = 0; i < old.length(); ++i )
            if ( old.select( i ) )
                rehashInnerStep( old, barLocal, iterLocal );
    }

    private void rehashInnerStep( final Buckets old, final ByteArray bar, final Iterator<V> iter )
    {
        final SingleThreadedBlock inputBlock = getBlockByIndex( old.getBlockIndex() );

        iter.reset( bar.reset( inputBlock.data, old.getOffset() ), old );
        if ( old.getBlockLength() == 1 ) //shortcut, no data copy for blocklen = 1
        {
            iter.advance( false );
            final int index = getIndex( iter.getKey(), m_data.length() );
            if ( !m_data.select( index ) )
                m_data.set( index, old.getBucket() );
            else
            {
                //copy/update the chain
                addToChainRehash( index, iter );
                inputBlock.decreaseEntries();
            }
        }
        else
        {
            while ( iter.hasNext() )
            {
                iter.advance( false );
                //copy/update the chain
                addToChainRehash( getIndex( iter.getKey(), m_data.length() ), iter );
            }
            inputBlock.decreaseEntries(); //bucket relocated
        }
    }

    /**
     * Get the bucket index for the given key
     * @param key A key
     * @param tabSize Bucket table size
     * @return Bucket index
     */
    private int getIndex( final int key, final int tabSize )
    {
        return Tools.getIndexFast( key, tabSize );
    }

    private static class Iterator<V>    {
        /** Underlying byte buffer */
        private ByteArray buf;
        /** Number of entries in the bucket */
        private int elems;
        /** Index of the current entry (0-based) */
        private int cur = 0;
        /** Current entry key, initialized by {@code advance} call */
        private int key;
        /** Current entry value, initialized by {@code advance} call */
        private V value;
        /** Serialization for keys */
        private final IIntSerializer m_keySerializer;
        /** Serialization for values */
        private final IObjectSerializer<V> m_valueSerializer;

        public Iterator( final IIntSerializer keySerializer, final IObjectSerializer<V> valueSerializer ) {
            m_keySerializer = keySerializer;
            m_valueSerializer = valueSerializer;
        }

        /**
         * Initialize an iterator by a buffer. This method will reads the number of entries if the current bucket length = max length
         * @param buf Byte buffer
         * @param data A reference to a current Buckets object
         * @return Same iterator object
         */
        Iterator<V> reset( final ByteArray buf, final Buckets data )
        {
            this.buf = buf;
            elems = data.getBlockLength() < data.maxEncodedLength() ? data.getBlockLength() : readUnsignedInt( buf );
            cur = 0;
            return this;
        }

        /**
         * Check if there are any not read entries left in the bucket
         * @return True if we can advance, false otherwise
         */
        public boolean hasNext()
        {
            return cur < elems;
        }

        /**
         * Read the next entry from the buffer
         */
        public void advance()
        {
            advance( true );
        }

        private void advance( final boolean readValue )
        {
            if ( cur == 0 )
                key = m_keySerializer.read( buf );
            else
                key = m_keySerializer.readDelta( key, buf, true );
            if ( readValue )
                readValue();
            ++cur;
        }

        /**
        * Memory allocation efficient method for looking up a value for a given key.
        * @param key Key to look up
        * @param noValue Value to return in case of failure
        * @return Found value or {@code noValue}
        */
        public V findKey( final int key, final V noValue )
        {
            while ( hasNext() ) {
                advance( false );
                if ( getKey() == key )
                    return readValue();
                else if ( getKey() > key ) //keys are sorted
                    return noValue;
                else
                    skipValue();
            }
            return noValue;
        }

        public void skipValue()
        {
            m_valueSerializer.skip( buf );
        }

        public V readValue()
        {
            return ( value = m_valueSerializer.read( buf ) );
        }

        /**
        * Skip the current entry
        */
        public void skip()
        {
            m_keySerializer.skip( buf );
            skipValue();
            ++cur;
        }

        /**
        * Get value length if the iterator is currently standing prior to the value (using advance(false) ).
        * This method does not change the input ByteArray / iterator state.
        * @return The length of value binary representation
        */
        public int getValueLength()
        {
            final int startPos = buf.position();
            skipValue();
            final int res = buf.position() - startPos;
            buf.position( startPos );
            return res;
        }

        /**
         * @return A key read by the last {@code advance} call
         */
        public int getKey() {
            return key;
        }

        /**
         * @return A value read by the last {@code advance} call
         */
        public V getValue() {
            return value;
        }

        /**
         * @return Number of entries in the bucket
         */
        public int getElems() {
            return elems;
        }

        public ByteArray getBuf()
        {
            return buf;
        }
    }

    /**
     * This class encapsulates the logic used to write all entries into the bucket.
     */
    private static final class Writer<V>    {
        /** Underlying byte buffer */
        private ByteArray buf;
        /** Is this a first entry (used for delta encoding) */
        private boolean first = true;
        /** Previously written key (used for delta encoding) */
        private int prevKey;
        /** Serialization for keys */
        private final IIntSerializer m_keySerializer;
        /** Serialization for values */
        private final IObjectSerializer<V> m_valueSerializer;

        public Writer( final IIntSerializer keySerializer, final IObjectSerializer<V> valueSerializer)
        {
            m_keySerializer = keySerializer;
            m_valueSerializer = valueSerializer;
        }


        /**
         * Reset a writer (useful if you need to write multiple entries in one method call).
         * This method does not write the element count into the bucket (caller should take care of it)
         * @param buf Underlying byte buffer
         * @return this
         */
        public Writer<V> reset( final ByteArray buf )
        {
            return reset( buf, 0 );
        }

        /**
         * Reset a writer (useful if you need to write multiple entries in one method call)
         * @param buf Underlying byte buffer
         * @param elems Number of elements to write, don't write anything if this value is not positive
         * @return this
         */
        public Writer<V> reset( final ByteArray buf, final int elems )
        {
            this.buf = buf;
            if ( elems > 0 )
                writeUnsignedInt( elems, buf );
            first = true;
            prevKey = 0;
            return this;
        }

        /**
         * Write a key-value pair
         * @param k Key to write
         * @param v Value to write
         */
        public void writePair( final int k, final V v )
        {
            if ( first ) {
                m_keySerializer.write( k, buf );
                first = false;
            }
            else
            {
                //keys are sorted, so we can write unsigned diff (but serializer will make a final decision)
                m_keySerializer.writeDelta( prevKey, k, buf, true );
            }
            m_valueSerializer.write( v, buf );
            prevKey = k;
        }


        /**
        * Optimization - copy the value binary representation ( do not try to (de)serialize it ).
        * We always take the iterator key as a key.
        * @param iter Iterator standing prior to a value
        */
        public void transferPair( final Iterator<V> iter )
        {
            if ( first ) {
                m_keySerializer.write( iter.getKey(), buf );
                first = false;
            }
            else
            {
                //keys are sorted, so we can write unsigned diff (but serializer will make a final decision)
                m_keySerializer.writeDelta( prevKey, iter.getKey(), buf, true );
            }
            prevKey = iter.getKey();
            //copy binary representation of a value
            final ByteArray iterBuf = iter.getBuf();
            final int startPos = iterBuf.position();
            iter.skipValue();
            buf.put( iterBuf.array(), startPos, iterBuf.position() - startPos );
        }

    }

    private ByteArray getByteArray( final SingleThreadedBlock ar )
    {
        return m_bar1.reset( ar.data, ar.pos );
    }

    private ByteArray getByteArray2( final SingleThreadedBlock ar )
    {
        return m_bar2.reset( ar.data, ar.pos );
    }

    private ByteArray getByteArray( final SingleThreadedBlock ar, final int offset )
    {
        return m_bar1.reset( ar.data, offset );
    }

    private SingleThreadedBlock getBlock( final int bytes )
    {
        return m_blockAllocator.getBlock( bytes, m_data );
    }

    private SingleThreadedBlock getBlockByIndex( final int index )
    {
        return m_blockAllocator.getBlockByIndex( index );
    }

    /**
     * All updating methods generate this structure containing a new chain information
     */
    private static class UpdateResult<V>    {
        public V retValue;
        public int sizeChange;

        public UpdateResult() {
        }

        public UpdateResult<V> set( V retValue, int sizeChange )
        {
            this.retValue = retValue;
            this.sizeChange = sizeChange;
            return this;
        }

        @Override
        public String toString() {
            return "UpdateResult{" +
                    "retValue=" + retValue +
                    ", sizeChange=" + sizeChange +
                    '}';
        }
    }

    private void changeSize( final int delta )
    {
        m_size += delta;

        if ( m_size > m_threshold )
        {
            final long multiplier = m_fillFactor <= 2 ? 2 : m_iFillFactor;
            int newCapacity = Primes.findNextPrime( ( long ) Math.ceil( multiplier * m_threshold / m_fillFactor ) );
            //check if we got too close to the max array size, otherwise we will have to make a pretty useless resize when got close to 2G*fillFactor entries
            if ( newCapacity * 1.5 > Primes.getMaxIntPrime() )
                newCapacity = Primes.getMaxIntPrime();
            //this check disables rehashing after a table has reached the maximal size
            final long newThreshold = newCapacity >= Primes.getMaxIntPrime() ? Long.MAX_VALUE : m_threshold * multiplier;

            final Buckets old = m_data;
            try {
                m_data = new Buckets( newCapacity, old.isLong() );
            }
            catch ( OutOfMemoryError ex )
            {
                //let's disable rehashing and keep on working
                m_threshold = Long.MAX_VALUE;
                return;
            }
            m_threshold = newThreshold;
            rehash( old );
        }
    }

    /////////////////////////////////////////////////////////////////////
    //   Some debugging
    /////////////////////////////////////////////////////////////////////

    public void printStats()
    {
        final int[] cnt = new int[ 1000 ];
        long sum = 0;
        for ( int i = 0; i < m_data.length(); ++i )
        {
            if ( m_data.select( i ) )
            {
                final int len = m_data.getBlockLength();
                cnt[ len ]++;
                sum += len;
            }
        }

        System.out.println( "Capacity = " + m_data.length() );
        System.out.println( "Size = " + sum );

        for ( int i = 0; i < cnt.length; ++i )
            if ( cnt[ i ] > 0 )
                System.out.println( "Length " + i + " = " + cnt[ i ] );
    }
}
