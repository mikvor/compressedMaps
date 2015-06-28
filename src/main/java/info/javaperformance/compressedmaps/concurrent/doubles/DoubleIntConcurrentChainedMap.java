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

package info.javaperformance.compressedmaps.concurrent.doubles;

import info.javaperformance.malloc.Block;
import info.javaperformance.malloc.ConcurrentBlockAllocator;
import info.javaperformance.serializers.*;
import info.javaperformance.tools.Buffers;
import info.javaperformance.tools.LongAllocator;
import info.javaperformance.tools.Primes;
import info.javaperformance.tools.Tools;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static info.javaperformance.buckets.LongBucketEncoding.*;
import static info.javaperformance.tools.VarLen.readUnsignedInt;
import static info.javaperformance.tools.VarLen.writeUnsignedInt;

/*
todo
1) check what's retained in the helper objects in ThreadLocals
2) check if we properly clean the blocks
3) BlockMap
 */

/**
 * A primitive concurrent hash map.
 * It is built on top of a collection of memory blocks managed by {@code ConcurrentBlockAllocator}. We use same serializers
 * as for non-concurrent collections here.
 *
 * This map does not use any locks, but it may spin using {@code LockSupport.parkNanos} method in a few cases.
 *
 * {@code get} operation is implemented similarly to JDK {@code ConcurrentHashMap} - it returns the value associated
 * with a requested key at the moment of method call (we get the bucket at the moment of method call and extract a value
 * out of it, while it can be updated in the mean time by another thread.
 *
 * Both {@code put} and {@code remove} use CAS loops for updates, so they always return the latest value
 * (latest at the moment of successful update).
 *
 * Rehashing is done concurrently in this map.
 * All 3 main operations ({@code get/put/remove}) join rehashing once they detect it is going on. No thread can update
 * the map state once rehashing has started.
 */
public class DoubleIntConcurrentChainedMap implements IDoubleIntConcurrentMap
{
    private static final int CPU_NUMBER = Runtime.getRuntime().availableProcessors();
    private static final int NO_VALUE = 0;

    /*
    We store multiple reusable objects here. They are needed to avoid unnecessary object allocations.
    These objects should not be static - some of the are initialized with map specific serializers, some may simply
    keep some map state for longer than needed.
     */
    private final ThreadLocal<Iterator> s_iters = new ThreadLocal<>();
    private final ThreadLocal<ByteArray> s_bar1 = new ThreadLocal<>();
    private final ThreadLocal<ByteArray> s_bar2 = new ThreadLocal<>();
    private final ThreadLocal<Writer> s_writers = new ThreadLocal<>();
    private final ThreadLocal<UpdateResult> s_updateRes = new ThreadLocal<UpdateResult>(){
        @Override
        protected UpdateResult initialValue() {
            return new UpdateResult();
        }
    };

    /** Key serializer */
    private final IDoubleSerializer m_keySerializer;
    /** Value serializer */
    private final IIntSerializer m_valueSerializer;
    /** Original fill factor */
    private final float m_fillFactor;
    /**
     * We check if we have to rehash only after any bucket length exceeds this value. This allows us to avoid
     * costly contended fields access.
     */
    private final int m_iFillFactor;
    /**
     * Most of variable map state is stored here
     */
    private final AtomicReference<Buffers> m_data;

    /** Memory blocks are allocated and tracked here */
    private final ConcurrentBlockAllocator m_blockAllocator = new ConcurrentBlockAllocator();
    /** This object helps us not to allocate extra long[] in {@code changeSize} */
    private final LongAllocator m_longAlloc = new LongAllocator();

    /** Max length of a single entry - optimization */
    private final int m_singleEntryLength;

    /**
     * Create a map with a given size, fill factor and key/value serializers
     * @param size Expected map size
     * @param fillFactor Map fill factor. Fill factors over 1.0 are supported and preferred for this map. This implementation
     *                   puts a soft limit of 16 for the fill factors. Such fill factors make buckets too long, which
     *                   increases the access/update costs, but the difference in the compression is getting smaller
     *                   and smaller.
     *                   Using fill factors below 1 is not prohibited, but you may end up with worse memory consumption
     *                   than {@code ConcurrentHashMap} can provide you.
     * @param keySerializer Serializer for keys
     * @param valueSerializer Serializer for values
     *
     * @throws NullPointerException If {@code keySerializer == null} or {@code valueSerializer == null}
     * @throws IllegalArgumentException If {@code fillFactor > 16} or {@code fillFactor <= 0.01}
     */
    public DoubleIntConcurrentChainedMap( final long size, final float fillFactor,
                                       final IDoubleSerializer keySerializer, final IIntSerializer valueSerializer )
    {
        Objects.requireNonNull( keySerializer, "Key serializer must be provided!" );
        Objects.requireNonNull( valueSerializer, "Value serializer must be provided!" );
        if ( fillFactor > 16 )
            throw new IllegalArgumentException( "Fill factors higher than 16 are not supported!" );
        if ( fillFactor <= 0.01 )
            throw new IllegalArgumentException( "Fill factor must be greater than 0.01 and less or equal to 16!" );

        m_keySerializer = keySerializer;
        m_valueSerializer = valueSerializer;
        m_fillFactor = fillFactor;
        m_iFillFactor = ( int ) Math.ceil( m_fillFactor );
        final long requestedCapacity = ( long ) Math.ceil( size / fillFactor );
        final int newCapacity = requestedCapacity >= Primes.getMaxIntPrime() ? Primes.getMaxIntPrime() : Primes.findNextPrime( requestedCapacity );
        //this threshold adjustment is needed on tiny initial size / large fill factor combinations so that we do not
        //rehash table without table size increase in future.
        long threshold;
        if ( newCapacity == Primes.getMaxIntPrime() )
            threshold = Long.MAX_VALUE;
        else {
            threshold = size;
            while ( Primes.findNextPrime( ( int ) Math.ceil( threshold * 2 / fillFactor ) ) == newCapacity )
                threshold *= 2;
        }
        m_data = new AtomicReference<>( new Buffers( m_longAlloc.allocate( newCapacity ), null, threshold, 0, 2 ) );
        m_singleEntryLength = m_keySerializer.getMaxLength() + m_valueSerializer.getMaxLength() + 1; //optimization
    }

    /**
     * Get block by bucket
     * @param bucket Bucket
     * @return Block
     */
    private Block getBlockByIndex( final long bucket )
    {
        return m_blockAllocator.getBlockByIndex( getBlockIndex( bucket ) );
    }

    @Override
    public int get( final double key )
    {
        final Buffers buffers = m_data.get();
        if ( buffers.old != null )
            rehash( buffers.nextStableVersion ); //buffers.cur will be valid on the exit from rehashing

        final long[] tab = buffers.cur;
        final int idx = getIndex( key, tab.length );

        final long bucket = getBucket( tab, idx );
        if ( bucket == EMPTY ) {
            //By definition of this method we return whatever value was valid at a call time
            //We can get here either at a stable stage (old==null) or after rehash, in both of which null bucket means no key.
            return NO_VALUE;
        }
        //we have definitely relocated something from this chain.
        else if ( bucket == RELOCATED ) {
            rehash( buffers.nextStableVersion );
            return get( key );
        }

        final Block input = getBlockByIndex(bucket);
        if ( input == null )
            return get( key ); //someone has relocated the data from this block, need to retry because we have a valid bucket

        final Iterator iter = getIterator().reset( getByteArray( input, getOffset( bucket ) ), getBlockLength( bucket ) );
        while ( iter.hasNext() ) {
            iter.advance();
            if ( iter.getKey() == key )
                return iter.value;
            else if ( iter.getKey() > key ) //keys are sorted
                return NO_VALUE;
        }
        return NO_VALUE;
    }

    @Override
    public int put( final double key, final int value )
    {
        Buffers buffers = m_data.get();
        if ( buffers.old != null ) {
            rehash( buffers.nextStableVersion ); //help rehashing, we are in rehashing state already
            buffers = m_data.get(); //it changes after rehashing
        }

        final int idx = getIndex( key, buffers.cur.length );
        long bucket = getBucket( buffers.cur, idx );
        //if bucket already relocated, do not waste time, rehash and then retry
        if ( bucket == RELOCATED )
        {
            rehash( buffers.nextStableVersion ); //just turned into rehashing
            return put( key, value );
        }

        //CAS in a loop
        while ( true )
        {
            //copy/update the chain
            final UpdateResult res = addToChain( bucket, key, value );
            /*
             Thread safety here: if we got to this point, we know that 'buffers' belong to stable state.
             It means it is either safe to set bucket here (stable state) or buffers.cur point to the
             old buffer (rehashing or subsequent states).
             During rehashing we atomically replace each cell with RELOCATED sentinel, which means 2 possibilities:
             1) we replace current cell before it is picked up by rehashing thread (size could be safely increased here)
             2) we try to CAS-replace a cell, but fail, because it was already replaced with RELOCATED (we go to 'else').
             */
            if ( res != null && compareAndSet( buffers.cur, idx, bucket, res.chain ) )
            {
                //commit usage changes to input block (output already updated)
                if (res.input != null) //could be null for new bucket
                    res.input.decreaseEntries();

                final int ret = res.retValue; //must be saved in case of rehash
                changeSize(res.sizeChange, buffers, getBlockLength( res.chain ) );
                return ret;
            }
            else {
                //rollback output block usage. It is managed by a current thread, so it is safe
                if ( res != null ) {
                    res.output.pos = res.outputPrevStart;
                    res.output.decreaseEntries();
                }

                //cell could be changed to not empty / null (empty) in the normal run
                //or to RELOCATED during rehashing by other thread
                bucket = getBucket( buffers.cur, idx );
                if ( bucket == RELOCATED )
                {
                    rehash( buffers.nextStableVersion );
                    return put( key, value );
                }
            }
        }
    }

    /*
    A version of {@code put}, which is called from rehashing only. It does not need to detect rehashing in progress.
     */
    private void doPutRehash( final long[] tab, final double key, final int value )
    {
        final int idx = getIndex( key, tab.length );
        long bucket = getBucket( tab, idx );
        //We need to copy existing chain into a new buffer, add/update an entry.
        //CAS in a loop
        while ( true )
        {
            //copy/update the chain
            final UpdateResult res = addToChain( bucket, key, value );
            if ( res != null && compareAndSet( tab, idx, bucket, res.chain ) ) {
                //commit usage changes to input block (output already updated)
                if ( res.input != null ) //could be null for new bucket
                    res.input.decreaseEntries();
                return;
            }
            else {
                //rollback output block usage. It is managed by a current thread, so it is safe
                if ( res != null )
                {
                    res.output.pos = res.outputPrevStart;
                    res.output.decreaseEntries();
                }
                //and get the updated bucket
                bucket = getBucket( tab, idx );
            }
        }
    }

    /**
     * Write a single entry bucket
     * @param output Use this block for output (it has enough space)
     * @param key Key
     * @param value Value
     * @return A long pointing to the written record
     */
    private long singleEntry( final Block output, final double key, final int value )
    {
        final int startPos = output.pos;
        final ByteArray bar = getByteArray( output );
        output.increaseEntries(); //allocate block prior to writing
        getWriter().reset( bar ).writePair( key, value );
        output.pos = bar.position();
        return pack( output.index, startPos, 1 );
    }

    /**
     * Add key/value to a given chain. A chain is locked during the operation, so it can be safely updated
     * @param bucket An existing chain
     * @param key Key
     * @param value Value
     * @return A new chain and an old value
     */
    private UpdateResult addToChain( final long bucket, final double key, final int value )
    {
        if ( bucket == EMPTY ) {
            final Block output = m_blockAllocator.getThreadLocalBlock( m_singleEntryLength );
            final int outputStart = output.pos;
            return getUpdateResult().set( singleEntry( output, key, value ), NO_VALUE, 1, null, output, outputStart );
        }

        final Block inputBlock = getBlockByIndex( bucket );
        if ( inputBlock == null )
            return null; //it means we are already late
        final int inputStartOffset = getOffset( bucket );

        final ByteArray input = getByteArray( inputBlock, inputStartOffset );
        final Iterator iter = getIterator().reset( input, getBlockLength( bucket ) );
        if ( iter.getElems() > MAX_ENCODED_LENGTH - 2 ) //could grow to 255+, which should be stored in the bucket
            return addToChainSlow( bucket, iter, inputBlock, inputStartOffset, key, value );

        while ( iter.hasNext() )
            iter.skip();
        final int chainLength = input.position() - inputStartOffset;
        input.position( inputStartOffset );
        iter.reset( input, getBlockLength( bucket ) );

        //2* is a safety net here due to possibility that a value may take longer in the delta form compared to original form
        final Block outputBlock = m_blockAllocator.getThreadLocalBlock( chainLength + 2 * m_singleEntryLength );
        final int startOutputPos = outputBlock.pos;
        final ByteArray baOutput = getByteArray2( outputBlock ) ;

        outputBlock.increaseEntries(); //allocate block
        final Writer writer = getWriter().reset( baOutput );

        int retValue = NO_VALUE;
        boolean inserted = false, updated = false;

        while ( iter.hasNext() )
        {
            iter.advance();
            if ( iter.getKey() < key )
                writer.writePair( iter.getKey(), iter.getValue() );
            else if ( iter.getKey() == key )
            {
                inserted = true;
                updated = true;
                retValue = iter.getValue();
                writer.writePair( key, value );
            }
            else
            {
                if ( !inserted )
                {
                    inserted = true;
                    writer.writePair( key, value );
                }
                writer.writePair( iter.getKey(), iter.getValue() );
            }
        }
        if ( !inserted ) //all keys are smaller
            writer.writePair( key, value );

        outputBlock.pos = baOutput.position();
        final int sizeChange = updated ? 0 : 1;
        return getUpdateResult().set( pack( outputBlock.index, startOutputPos, iter.getElems() + sizeChange ),
                retValue, sizeChange, inputBlock, outputBlock, startOutputPos );
    }

    /**
     * This is a special version of previous method which deals with chains of possibly over 127 elements.
     * Add key/value to a given chain. A chain is locked during the operation, so it can be safely updated
     * @param iter Input iterator
     * @param inputBlock Input block
     * @param inputStartOffset Start offset for the iterator (prior to len)
     * @param key Key
     * @param value Value
     * @return A new chain and an old value
     */
    private UpdateResult addToChainSlow( final long bucket, final Iterator iter, final Block inputBlock,
                                         final int inputStartOffset, final double key, final int value )
    {
        boolean hasKey = false;
        int retValue = NO_VALUE;
        while ( iter.hasNext() )
        {
            iter.advance();
            if ( iter.getKey() == key )
            {
                hasKey = true;
                retValue = iter.getValue();
            }
        }
        final int chainLength = iter.getBuf().position() - inputStartOffset;
        final int elems = hasKey ? iter.getElems() : iter.getElems() + 1;
        final Block outputBlock = m_blockAllocator.getThreadLocalBlock( chainLength + 2 * m_singleEntryLength + 2 ); //2 for transition from header to chain length
        final int startOutputPos = outputBlock.pos;
        final ByteArray output = getByteArray2( outputBlock );

        outputBlock.increaseEntries();//allocate block
        //here we write the correct number of elements upfront
        final Writer writer = getWriter().reset( output, elems );
        boolean inserted = false;

        //fully reset the iterator (position on the bucket length)
        iter.getBuf().position( inputStartOffset );
        iter.reset( iter.getBuf(), getBlockLength( bucket ) );

        while ( iter.hasNext() )
        {
            iter.advance();
            if ( iter.getKey() < key )
                writer.writePair( iter.getKey(), iter.getValue() );
            else if ( iter.getKey() == key )
            {
                inserted = true;
                retValue = iter.getValue();
                writer.writePair( key, value );
            }
            else
            {
                if ( !inserted )
                {
                    inserted = true;
                    writer.writePair( key, value );
                }
                writer.writePair( iter.getKey(), iter.getValue() );
            }
        }
        if ( !inserted ) //all keys are smaller
            writer.writePair( key, value );

        outputBlock.pos = output.position();
        return getUpdateResult().set( pack( outputBlock.index, startOutputPos, MAX_ENCODED_LENGTH ),
                retValue, hasKey ? 0 : 1, inputBlock, outputBlock, startOutputPos );
    }

    @Override
    public int remove( final double key )
    {
        Buffers buffers = m_data.get();
        if ( buffers.old != null ) {
            rehash( buffers.nextStableVersion ); //help rehashing, we are in rehashing state already
            return remove( key );
        }

        final int idx = getIndex( key, buffers.cur.length );
        long bucket = getBucket( buffers.cur,  idx );
        if ( bucket == EMPTY )
            return NO_VALUE;
        else if ( bucket == RELOCATED )
        {
            rehash( buffers.nextStableVersion );
            return remove( key );
        }

        //CAS loop
        while ( true )
        {
            final UpdateResult res = removeKey( bucket, key );
            /*
             * Possible returns:
              * null - restart
              * res.sizeChange = 0, res.chain = bucket - no key
              * res.sizeChange = -1, res.chain = EMPTY - remove single key
              * res.sizeChange = -1, res.chain != EMPTY - remove from long chain (output required)
             */
            if ( res != null )
            {
                if ( res.chain == bucket ) //equal to 'bucket' - no changes
                {
                    if ( res.chain == getBucket( buffers.cur, idx ) )
                        return NO_VALUE; //no update, key not found
                }
                else if ( compareAndSet( buffers.cur, idx, bucket, res.chain ) )
                {
                    //commit usage changes
                    res.input.decreaseEntries();

                    final int ret = res.retValue; //must be saved in case of rehash
                    changeSize( res.sizeChange, buffers, getBlockLength( res.chain ) );
                    return ret;
                }
                //CAS failed, rollback the output position, it is handled by current thread anyway
                if ( res.output != null )
                {
                    res.output.pos = res.outputPrevStart;
                    res.output.decreaseEntries();
                }
            }

            bucket = getBucket( buffers.cur, idx );
            if ( bucket == RELOCATED )
            {
                rehash( buffers.nextStableVersion ); //saved version is prior to rehashing
                return remove( key );
            }
            else if ( bucket == EMPTY )
                return NO_VALUE;
        }
    }

    /**
     * Remove a given key from a chain.
     * 2 special cases are supported:
     * 1) key not found - same chain is returned
     * 2) removal from a 1-entry long chain - null chain is returned with a valid retValue
     * @param bucket Existing chain
     * @param key Key to remove
     * @return Updated or original chain
     */
    private UpdateResult removeKey( final long bucket, final double key )
    {
        final Block inputBlock = getBlockByIndex( bucket );
        if ( inputBlock == null )
            return null;// too late, need to rerun
        final int inputStartOffset = getOffset( bucket );

        final ByteArray input = getByteArray( inputBlock, inputStartOffset );
        final Iterator iter = getIterator().reset( input, getBlockLength( bucket ) );

        boolean hasKey = false;
        int retValue = NO_VALUE;
        while ( iter.hasNext() )
        {
            iter.advance();
            if ( iter.getKey() == key )
            {
                hasKey = true;
                retValue = iter.getValue();
            }
        }
        if ( !hasKey )
            return getUpdateResult().set( bucket, NO_VALUE, 0, null, null, 0 );

        //special case - chain removal
        if ( iter.getElems() == 1 )
            return getUpdateResult().set( EMPTY, retValue, -1, inputBlock, null, 0 );

        final int chainLength = input.position() - inputStartOffset;
        input.position( inputStartOffset );
        iter.reset( input, getBlockLength( bucket ) );

        final Block outputBlock = m_blockAllocator.getThreadLocalBlock( Math.min( chainLength, ( iter.getElems() - 1 ) * m_singleEntryLength ) );
        final int startOutputPos = outputBlock.pos;
        final ByteArray output = getByteArray2( outputBlock );
        outputBlock.increaseEntries(); //allocate ticket
        final Writer writer = getWriter().reset( output, iter.getElems() <= MAX_ENCODED_LENGTH ? 0 : iter.getElems() - 1 );
        while ( iter.hasNext() )
        {
            iter.advance();
            if ( iter.getKey() != key )
                writer.writePair( iter.getKey(), iter.getValue() );
        }

        outputBlock.pos = output.position();
        return getUpdateResult().set( pack( outputBlock.index, startOutputPos,
                        iter.getElems() <= MAX_ENCODED_LENGTH ? iter.getElems() - 1 : MAX_ENCODED_LENGTH ),
                retValue, -1, inputBlock, outputBlock, startOutputPos );
    }

    @Override
    public long size() {
        return calculateSize();
    }

        /*
    Rehashing:
    new_table is initialized.
    'get', 'put' and 'remove' check that resize is on and help other threads.

    Updates:
    if CAS in the 'put' fails, check if new_table is allocated - help resizing and then execute 'put'
    Same for 'remove'

    Resize:
    Increase the number of workers.
    Each participating thread start from random pos and wrap around until the same pos to reduce contention.
    On non-empty record :
    CAS replace the current value with REPLACED and copy data to the new map.
    Special case:
    If bucket length = 1, we do not need to transfer the chain. Instead we can move the {@code long} key right
    to the correct bucket in the new map (provided it is empty).

    Once finished iteration, reduce the number of workers. The last one moves new table to the current state.
     */

    /**
     * Help rehashing the table.
     * @param nextStableVersion Leave this method once the current version is greater or equal
     */
    private void rehash( final int nextStableVersion )
    {
        Buffers buffers = m_data.get();

        if ( buffers.version >= nextStableVersion )
            return; //done already

        //take ticket
        while ( !m_data.compareAndSet( buffers, buffers = buffers.addWorker() ) )
        {
            buffers = m_data.get();
            if ( buffers.version >= nextStableVersion )
                return; //done already
        }

        rehashInnerLoop( buffers );

        while ( !m_data.compareAndSet( buffers, buffers = buffers.removeWorker() ) )
            buffers = m_data.get();

        if ( buffers.resizeWorkers > 0 )
        {
            //wait until all data is copied, all workers must finish before we can proceed
            while ( m_data.get().version < nextStableVersion )
                LockSupport.parkNanos(1);
        }
    }

    /**
     * Actual data transfer during rehashing happens here
     * @param buffers Current data object
     */
    private void rehashInnerLoop( final Buffers buffers )
    {
        final long[] old = buffers.old;
        final long[] dest = buffers.cur;
        final ByteArray barLocal = new ByteArray();
        final Iterator iterLocal = new Iterator( m_keySerializer, m_valueSerializer );

        //start from random position and wrap round. It should reduce write contention
        final int startPos = ThreadLocalRandom.current().nextInt( old.length );

        for ( int i = startPos; i < old.length; ++i )
            if ( !rehashInnerStep(old, dest, barLocal, iterLocal, i) )
                i += old.length / CPU_NUMBER; //jump ahead to reduce contention / quickly catch up
        for ( int i = 0; i < startPos; ++i )
            if ( !rehashInnerStep(old, dest, barLocal, iterLocal, i) )
                i += old.length / CPU_NUMBER; //jump ahead to reduce contention / quickly catch up
    }

    private boolean rehashInnerStep( final long[] old, final long[] dest, final ByteArray bar, final Iterator iter, final int idxOld )
    {
        final long bucket = getBucket( old, idxOld );
        //Put RELOCATED into each processed cell. This way we distinguish between not used and relocated cells.
        //Besides that, RELOCATED is not a valid chain.
        if ( bucket != RELOCATED && compareAndSet( old, idxOld, bucket, RELOCATED ) )
        {
            if ( bucket == EMPTY ) //empty cells have to be replaced anyway
                return true;

            //this thread is the only one to process this chain
            final Block inputBlock = getBlockByIndex( bucket );
            final int offset = getOffset( bucket );

            final int blockLength = getBlockLength( bucket );
            iter.reset( bar.reset( inputBlock.data, offset ), blockLength );
            if ( blockLength == 1 ) //shortcut, no data copy for blocklen = 1
            {
                iter.advance();
                //if CAS fails, the dest bucket is 1+ long, so we need to go via a long path
                final int index = getIndex( iter.getKey(), dest.length );
                if ( !compareAndSet( dest, index, EMPTY, bucket ) )
                {
                    doPutRehash(dest, iter.getKey(), iter.getValue());
                    inputBlock.decreaseEntries();
                }
            }
            else
            {
                while ( iter.hasNext() )
                {
                    iter.advance();
                    doPutRehash( dest, iter.getKey(), iter.getValue() );
                }
                inputBlock.decreaseEntries(); //bucket relocated
            }
            return true;
        }
        return bucket != RELOCATED;
    }

    /**
     * Get the bucket index for the given key
     * @param key A key
     * @param tabSize Bucket table size
     * @return Bucket index
     */
    private int getIndex( final double key, final int tabSize )
    {
        return Tools.getIndexFast( key, tabSize );
    }


    private static class Iterator
    {
        /** Underlying byte buffer */
        private ByteArray buf;
        /** Number of entries in the bucket */
        private int elems;
        /** Index of the current entry (0-based) */
        private int cur = 0;
        /** Current entry key, initialized by {@code advance} call */
        private double key;
        /** Current entry value, initialized by {@code advance} call */
        private int value;
        /** Serialization for keys */
        private final IDoubleSerializer m_keySerializer;
        /** Serialization for values */
        private final IIntSerializer m_valueSerializer;

        public Iterator( final IDoubleSerializer keySerializer, final IIntSerializer valueSerializer ) {
            m_keySerializer = keySerializer;
            m_valueSerializer = valueSerializer;
        }

        /**
         * Initialize an iterator by a buffer. This method will reads the number of entries if {@code chainLength == 0xFF}
         * @param buf Byte buffer
         * @param chainLength Chain length stored in the header. 0xFF triggers reading actual length from the bucket
         * @return Same iterator object
         */
        Iterator reset( final ByteArray buf, final int chainLength )
        {
            this.buf = buf;
            elems = chainLength != MAX_ENCODED_LENGTH ? chainLength : readUnsignedInt( buf );
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
            if ( cur == 0 ) {
                key = m_keySerializer.read( buf );
                value = m_valueSerializer.read( buf );
            } else {
                key = m_keySerializer.readDelta( key, buf, true );
                value = m_valueSerializer.readDelta( value, buf, false );
            }
            ++cur;
        }

        /**
        * Skip the current entry
        */
        public void skip()
        {
            m_keySerializer.skip( buf );
            m_valueSerializer.skip( buf );
            ++cur;
        }

        /**
         * @return A key read by the last {@code advance} call
         */
        public double getKey() {
            return key;
        }

        /**
         * @return A value read by the last {@code advance} call
         */
        public int getValue() {
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
    private static final class Writer
    {
        /** Underlying byte buffer */
        private ByteArray buf;
        /** Is this a first entry (used for delta encoding) */
        private boolean first = true;
        /** Previously written key (used for delta encoding) */
        private double prevKey = 0;
        /** Previously written value (used for delta encoding) */
        private int prevValue = 0;
        /** Serialization for keys */
        private final IDoubleSerializer m_keySerializer;
        /** Serialization for values */
        private final IIntSerializer m_valueSerializer;

        public Writer( final IDoubleSerializer keySerializer, final IIntSerializer valueSerializer)
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
        public Writer reset( final ByteArray buf )
        {
            return reset( buf, 0 );
        }

        /**
         * Reset a writer (useful if you need to write multiple entries in one method call)
         * @param buf Underlying byte buffer
         * @param elems Number of elements to write, don't write anything if this value is not positive
         * @return this
         */
        public Writer reset( final ByteArray buf, final int elems )
        {
            this.buf = buf;
            if ( elems > 0 )
                writeUnsignedInt( elems, buf );
            first = true;
            prevKey = 0;
            prevValue = 0;
            return this;
        }

        /**
         * Write a key-value pair
         * @param k Key to write
         * @param v Value to write
         */
        public void writePair( final double k, final int v )
        {
            if ( first ) {
                m_keySerializer.write( k, buf );
                m_valueSerializer.write( v, buf );
                first = false;
            }
            else
            {
                //keys are sorted, so we can write unsigned diff (but serializer will make a final decision)
                m_keySerializer.writeDelta( prevKey, k, buf, true );
                //values are NOT sorted
                m_valueSerializer.writeDelta( prevValue, v, buf, false );
            }
            prevKey = k;
            prevValue = v;
        }
    }

    private static ByteArray getByteArray( final ThreadLocal<ByteArray> tba, final Block ar )
    {
        ByteArray res = tba.get();
        if ( res == null )
            tba.set( res = new ByteArray() );
        res.reset( ar.data, ar.pos );
        return res;
    }

    private ByteArray getByteArray( final Block ar )
    {
        return getByteArray( s_bar1, ar );
    }

    private ByteArray getByteArray2( final Block ar )
    {
        return getByteArray( s_bar2, ar );
    }

    private static ByteArray getByteArray( final ThreadLocal<ByteArray> tba, final Block ar, final int offset )
    {
        ByteArray res = tba.get();
        if ( res == null )
            tba.set( res = new ByteArray() );
        res.reset( ar.data, offset );
        return res;
    }

    private ByteArray getByteArray( final Block ar, final int offset )
    {
        return getByteArray( s_bar1, ar, offset );
    }

    /**
     * Get a thread local iterator. Iterators do not depend on any inner map fields, so they could be safely used on per-thread basis.
     * @return A cached iterator object
     */
    private Iterator getIterator()
    {
        Iterator res = s_iters.get();
        if ( res == null )
            s_iters.set( res = new Iterator( m_keySerializer, m_valueSerializer ) );
        return res;
    }

    /**
     * Get a cached writer for the current thread
     * @return A cached writer for the current thread
     */
    private Writer getWriter()
    {
        Writer w = s_writers.get();
        if ( w == null )
            s_writers.set( w = new Writer( m_keySerializer, m_valueSerializer ) );
        return w;
    }

    private UpdateResult getUpdateResult()
    {
        return s_updateRes.get();
    }

    /**
     * All optimistic map changes are returned via this class instances. They contain enough information
     * to commit/rollback these changes using CAS. An instance is always written and then read by the same
     * thread, so no synchronization is needed.
     */
    private static class UpdateResult
    {
        public long chain;
        public int retValue;
        public int sizeChange;
        public Block input;
        public Block output;
        public int outputPrevStart;

        public UpdateResult() {
        }

        public UpdateResult set( long chain, int retValue, int sizeChange, Block input, Block output, int outputPrevStart )
        {
            this.chain = chain;
            this.retValue = retValue;
            this.sizeChange = sizeChange;
            this.input = input;
            this.output = output;
            this.outputPrevStart = outputPrevStart;
            return this;
        }

        @Override
        public String toString() {
            return "UpdateResult{" +
                    "chain=" + chain +
                    ", retValue=" + retValue +
                    ", sizeChange=" + sizeChange +
                    ", input=" + input +
                    ", output=" + output +
                    ", outputPrevStart=" + outputPrevStart +
                    '}';
        }
    }

    /////////////////////////////////////////////////////////////////////
    // Size tracking
    /////////////////////////////////////////////////////////////////////

    private static class MutableLong
    {
        public long v;
    }

    private final ThreadLocal<MutableLong> s_size = new ThreadLocal<MutableLong>(){
        @Override
        protected MutableLong initialValue() {
            final MutableLong res = new MutableLong();
            m_sizes.add( res );
            return res;
        }
    };
    private final CopyOnWriteArrayList<MutableLong> m_sizes = new CopyOnWriteArrayList<>();

    private long calculateSize()
    {
        //todo may need volatile read
        long res = 0;
        for ( final MutableLong ml : m_sizes )
        {
            res += ml.v;
        }
        return res;
    }

    private void addSize( final int delta )
    {
        s_size.get().v += delta;
    }

    private void changeSize( final int delta, final Buffers curBuffers, final int bucketLength )
    {
        if ( delta == 0 )
            return;
        addSize( delta );

        if ( bucketLength > m_iFillFactor && calculateSize() > curBuffers.threshold )
        {
            final long multiplier = m_fillFactor <= 2 ? 2 : m_iFillFactor;
            int newCapacity = Primes.findNextPrime( ( long ) Math.ceil( multiplier * curBuffers.threshold / m_fillFactor ) );
            //check if we got too close to the max array size, otherwise we will have to make a pretty useless resize when got close to 2G*fillFactor entries
            if ( newCapacity * 1.5 > Primes.getMaxIntPrime() )
                newCapacity = Primes.getMaxIntPrime();
            //this check disables rehashing after a table has reached the maximal size
            final long newThreshold = newCapacity >= Primes.getMaxIntPrime() ? Long.MAX_VALUE : curBuffers.threshold * multiplier;
            //Entering rehashing mode. It does not matter which thread changes it, we still have to enter {@code rehash}.
            //Using if-else here helps with debugging
            if ( m_data.compareAndSet( curBuffers, new Buffers( m_longAlloc.allocate( newCapacity ), curBuffers.cur,
                            newThreshold,
                            curBuffers.version + 1, //switching to rehashing mode
                            curBuffers.nextStableVersion //next stable version does not change at this moment
                    ) ) )
                rehash( curBuffers.nextStableVersion ); //we must use cached version here, actual may be greater
            else
                rehash( curBuffers.nextStableVersion ); //we must use cached version here, actual may be greater
        }
    }

    /////////////////////////////////////////////////////////////
    // Unsafe bucket accessors - avoid AtomicReferenceArray overhead
    /////////////////////////////////////////////////////////////

    private static final int BB_BASE;
    private static final int BB_SHIFT;

    private static final Unsafe unsafe;
    static
    {
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe)field.get(null);

            BB_BASE = unsafe.arrayBaseOffset(long[].class);
            int scale = unsafe.arrayIndexScale(long[].class);
            if ((scale & (scale - 1)) != 0)
                throw new Error("data type scale not a power of two");
            BB_SHIFT = 31 - Integer.numberOfLeadingZeros(scale);

        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private boolean compareAndSet( final long[] ar, final int idx, final long expected, final long update )
    {
        return unsafe.compareAndSwapLong(ar, ((long) idx << BB_SHIFT) + BB_BASE, expected, update);
    }

    private long getBucket( final long[] ar, final int idx )
    {
        return unsafe.getLongVolatile(ar, ((long) idx << BB_SHIFT) + BB_BASE);
    }


    /////////////////////////////////////////////////////////////////////
    //   Some debugging
    /////////////////////////////////////////////////////////////////////

    public void printStats()
    {
        final int[] cnt = new int[ 1000 ];
        long sum = 0;
        final long[] tab = m_data.get().cur;
        for ( int i = 0; i < tab.length; ++i )
        {
            final long bucket = getBucket( tab, i );
            if ( bucket != EMPTY )
            {
                final int len = getBlockLength( bucket );
                cnt[ len ]++;
                sum += len;
            }
        }

        System.out.println( "Capacity = " + tab.length );
        System.out.println( "Size = " + sum );

        for ( int i = 0; i < cnt.length; ++i )
            if ( cnt[ i ] > 0 )
                System.out.println( "Length " + i + " = " + cnt[ i ] );
    }

}
