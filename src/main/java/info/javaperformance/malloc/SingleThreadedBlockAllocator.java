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

import info.javaperformance.buckets.Buckets;

import java.util.ArrayDeque;

public class SingleThreadedBlockAllocator {
    public static final long DEFAULT_RECYCLE_BOUND = 32 * 1024;

    /** Data blocks are stored here */
    private final SingleThreadedBlockMap m_blocks = new SingleThreadedBlockMap();
    /** Always take next value for block allocation */
    private int m_nextBlock = 0;
    /** Block recycle queue. Has an upper bound in order to avoid keeping too much data */
    private final ArrayDeque<SingleThreadedBlock> m_recycle;
    /** Currently appended block */
    private SingleThreadedBlock m_currentBlock = null;
    /** Maximal amount of memory in the blocks we want to recycle */
    private final long m_recycleMemoryLimit;
    /** The amount of storage in the currently available recycled blocks */
    private long m_currentlyRecycled;

    /**
     * Create an allocator with a given recycle memory limit
     * @param recycleMemoryLimit Maximal amount of memory we want to keep in the recycle queue
     */
    public SingleThreadedBlockAllocator( final long recycleMemoryLimit ) {
        m_recycleMemoryLimit = recycleMemoryLimit;
        m_recycle = new ArrayDeque<>( 16 );
    }

    /**
     * Get a block with a given index
     * @param index Block index
     * @return A block or {@code null} if a block is not found (which generally means a bug)
     */
    public SingleThreadedBlock getBlockByIndex( final int index )
    {
        return m_blocks.get( index );
    }

    /**
     * Remove a block from the allocator. Should be called by blocks only when their usage counter goes down to zero
     * @param blockId Block index
     */
    public void removeBlock( final int blockId )
    {
        final SingleThreadedBlock old = m_blocks.remove( blockId );
        //save the block for later reuse. It most likely resides in the old gen already, so there is
        //not much sense to discard it any longer.
        if ( old != null && old.data.length + m_currentlyRecycled <= m_recycleMemoryLimit ) {
            m_recycle.add( old );
            m_currentlyRecycled += old.data.length;
        }
    }

    /**
     * Allocate a new block of a given size.
     * @param blockSize Block size, should be defined by callers
     * @return A new block
     */
    private SingleThreadedBlock allocateNewBlock( final int blockSize )
    {
        final int id = ++m_nextBlock;
        SingleThreadedBlock b = null;
        //try reusing a block prior to allocation
        if ( m_currentlyRecycled > 0 ) {
            while ( m_currentlyRecycled > 0 ) {
                final SingleThreadedBlock block = m_recycle.removeFirst();
                m_currentlyRecycled -= block.data.length;
                //we should not allocate blocks bigger than requested. Smaller blocks are OK.
                if ( block.data.length <= blockSize ) {
                    b = block.reset( id );
                    break;
                }
            }
        }
        if ( b == null )
            b = new SingleThreadedBlock( this, id, blockSize );
        m_blocks.put( id, b );
        return b;
    }

    /**
     * Get current or allocate a new thread local block
     * @param forceNew True to force a new block allocation
     * @param data Buckets object, used to calculate the block size
     * @return A block managed by a current thread
     */
    private SingleThreadedBlock getCurrentBlock( final boolean forceNew, final Buckets data )
    {
        if ( forceNew )
            return ( m_currentBlock = allocateNewBlock( data.getBlockSize( m_blocks.size() ) ) );
        else
        {
            if ( m_currentBlock == null )
                m_currentBlock = allocateNewBlock( data.getBlockSize( m_blocks.size() ) );
            return m_currentBlock;
        }
    }

    /**
     * Get a thread local block which can contain the requested amount of data
     * @param requiredSize Required space
     * @param data Buckets object, used to calculate the block size
     * @return A block
     */
    public SingleThreadedBlock getBlock( final int requiredSize, final Buckets data )
    {
        SingleThreadedBlock cur = getCurrentBlock( false, data );
        if ( !cur.hasSpace( requiredSize ) ) {
            cur.writeFinished();
            cur = getCurrentBlock( true, data );
        }
        return cur;
    }

}
