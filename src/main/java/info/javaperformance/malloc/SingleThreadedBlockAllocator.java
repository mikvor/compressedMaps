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

public class SingleThreadedBlockAllocator implements IBlockAllocator {
    /** Data blocks are stored here */
    private final SingleThreadedBlockMap m_blocks = new SingleThreadedBlockMap();
    /** Always take next value for block allocation */
    private int m_nextBlock = 0;

    //must not be static - we don't want to share updateable objects
    private Block m_currentBlock = null;

    @Override
    public Block getBlockByIndex( final int index )
    {
        return m_blocks.get( index );
    }

    @Override
    public void removeBlock( final int blockId )
    {
        m_blocks.remove( blockId );
    }

    /**
     * Allocate a new block of a given size.
     * @param blockSize Block size, should be defined by callers
     * @return A new block
     */
    private Block allocateNewBlock( final int blockSize )
    {
        final int id = ++m_nextBlock;
        final Block b = new Block( this, id, blockSize );
        m_blocks.put( id, b );
        return b;
    }

    /**
     * Get current or allocate a new thread local block
     * @param forceNew True to force a new block allocation
     * @param data Buckets object, used to calculate the block size
     * @return A block managed by a current thread
     */
    private Block getCurrentBlock( final boolean forceNew, final Buckets data )
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
    public Block getBlock( final int requiredSize, final Buckets data )
    {
        Block cur = getCurrentBlock( false, data );
        if ( !cur.hasSpace( requiredSize ) ) {
            cur.writeFinished();
            cur = getCurrentBlock( true, data );
        }
        return cur;
    }

}
