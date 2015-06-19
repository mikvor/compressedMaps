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
 * A single threaded version of a block class with removed concurrency requirements
 * and recycling support.
 */
public class SingleThreadedBlock {
    /** Memory allocator. This reference is needed for the cleanup. */
    private final SingleThreadedBlockAllocator parent;
    /** We store data here */
    public final byte[] data;
    /** Index of this block in the allocator */
    private int m_index;
    /** Current write position */
    public int pos;
    /** Usage counter */
    private int m_used = 0;
    /** Have we filled the whole block? */
    private boolean m_writeDone;

    public SingleThreadedBlock( final SingleThreadedBlockAllocator parent, final int index, final int size )
    {
        this.parent = parent;
        m_index = index;
        data = new byte[ size ];
        pos = 0;
        m_writeDone = false;
    }

    /**
     * Reset the same block before reusing it.
     * @param newIndex New block index
     * @return this
     */
    public SingleThreadedBlock reset( final int newIndex )
    {
        m_index = newIndex;
        pos = 0;
        m_used = 0;
        m_writeDone = false;
        return this;
    }

    public int getIndex() {
        return m_index;
    }

    /**
     * Check if we have enough space to insert another {@code req} bytes.
     * @param req Number of bytes we want to write
     * @return True if there is enough space, false otherwise
     */
    public boolean hasSpace( final int req )
    {
        return data.length - pos >= req;
    }

    /**
     * Decrease usage counter
     */
    public void decreaseEntries()
    {
        //clean up itself
        if ( --m_used == 0 && m_writeDone )
            parent.removeBlock( m_index );
    }

    /**
     * Increase usage counter
     */
    public void increaseEntries()
    {
        ++m_used;
    }

    /**
     * Mark this block as readonly. We will not append any more data into it.
     */
    public void writeFinished()
    {
        m_writeDone = true;
    }

    @Override
    public String toString() {
        return "Block{" +
                "data.len=" + data.length +
                ", index=" + m_index +
                ", pos=" + pos +
                ", used=" + m_used +
                ", writeDone=" + m_writeDone +
                '}';
    }

}
