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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit of memory allocation.
 */
public class Block {
    /** Memory allocator. This reference is needed for the cleanup. */
    private final IBlockAllocator parent;
    /** We stre data here */
    public final byte[] data;
    /** Index of this block in the allocator */
    public final int index;
    /** Current write position */
    public int pos;
    /** Usage counter */
    private final AtomicInteger used = new AtomicInteger( 0 );
    /** Have we filled the whole block? */
    public volatile boolean writeDone;

    public Block( final IBlockAllocator parent, final int index, final int size )
    {
        this.parent = parent;
        this.index = index;
        data = new byte[ size ];
        pos = 0;
        writeDone = false;
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
        if ( used.decrementAndGet() == 0 && writeDone )
            parent.removeBlock( index );
    }

    /**
     * Increase usage counter
     */
    public void increaseEntries()
    {
        used.incrementAndGet();
    }

    /**
     * Mark this block as readonly. We will not append any more data into it.
     */
    public void writeFinished()
    {
        writeDone = true; //from now on only useCount could be reduced
    }

    @Override
    public String toString() {
        return "Block{" +
                "data.len=" + data.length +
                ", index=" + index +
                ", pos=" + pos +
                ", used=" + used +
                ", writeDone=" + writeDone +
                '}';
    }
}
