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
 * Methods common between single-threaded and concurrent block allocators
 */
public interface IBlockAllocator {

    /**
     * Retrieve a block by its index. Note that null returned from this method generally means an error.
     * @param index Block index
     * @return A block with a given index or null if a block is not found
     */
    public Block getBlockByIndex( final int index );

    /**
     * Remove block from the allocator. Called by a block itself once it is full and contains no used chains
     * @param blockId Block id
     */
    public void removeBlock( final int blockId );

}
