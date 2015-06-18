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

package info.javaperformance.compressedmaps;

import info.javaperformance.compressedmaps.concurrent.longint.IConcurrentLongIntMap;
import info.javaperformance.compressedmaps.concurrent.longint.LongIntConcurrentChainedMap;
import info.javaperformance.compressedmaps.normal.intint.IIntIntMap;
import info.javaperformance.compressedmaps.normal.intint.IntIntChainedMap;
import info.javaperformance.compressedmaps.normal.intlong.IIntLongMap;
import info.javaperformance.compressedmaps.normal.intlong.IntLongChainedMap;
import info.javaperformance.compressedmaps.normal.longint.ILongIntMap;
import info.javaperformance.compressedmaps.normal.longint.LongIntChainedMap;
import info.javaperformance.compressedmaps.normal.longlong.ILongLongMap;
import info.javaperformance.compressedmaps.normal.longlong.LongLongChainedMap;
import info.javaperformance.serializers.IIntSerializer;
import info.javaperformance.serializers.ILongSerializer;

/**
 * The entry point for all map users. This class provides the factory methods which allow you to create
 * the maps without binding yourself to the concrete implementations.
 *
 * All factories have the same constraints:
 * - fill factor between 0.01 (exclusive) and 16 (inclusive)
 * - initial size could be greater than {@code Integer.MAX_VALUE} (it makes sense because the fill factor could be greater than 1)
 *
 * todo
 * generate this file after concurrent code generator is ready
 */
public class CompressedMapFactory
{
    /////////////////////////////////////////////////////////////
    //  Single threaded maps
    /////////////////////////////////////////////////////////////

    public IIntIntMap singleThreadedIntIntMap( final long size, final float fillFactor )
    {
        return new IntIntChainedMap( size, fillFactor );
    }

    public IIntIntMap singleThreadedIntIntMap( final long size, final float fillFactor,
                                               final IIntSerializer keySerializer, final IIntSerializer valueSerializer,
                                               final long blockCacheLimit )
    {
        return new IntIntChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    public IIntLongMap singleThreadedIntLongMap( final long size, final float fillFactor )
    {
        return new IntLongChainedMap( size, fillFactor );
    }

    public IIntLongMap singleThreadedIntLongMap( final long size, final float fillFactor,
                                                 final IIntSerializer keySerializer, final ILongSerializer valueSerializer,
                                                 final long blockCacheLimit )
    {
        return new IntLongChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    public ILongIntMap singleThreadedLongIntMap( final long size, final float fillFactor )
    {
        return new LongIntChainedMap( size, fillFactor );
    }

    public ILongIntMap singleThreadedLongIntMap( final long size, final float fillFactor,
                                                 final ILongSerializer keySerializer, final IIntSerializer valueSerializer,
                                                 final long blockCacheLimit )
    {
        return new LongIntChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    public ILongLongMap singleThreadedLongLongMap( final long size, final float fillFactor )
    {
        return new LongLongChainedMap( size, fillFactor );
    }

    public ILongLongMap singleThreadedLongLongMap( final long size, final float fillFactor,
                                                   final ILongSerializer keySerializer, final ILongSerializer valueSerializer,
                                                   final long blockCacheLimit )
    {
        return new LongLongChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    /////////////////////////////////////////////////////////////
    //  Concurrent maps
    /////////////////////////////////////////////////////////////

    public IConcurrentLongIntMap concurrentLongIntMap( final long size, final float fillFactor )
    {
        return new LongIntConcurrentChainedMap( size, fillFactor );
    }

    public IConcurrentLongIntMap concurrentLongIntMap( final long size, final float fillFactor,
                                                       final ILongSerializer keySerializer, final IIntSerializer valueSerializer )
    {
        return new LongIntConcurrentChainedMap( size, fillFactor, keySerializer, valueSerializer );
    }
}
