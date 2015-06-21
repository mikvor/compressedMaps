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

import info.javaperformance.compressedmaps.normal.longs.*;
import info.javaperformance.compressedmaps.concurrent.longs.*;

import info.javaperformance.serializers.IFloatSerializer;

import info.javaperformance.serializers.ILongSerializer;

import info.javaperformance.serializers.IDoubleSerializer;

import info.javaperformance.serializers.IIntSerializer;

/**
 * The entry point for all map users. This class provides the factory methods which allow you to create
 * the maps without binding yourself to the concrete implementations.
 *
 * All factories have the same constraints:
 * - fill factor between 0.01 (exclusive) and 16 (inclusive)
 * - initial size could be greater than {@code Integer.MAX_VALUE} (it makes sense because the fill factor could be greater than 1)
 */
public class LongMapFactory
{
    /////////////////////////////////////////////////////////////
    //  Single threaded maps
    /////////////////////////////////////////////////////////////

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

    public ILongFloatMap singleThreadedLongFloatMap( final long size, final float fillFactor )
    {
        return new LongFloatChainedMap( size, fillFactor );
    }

    public ILongFloatMap singleThreadedLongFloatMap( final long size, final float fillFactor,
                                                   final ILongSerializer keySerializer, final IFloatSerializer valueSerializer,
                                                   final long blockCacheLimit )
    {
        return new LongFloatChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    public ILongDoubleMap singleThreadedLongDoubleMap( final long size, final float fillFactor )
    {
        return new LongDoubleChainedMap( size, fillFactor );
    }

    public ILongDoubleMap singleThreadedLongDoubleMap( final long size, final float fillFactor,
                                                   final ILongSerializer keySerializer, final IDoubleSerializer valueSerializer,
                                                   final long blockCacheLimit )
    {
        return new LongDoubleChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    /////////////////////////////////////////////////////////////
    //  Concurrent maps
    /////////////////////////////////////////////////////////////

    public ILongIntConcurrentMap concurrentLongIntMap( final long size, final float fillFactor )
    {
        return new LongIntConcurrentChainedMap( size, fillFactor );
    }

    public ILongIntConcurrentMap concurrentLongIntMap( final long size, final float fillFactor,
                                                         final ILongSerializer keySerializer, final IIntSerializer valueSerializer )
    {
        return new LongIntConcurrentChainedMap( size, fillFactor, keySerializer, valueSerializer );
    }

    public ILongLongConcurrentMap concurrentLongLongMap( final long size, final float fillFactor )
    {
        return new LongLongConcurrentChainedMap( size, fillFactor );
    }

    public ILongLongConcurrentMap concurrentLongLongMap( final long size, final float fillFactor,
                                                         final ILongSerializer keySerializer, final ILongSerializer valueSerializer )
    {
        return new LongLongConcurrentChainedMap( size, fillFactor, keySerializer, valueSerializer );
    }

    public ILongFloatConcurrentMap concurrentLongFloatMap( final long size, final float fillFactor )
    {
        return new LongFloatConcurrentChainedMap( size, fillFactor );
    }

    public ILongFloatConcurrentMap concurrentLongFloatMap( final long size, final float fillFactor,
                                                         final ILongSerializer keySerializer, final IFloatSerializer valueSerializer )
    {
        return new LongFloatConcurrentChainedMap( size, fillFactor, keySerializer, valueSerializer );
    }

    public ILongDoubleConcurrentMap concurrentLongDoubleMap( final long size, final float fillFactor )
    {
        return new LongDoubleConcurrentChainedMap( size, fillFactor );
    }

    public ILongDoubleConcurrentMap concurrentLongDoubleMap( final long size, final float fillFactor,
                                                         final ILongSerializer keySerializer, final IDoubleSerializer valueSerializer )
    {
        return new LongDoubleConcurrentChainedMap( size, fillFactor, keySerializer, valueSerializer );
    }

}

