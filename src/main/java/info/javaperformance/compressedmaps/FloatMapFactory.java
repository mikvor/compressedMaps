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

import info.javaperformance.compressedmaps.normal.floats.*;
import info.javaperformance.compressedmaps.concurrent.floats.*;

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
public class FloatMapFactory
{
    /////////////////////////////////////////////////////////////
    //  Single threaded maps
    /////////////////////////////////////////////////////////////

    public IFloatIntMap singleThreadedFloatIntMap( final long size, final float fillFactor )
    {
        return new FloatIntChainedMap( size, fillFactor );
    }

    public IFloatIntMap singleThreadedFloatIntMap( final long size, final float fillFactor,
                                                   final IFloatSerializer keySerializer, final IIntSerializer valueSerializer,
                                                   final long blockCacheLimit )
    {
        return new FloatIntChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    public IFloatLongMap singleThreadedFloatLongMap( final long size, final float fillFactor )
    {
        return new FloatLongChainedMap( size, fillFactor );
    }

    public IFloatLongMap singleThreadedFloatLongMap( final long size, final float fillFactor,
                                                   final IFloatSerializer keySerializer, final ILongSerializer valueSerializer,
                                                   final long blockCacheLimit )
    {
        return new FloatLongChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    public IFloatFloatMap singleThreadedFloatFloatMap( final long size, final float fillFactor )
    {
        return new FloatFloatChainedMap( size, fillFactor );
    }

    public IFloatFloatMap singleThreadedFloatFloatMap( final long size, final float fillFactor,
                                                   final IFloatSerializer keySerializer, final IFloatSerializer valueSerializer,
                                                   final long blockCacheLimit )
    {
        return new FloatFloatChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    public IFloatDoubleMap singleThreadedFloatDoubleMap( final long size, final float fillFactor )
    {
        return new FloatDoubleChainedMap( size, fillFactor );
    }

    public IFloatDoubleMap singleThreadedFloatDoubleMap( final long size, final float fillFactor,
                                                   final IFloatSerializer keySerializer, final IDoubleSerializer valueSerializer,
                                                   final long blockCacheLimit )
    {
        return new FloatDoubleChainedMap( size, fillFactor, keySerializer, valueSerializer, blockCacheLimit );
    }

    /////////////////////////////////////////////////////////////
    //  Concurrent maps
    /////////////////////////////////////////////////////////////

    public IFloatIntConcurrentMap concurrentFloatIntMap( final long size, final float fillFactor )
    {
        return new FloatIntConcurrentChainedMap( size, fillFactor );
    }

    public IFloatIntConcurrentMap concurrentFloatIntMap( final long size, final float fillFactor,
                                                         final IFloatSerializer keySerializer, final IIntSerializer valueSerializer )
    {
        return new FloatIntConcurrentChainedMap( size, fillFactor, keySerializer, valueSerializer );
    }

    public IFloatLongConcurrentMap concurrentFloatLongMap( final long size, final float fillFactor )
    {
        return new FloatLongConcurrentChainedMap( size, fillFactor );
    }

    public IFloatLongConcurrentMap concurrentFloatLongMap( final long size, final float fillFactor,
                                                         final IFloatSerializer keySerializer, final ILongSerializer valueSerializer )
    {
        return new FloatLongConcurrentChainedMap( size, fillFactor, keySerializer, valueSerializer );
    }

    public IFloatFloatConcurrentMap concurrentFloatFloatMap( final long size, final float fillFactor )
    {
        return new FloatFloatConcurrentChainedMap( size, fillFactor );
    }

    public IFloatFloatConcurrentMap concurrentFloatFloatMap( final long size, final float fillFactor,
                                                         final IFloatSerializer keySerializer, final IFloatSerializer valueSerializer )
    {
        return new FloatFloatConcurrentChainedMap( size, fillFactor, keySerializer, valueSerializer );
    }

    public IFloatDoubleConcurrentMap concurrentFloatDoubleMap( final long size, final float fillFactor )
    {
        return new FloatDoubleConcurrentChainedMap( size, fillFactor );
    }

    public IFloatDoubleConcurrentMap concurrentFloatDoubleMap( final long size, final float fillFactor,
                                                         final IFloatSerializer keySerializer, final IDoubleSerializer valueSerializer )
    {
        return new FloatDoubleConcurrentChainedMap( size, fillFactor, keySerializer, valueSerializer );
    }

}

