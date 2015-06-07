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

package info.javaperformance.tools;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * This class acts as a custom allocator for long[] used as (block; offset) pairs in the maps.
 * It is required for concurrent maps because there is high likelihood that several threads may attempt
 * to allocate array of the same length at once, but only one of these arrays will be used.
 */
public class LongAllocator {
    private final AtomicInteger m_allocating = new AtomicInteger( -1 );
    private volatile WeakReference<long[]> m_data = new WeakReference<>( null );

    public long[] allocate( final int size )
    {
        final WeakReference<long[]> data = m_data;
        final long[] ar = data.get();
        if ( ar != null && ar.length >= size )
            return ar;

        final int currentlyAllocated = m_allocating.get();
        if ( currentlyAllocated < 0 ) {
            if (m_allocating.compareAndSet(currentlyAllocated, size )) {
                try {
                    final long[] res = new long[size];
                    m_data = new WeakReference<>(res);
                    m_allocating.set( -1 );
                    return res;
                }
                catch ( OutOfMemoryError ex )
                {
                    m_allocating.set( -1 ); //at least do not hold the lock
                    throw ex;
                }
            }
        }
        while ( m_allocating.get() >= 0 )
            LockSupport.parkNanos( 10 );
        return allocate( size );
    }
}
