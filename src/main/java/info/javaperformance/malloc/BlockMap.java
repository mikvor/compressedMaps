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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

/**
 * TODO This class is not ready yet.
 *
 * A concurrent block map.
 * It works based on the assumption that most of keys are consecutive and we insert keys in nearly consecutive order
 * (some insertions may be delayed, but they will happen eventually).
 *
 * This implementation should keep the dense part of keys in the array and the rest (we should minimize it)
 * in the Java ConcurrentHashMap (it requires boxing, so its use should be discouraged).
 */
public class BlockMap {
    private final ConcurrentHashMap<Integer, Block> m_other = new ConcurrentHashMap<>( 16 );
    private final AtomicReference<Buffer> m_data = new AtomicReference<>( new Buffer( 1024 ) );
    private final AtomicReference<Thread> m_relocating = new AtomicReference<>( null );

    private static final Block RELOCATED = new Block( null, -1, 0 );

    private static class Buffer
    {
        public final AtomicReferenceArray<Block> m_data;
        public final int m_base;
        public final AtomicInteger m_size = new AtomicInteger( 0 );

        public Buffer( final int size )
        {
            this( new AtomicReferenceArray<Block>( size ), 0 );
        }

        public Buffer( final AtomicReferenceArray<Block> data, final int base )
        {
            m_data = data;
            m_base = base;
        }

        public boolean inRange( final int index )
        {
            return index >= m_base && index < m_base + m_data.length();
        }

        public Block get( final int index )
        {
            return m_data.get( index - m_base );
        }

        public void set( final int index, final Block block )
        {
            final Block old = m_data.getAndSet( index - m_base, block );
            if ( block != RELOCATED ) {
                if (old == null && block != null)
                    m_size.incrementAndGet();
                else if (old != null && block == null)
                    m_size.decrementAndGet();
            }
        }

        public boolean compareAndSet( final int index, final Block expected, final Block newVal )
        {
            if ( m_data.compareAndSet( index - m_base, expected, newVal ) )
            {
                if (expected == null && newVal != null)
                    m_size.incrementAndGet();
                else if (expected != null && newVal == null)
                    m_size.decrementAndGet();
                return true;
            }
            return false;
        }

        public int getBase()
        {
            return m_base;
        }

        public int size()
        {
            return m_size.get();
        }

        @Override
        public String toString() {
            return "Buffer{" +
                    "m_data.len=" + m_data.length() +
                    ", m_base=" + m_base +
                    ", m_size=" + m_size +
                    '}';
        }
    }


    public Block get(final int index)
    {
        final Buffer buf = m_data.get();
        if ( buf.inRange( index ) ) {
            final Block res = buf.get( index );
            if ( res == RELOCATED ) {
                while ( m_relocating.get() != null ) //wait until done
                    LockSupport.parkNanos( 1 );
                return get(index);
            }
            return res;
        }
        else
            return m_other.get( index );
    }

    public void remove(final int index)
    {
        while ( m_relocating.get() != null ) //wait for update until relocation is done
            LockSupport.parkNanos( 1 );

        final Buffer buf = m_data.get();
        if ( buf.inRange( index ) ) {
            final Block cur = buf.get( index );
            if ( cur == RELOCATED || !buf.compareAndSet( index, cur, null ) )
                remove(index); //retry
        }
        else
            m_other.remove( index );
    }

    public void put( final int index, final Block block )
    {
        while ( m_relocating.get() != null )
            LockSupport.parkNanos( 1 );

        final Buffer buf = m_data.get();
        if ( buf.inRange( index ) ) {
            final Block cur = buf.get( index );
            if ( cur == RELOCATED || !buf.compareAndSet( index, cur, block ) )
                put(index, block); //assuming method return after this call
        }
        else if ( buf.getBase() > index )
        {
            if ( buf.getBase() > 0 && index < 0 ) {
                //this is possible only in case of wrapping the indices over Integer.MAX_VALUE.
                //we need to relocate the array contents into a map and start a new array from this index
                if ( !m_relocating.compareAndSet( null, Thread.currentThread() ) ) {
                    while ( m_relocating.get() != null ) //wait for update until relocation is done
                        LockSupport.parkNanos(1);
                    put(index, block);
                } else {
                    final Buffer newBuf = createBuffer( 0, index );

                    //copy all existing blocks into a map
                    for ( int i = 0; i < buf.m_data.length(); ++i ) {
                        final Block b = buf.m_data.getAndSet(i, RELOCATED);
                        if ( b != null )
                            m_other.put(b.index, b); //extra keys would not hurt the access
                    }
                    newBuf.set(index, block);
                    m_data.set(newBuf);
                    m_relocating.set( null ); //unlock for updates
                }
            }
            else
                m_other.put( index, block ); //out of range, so could safely go into map
        }
        else {
            //we are inserting to the right of the array, need to clean up and possibly extend
            //new size = max( double the number of existing entries or 1024 )
            if ( !m_relocating.compareAndSet( null, Thread.currentThread() ) )
            {
                while ( m_relocating.get() != null ) //wait for update until relocation is done
                    LockSupport.parkNanos(1);
                put( index, block );
            }
            else
            {
                final AtomicReferenceArray<Block> data = buf.m_data;
                //split into 50 sections. Clean until a section with 75+% population.
                final int SECTIONS = 50;
                final int section = data.length() / SECTIONS;
                for ( int i = 0; i < SECTIONS; ++i )
                {
                    int used = 0;
                    final int start = section * i;
                    final int end = i == SECTIONS - 1 ? data.length() : (i + 1) * section;
                    for ( int j = start; j < end; ++j )
                        if ( data.get( j ) != null )
                            used++;
                    if ( used < ( end - start ) * 3 / 4 )
                    {
                        if ( used > 0 )
                            for ( int j = start; j < end; ++j ) {
                                final Block b = data.getAndSet(j, RELOCATED);
                                if ( b != null )
                                    m_other.put( b.index, b );
                            }
                    }
                    else
                    {
                        int p = start;
                        while ( p < end && data.get( p ) == null )
                            ++p;
                        final Buffer newBuf = copyArray( data, p, buf.getBase() );
                        System.out.println( "Resize, old buf = " + buf + ", new buf = " + newBuf );

                        newBuf.set( index, block ); //current put operation.
                        m_data.set( newBuf );
                        m_relocating.set( null ); //unlock for updates
                        return;
                    }
                }

                //data is too sparse
                final Buffer newBuf = createBuffer( 0, index );
                System.out.println( "Resize, old buf = " + buf + ", new buf = " + newBuf );

                newBuf.set( index, block ); //current put call
                m_data.set( newBuf );
                m_relocating.set( null ); //unlock for updates
            }
        }
    }

    public int size()
    {
        return m_data.get().size() + m_other.size();
    }

    private static Buffer copyArray( final AtomicReferenceArray<Block> data, final int startPos, final int oldBufBase )
    {
        final int remaining = data.length() - startPos;
        final int base = oldBufBase + startPos;
        final Buffer buf = createBuffer( remaining, base );
        for ( int i = 0; i < remaining; ++i )
            buf.set( base + i, data.getAndSet(startPos + i, RELOCATED) );
        return buf;
    }

    private static Buffer createBuffer( final int remaining, final int base )
    {
        return new Buffer( new AtomicReferenceArray<Block>(
                Math.min(
                    Math.max( 1024, remaining * 2 ),
                    base >= 1 ? Integer.MAX_VALUE - base + 1 : Integer.MAX_VALUE
                )
        ), base );
    }


}
