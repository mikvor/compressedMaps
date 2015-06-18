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

import java.util.HashMap;
import java.util.Map;

public class SingleThreadedBlockMap {
    private final Map<Integer, SingleThreadedBlock> m_other = new HashMap<>( 16, 0.75f );
    private SingleThreadedBlock[] m_data = new SingleThreadedBlock[ 1024 ];
    private int m_blockBase = 0;
    private int m_arSize = 0;

    private boolean inRange( final int index )
    {
        return index >= m_blockBase && index < m_blockBase + m_data.length;
    }

    public SingleThreadedBlock get( final int index )
    {
        return inRange( index ) ? m_data[ index - m_blockBase ] : m_other.get( index );
    }

    public SingleThreadedBlock remove(final int index)
    {
        if ( inRange( index ) ) {
            final SingleThreadedBlock res = m_data[ index - m_blockBase ];
            m_data[ index - m_blockBase ] = null;
            --m_arSize;
            return res;
        }
        else
            return m_other.remove( index );
    }

    public void put( final int index, final SingleThreadedBlock block )
    {
        if ( inRange( index ) ) {
            m_data[index - m_blockBase] = block;
            ++m_arSize;
        }
        else if ( m_blockBase > index )
        {
            if ( m_blockBase > 0 && index < 0 )
            {
                //wraparound at Integer.MAX_VALUE
                for ( final SingleThreadedBlock b : m_data )
                    if ( b != null )
                        m_other.put( b.getIndex(), b );

                m_data = new SingleThreadedBlock[ 1024 ];
                m_arSize = 1;
                m_blockBase = index;
                m_data[ 0 ] = block;
            }
            else
                throw new IllegalStateException( "Allocation requests for single threaded BlockMap " +
                        "should not go backwards! m_blockBase = " + m_blockBase + ", requested index = " + index );
        }
        else
        {
            //adding to the right of array, need to cleanup and/or extend the array
            //split an array into 50 sections. Clean until a section with 75+% population.
            final int SECTIONS = 50;
            final int section = m_data.length / SECTIONS;
            for ( int i = 0; i < SECTIONS; ++i )
            {
                int used = 0;
                final int start = section * i;
                final int end = i == SECTIONS - 1 ? m_data.length : (i + 1) * section;
                for ( int j = start; j < end; ++j )
                    if ( m_data[ j ] != null )
                        used++;
                if ( used < ( end - start ) * 3 / 4 )
                {
                    if ( used > 0 ) {
                        //check a corner case first - if all non empty entries are at the end and the next block
                        //is mostly used, then start from this block
                        if ( i < SECTIONS - 1 ) {
                            boolean allUsed = true;
                            for ( int j = end - used; j < end; ++j )
                                if ( m_data[ j ] == null ) {
                                    allUsed = false;
                                    break;
                                }
                            if ( allUsed ) {
                                //now we need to check the next block usage
                                final int startN = section * (i+1);
                                final int endN = (i+1) == SECTIONS - 1 ? m_data.length : (i + 2) * section;
                                int usedN = 0;
                                for ( int j = startN; j < endN; ++j )
                                    if ( m_data[ j ] != null )
                                        usedN++;
                                if ( usedN >= ( endN - startN ) * 3 / 4 )
                                {
                                    //fine, next block is full
                                    final int startPos = end - used;
                                    final int remaining = m_data.length - startPos;
                                    moveData( index, block, startPos, remaining );
                                    return;
                                }
                            }
                        }
                        for ( int j = start; j < end; ++j ) {
                            final SingleThreadedBlock b = m_data[ j ];
                            if ( b != null )
                                m_other.put( b.getIndex(), b );
                        }
                    }
                }
                else
                {
                    int startPos = start;
                    while ( startPos < end && m_data[ startPos ] == null )
                        ++startPos;
                    final int remaining = m_data.length - startPos;
                    moveData( index, block, startPos, remaining );
                    return;
                }
            }
            //data is too sparse
            m_data = new SingleThreadedBlock[ getNewBufferSize( 0, index ) ];
            m_blockBase = index;
            m_data[ 0 ] = block;
            m_arSize = 1;
        }
    }

    private void moveData( final int index, final SingleThreadedBlock block, final int startPos, final int remaining )
    {
        m_arSize = 1;
        final int newBufferSize = getNewBufferSize( remaining, m_blockBase );
        if ( newBufferSize >= m_data.length / 2 && newBufferSize <= m_data.length * 1.2 && m_data.length >= remaining )
        {
            //optimization for relatively small maps
            for ( int j = 0; j < remaining; ++j ) {
                m_data[ j ] = m_data[ j + startPos ];
                m_data[ j + startPos ] = null;
                if ( m_data[ j ] != null )
                    ++m_arSize;
            }
        }
        else {
            final SingleThreadedBlock[] newAr = new SingleThreadedBlock[ newBufferSize ];
            for ( int j = 0; j < remaining; ++j ) {
                newAr[ j ] = m_data[ j + startPos ];
                if ( m_data[ j + startPos ] != null )
                    ++m_arSize;
            }
            m_data = newAr;
        }
        m_blockBase += startPos;
        m_data[ index - m_blockBase ] = block;
    }

    public int size()
    {
        return m_arSize + m_other.size();
    }

    private static int getNewBufferSize( final int remaining, final int base ) {
        return Math.min(
                    Math.max( 1024, remaining * 2 ),
                    base >= 1 ? Integer.MAX_VALUE - base + 1 : Integer.MAX_VALUE
                );
    }

}
