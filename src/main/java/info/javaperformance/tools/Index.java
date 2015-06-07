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

import java.util.Arrays;

/**
 * This class implements the offset index: you set the index capacity and then use it like array (get/set methods).
 * All methods are O(1).
 * The advantage of this class is that it uses 3 instead of 4 bytes per cell until you add a value higher or equal than 2^24 to it
 * (it allows to gain space savings on aux buffers up to 16M)
 */
public class Index {
    private static final int EMPTY_OFFSET = -1;
    private static final byte EMPTY_BYTE = (byte) 0xFF;

    private static final int THRESHOLD = 0xFF0000; //matches with isEmpty definition

    private int[] array;
    private byte[] small;

    public Index( final int capacity )
    {
        small = new byte[ capacity * 3 ];
        Arrays.fill(small, EMPTY_BYTE);
    }

    public boolean isEmpty( final int idx )
    {
        if ( array != null )
            return array[ idx ] == EMPTY_OFFSET;
        else
            return small[ idx * 3 ] == EMPTY_BYTE;
    }

    public void setEmpty( final int idx )
    {
        if ( array != null )
            array[ idx ] = EMPTY_OFFSET;
        else
            small[ idx * 3 ] = EMPTY_BYTE;
    }

    public int get( final int idx )
    {
        if ( array != null )
            return array[ idx ];
        else
        {
            final int base = idx * 3;
            return (small[ base ] & 0xFF) << 16 | ( small[ base + 1 ] & 0xFF ) << 8 | ( small[ base + 2 ] & 0xFF );
        }
    }

    public void set( final int idx, final int value )
    {
        if ( array != null )
            array[ idx ] = value;
        else
        {
            if ( value >= THRESHOLD )
            {
                //upgrade
                final int[] ar = new int[ size() ];
                for ( int i = 0; i < size(); ++i )
                    ar[ i ] = isEmpty( i ) ? EMPTY_OFFSET : get( i );
                small = null;
                array = ar;
                array[ idx ] = value;
                return;
            }

            final int base = idx * 3;
            small[ base ] = (byte) ((value >> 16) & 0xFF);
            small[ base + 1 ] = (byte) ((value >> 8) & 0xFF);
            small[ base + 2 ] = (byte) (value & 0xFF);
        }
    }

    public int size()
    {
        if ( array != null )
            return array.length;
        else
            return small.length / 3;
    }

    public int memorySize()
    {
        return array != null ? size() * 4 : size() * 3;
    }

}
