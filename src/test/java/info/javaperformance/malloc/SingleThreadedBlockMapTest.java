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

import junit.framework.TestCase;

public class SingleThreadedBlockMapTest extends TestCase {
    public void testSimple()
    {
        final SingleThreadedBlockMap map = new SingleThreadedBlockMap();
        final int DELAY = 100000;
        for ( int i = 0; i < DELAY; ++i )
            addBlock( map, i );
        assertEquals( DELAY, map.size() );
        for ( int i = DELAY; i < DELAY * 10; ++i )
        {
            assertEquals( i - DELAY, map.get( i - DELAY ).getIndex() );
            addBlock( map, i );
            assertEquals( DELAY + 1, map.size() );
            map.remove( i - DELAY );
            assertEquals( DELAY, map.size() );
        }
    }

    public void testWrap()
    {
        int id = Integer.MAX_VALUE - 1;
        final SingleThreadedBlockMap map = new SingleThreadedBlockMap();
        for ( int i = 1; i <= 4; ++i )
        {
            addBlock( map, id + i );
            assertEquals( i, map.size() );
        }
        checkPresent(map, id + 1, id + 2, id + 3, id + 4);
    }

    public void testAFewNullsInTheMiddle()
    {
        final SingleThreadedBlockMap map = new SingleThreadedBlockMap();
        addBlock( map, 0 );
        addBlock( map, 1 );
        for ( int i = 3; i < 2000; ++i )
            addBlock( map, i );

        assertEquals( 1999, map.size() );
        checkPresent(map, 0, 1);
        checkAbsent( map, 2 );
        for ( int i = 3; i < 2000; ++i )
            checkPresent(map, i);
    }

    public void testManyNullsInTheMiddle()
    {
        final SingleThreadedBlockMap map = new SingleThreadedBlockMap();
        addBlock( map, 0 );
        addBlock(map, 1);
        for ( int i = 30; i < 2000; ++i )
            addBlock( map, i );

        assertEquals(1972, map.size());
        checkPresent(map, 0, 1);
        for ( int i = 2; i < 30; ++i )
            checkAbsent( map, i );
        for ( int i = 30; i < 2000; ++i )
            checkPresent( map, i );
    }

    private void checkPresent( final SingleThreadedBlockMap map, final int... ids )
    {
        for ( int id : ids )
        {
            assertNotNull( map.get( id ) );
            assertEquals( id , map.get( id ).getIndex() );
        }
    }

    private void checkAbsent( final SingleThreadedBlockMap map, final int... ids )
    {
        for ( int id : ids )
            assertNull( map.get(id) );
    }

    private void addBlock( final SingleThreadedBlockMap map, final int index )
    {
        map.put(index, new SingleThreadedBlock(null, index, 1));
    }

}
