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

package info.javaperformance.compressedmaps.normal.floats;

import info.javaperformance.compressedmaps.FloatMapFactory;
import junit.framework.TestCase;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class FloatIntChainedMapTest extends TestCase
{
    //fill factors to be tested
    private final static float[] FILL_FACTORS = { 0.25f, 0.5f, 0.75f, 0.9f, 0.99f, 1f, 2f, 3f, 5f, 16f };
    private final int SIZE = 100000;
    private static final int NOT_PRESENT = 0;

    protected IFloatIntMap makeMap( final long size, final float fillFactor )
    {
        return FloatMapFactory.singleThreadedFloatIntMap( size, fillFactor );
    }

    /**
     * Add keys 0-SIZE to the map
     */
    public void testPut()
    {
        for ( final float ff : FILL_FACTORS )
            testPutHelper( ff );
    }

    private void testPutHelper( final float fillFactor )
    {
        final IFloatIntMap map = makeMap(100, fillFactor);
        for ( int i = 0; i < SIZE; ++i )
        {
            assertEquals( NOT_PRESENT, map.put( i, i ) );

            assertEquals( i + 1, map.size() );
            assertEquals( ( int )i, map.get( i ) );
        }
        //now check the final state
        assertEquals( SIZE, map.size() );
        for ( int i = 0; i < SIZE; ++i )
            assertEquals(  ( int )i, map.get( i ));
    }

    /**
     * Add a series of negative keys to the map
     */
    public void testPutNegative()
    {
        for ( final float ff : FILL_FACTORS )
            testPutNegative( ff );
    }

    private void testPutNegative( final float fillFactor )
    {
        final IFloatIntMap map = makeMap(100, fillFactor);
        for ( int i = 0; i < SIZE; ++i )
        {
            map.put( -i, -i);
            assertEquals( i + 1, map.size() );
            assertEquals(  ( int )( -i ), map.get( -i ));
        }
        //now check the final state
        assertEquals(SIZE, map.size());
        for ( int i = 0; i < SIZE; ++i )
            assertEquals(  ( int )( -i ), map.get( -i ) );
    }

    /**
     * Add a set of keys to the map. Then add it again to test update operations.
     */
    public void testPutThenUpdate()
    {
        for ( final float ff : FILL_FACTORS )
            testPutThenUpdate( ff );
    }

    private void testPutThenUpdate( final float fillFactor )
    {
        final IFloatIntMap map = makeMap( 100, fillFactor );
        for ( int i = 0; i < SIZE; ++i )
        {
            assertEquals( NOT_PRESENT, map.put( i, i ) );
            assertEquals( i + 1, map.size() );
            assertEquals( ( int )i, map.get( i ) );
        }
        //now check the initial state
        assertEquals( SIZE, map.size() );
        for ( int i = 0; i < SIZE; ++i )
            assertEquals( ( int )i, map.get( i ) );

        //now try to update all keys
        for ( int i = 0; i < SIZE; ++i )
        {
            assertEquals( ( int )i, map.put( i, i + 1 ) );
            assertEquals( SIZE, map.size() );
            assertEquals( ( int )( i + 1 ), map.get( i ) );
        }
        //and check the final state
        for ( int i = 0; i < SIZE; ++i )
            assertEquals( ( int )( i + 1 ), map.get( i ) );
    }

    /**
     * Add random keys to the map. We use random seeds for the random generator (each test run is unique), so we log
     * the seeds used to initialize Random-s.
     */
    public void testPutRandom()
    {
        for ( final float ff : FILL_FACTORS )
            testPutRandom( ff );
    }

    private void testPutRandom( final float fillFactor )
    {
        final int seed = ThreadLocalRandom.current().nextInt();
        System.out.println( "testPutRandom: ff = " + fillFactor + ", seed = " + seed);
        final Random r = new Random( seed );
        final Set<Float> set = new LinkedHashSet<>( SIZE );
        final float[] vals = new float[ SIZE ];
        while ( set.size() < SIZE )
            set.add( r.nextFloat() );
        int i = 0;
        for ( final Float v : set )
            vals[ i++ ] = v;
        final IFloatIntMap map = makeMap(100, fillFactor);
        for ( i = 0; i < vals.length; ++i )
        {
            assertEquals( NOT_PRESENT, map.put( vals[i], (int)vals[i]  ) );
            assertEquals( i + 1, map.size());
            assertEquals( (int)vals[ i ], map.get( vals[ i ] ));
        }
        //now check the final state
        assertEquals( SIZE, map.size() );
        for ( i = 0; i < vals.length; ++i )
            assertEquals( (int)vals[ i ], map.get( vals[ i ] ) );
    }

    /**
     * Interleaved put and remove operations - we remove half of added entries
     */
    public void testRemove()
    {
        for ( final float ff : FILL_FACTORS )
            testRemoveHelper( ff );
    }

    private void testRemoveHelper( final float fillFactor )
    {
        final IFloatIntMap map = makeMap(100, fillFactor);
        int addCnt = 0, removeCnt = 0;
        for ( int i = 0; i < SIZE; ++i )
        {
            assertEquals( NOT_PRESENT, map.put( addCnt, addCnt ) );
            assertEquals( i + 1, map.size() );
            addCnt++;

            assertEquals( NOT_PRESENT, map.put( addCnt, addCnt ) );
            assertEquals( i + 2, map.size() ); //map grows by one element on each iteration
            addCnt++;

            assertEquals( (int)removeCnt, map.remove(removeCnt));
            removeCnt++;

            assertEquals( i + 1, map.size()); //map grows by one element on each iteration
        }

        assertEquals( SIZE, map.size() );
        for ( int i = removeCnt; i < addCnt; ++i )
            assertEquals( ( int )i, map.get( i ) );
    }

    public void testRandomRemove()
    {
        for ( final float ff: FILL_FACTORS )
            testRandomRemoveHelper( ff );
    }

    private void testRandomRemoveHelper( final float ff )
    {
        final Random r = new Random( 1 );
        final int[] values = new int[ SIZE ];
        Set<Float> ks = new LinkedHashSet<>( SIZE );
        while ( ks.size() < SIZE )
            ks.add( r.nextFloat() );
        final Float[] keys = ks.toArray( new Float[ SIZE ] );
        ks = null;

        assertEquals( SIZE, keys.length );

        for ( int i = 0; i < SIZE; ++i )
            values[ i ] = r.nextInt();

        IFloatIntMap m = makeMap( 100, ff );
        int add = 0, remove = 0;
        while ( add < SIZE )
        {
            assertEquals( NOT_PRESENT, m.put( keys[ add ], values[ add ] ) );
            ++add;
            assertEquals( NOT_PRESENT, m.put( keys[ add ], values[ add ] ) );
            ++add;

            assertEquals( values[ remove ], m.remove( keys[ remove ] ) );
            remove++;

            assertEquals( remove, m.size() );
        }

        assertEquals( SIZE / 2, m.size() );

        for ( int i = 0; i < SIZE / 2; ++i )
            assertEquals( NOT_PRESENT, m.get( keys[ i ] ) );
        for ( int i = SIZE / 2; i < SIZE; ++i )
            assertEquals( values[ i ], m.get( keys[ i ] ) );
    }

}
