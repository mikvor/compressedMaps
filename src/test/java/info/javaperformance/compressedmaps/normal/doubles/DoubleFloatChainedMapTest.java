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

package info.javaperformance.compressedmaps.normal.doubles;

import info.javaperformance.compressedmaps.DoubleMapFactory;
import junit.framework.TestCase;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class DoubleFloatChainedMapTest extends TestCase
{
    //fill factors to be tested
    private final static float[] FILL_FACTORS = { 0.25f, 0.5f, 0.75f, 0.9f, 0.99f, 1f, 2f, 3f, 5f, 16f };
    private final int SIZE = 100000;
    private static final float NOT_PRESENT = 0;

    protected IDoubleFloatMap makeMap( final long size, final float fillFactor )
    {
        return DoubleMapFactory.singleThreadedDoubleFloatMap( size, fillFactor );
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
        final IDoubleFloatMap map = makeMap(100, fillFactor);
        for ( int i = 0; i < SIZE; ++i )
        {
            assertEquals( NOT_PRESENT, map.put( i, i ) );

            assertEquals( i + 1, map.size() );
            assertEquals( ( float )i, map.get( i ) );
        }
        //now check the final state
        assertEquals( SIZE, map.size() );
        for ( int i = 0; i < SIZE; ++i )
            assertEquals(  ( float )i, map.get( i ));
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
        final IDoubleFloatMap map = makeMap(100, fillFactor);
        for ( int i = 0; i < SIZE; ++i )
        {
            map.put( -i, -i);
            assertEquals( i + 1, map.size() );
            assertEquals(  ( float )( -i ), map.get( -i ));
        }
        //now check the final state
        assertEquals(SIZE, map.size());
        for ( int i = 0; i < SIZE; ++i )
            assertEquals(  ( float )( -i ), map.get( -i ) );
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
        final IDoubleFloatMap map = makeMap(100, fillFactor);
        for ( int i = 0; i < SIZE; ++i )
        {
            map.put( i, i );
            assertEquals( i + 1, map.size());
            assertEquals( ( float )i, map.get( i ));
        }
        //now check the initial state
        assertEquals(SIZE, map.size());
        for ( int i = 0; i < SIZE; ++i )
            assertEquals( ( float )i, map.get( i ));

        //now try to update all keys
        for ( int i = 0; i < SIZE; ++i )
        {
            map.put( i, i + 1 );
            assertEquals( SIZE, map.size() );
            assertEquals( ( float )( i + 1 ), map.get( i ));
        }
        //and check the final state
        for ( int i = 0; i < SIZE; ++i )
            assertEquals( ( float )( i + 1 ), map.get( i ));
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
        final Set<Double> set = new HashSet<>( SIZE );
        final double[] vals = new double[ SIZE ];
        while ( set.size() < SIZE )
            set.add( r.nextDouble() );
        int i = 0;
        for ( final Double v : set )
            vals[ i++ ] = v;
        final IDoubleFloatMap map = makeMap(100, fillFactor);
        for ( i = 0; i < vals.length; ++i )
        {
            assertEquals( NOT_PRESENT, map.put( vals[i], (float)vals[i]  ) );
            assertEquals( i + 1, map.size());
            assertEquals( (float)vals[ i ], map.get( vals[ i ] ));
        }
        //now check the final state
        assertEquals( SIZE, map.size() );
        for ( i = 0; i < vals.length; ++i )
            assertEquals( (float)vals[ i ], map.get( vals[ i ] ) );
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
        final IDoubleFloatMap map = makeMap(100, fillFactor);
        int addCnt = 0, removeCnt = 0;
        for ( int i = 0; i < SIZE; ++i )
        {
            assertEquals( NOT_PRESENT, map.put( addCnt, addCnt ) );
            assertEquals( i + 1, map.size() );
            addCnt++;

            assertEquals( NOT_PRESENT, map.put( addCnt, addCnt ) );
            assertEquals( i + 2, map.size() ); //map grows by one element on each iteration
            addCnt++;

            assertEquals( (float)removeCnt, map.remove(removeCnt));
            removeCnt++;

            assertEquals( i + 1, map.size()); //map grows by one element on each iteration
        }

        assertEquals( SIZE, map.size() );
        for ( int i = removeCnt; i < addCnt; ++i )
            assertEquals( ( float )i, map.get( i ) );
    }

    public void testRandomRemove()
    {
        for ( final float ff: FILL_FACTORS )
            testRandomRemoveHelper( ff );
    }

    private void testRandomRemoveHelper( final float ff )
    {
        final Random r = new Random( 1 );
        final float[] values = new float[ SIZE ];
        Set<Double> ks = new HashSet<>( SIZE );
        while ( ks.size() < SIZE )
            ks.add( r.nextDouble() );
        final Double[] keys = ks.toArray( new Double[ SIZE ] );
        ks = null;

        assertEquals(SIZE, keys.length);

        for ( int i = 0; i < SIZE; ++i )
            values[ i ] = r.nextFloat();

        IDoubleFloatMap m = makeMap( 100, ff );
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
