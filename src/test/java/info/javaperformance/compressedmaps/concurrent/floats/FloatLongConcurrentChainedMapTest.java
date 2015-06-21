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

package info.javaperformance.compressedmaps.concurrent.floats;

import junit.framework.TestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class FloatLongConcurrentChainedMapTest extends TestCase
{
    private static final int PUT_MAP_SIZE = 1 * 1000 * 1000;
    private static final int INITIAL_CAPACITY = 1;
    private static final long ZERO = 0;
    private static final long ONE = 1;
    private static final long TWO = 2;

    private static final float[] FF = { 0.5f, 1, 5 };
    private static final int[] THREADS = { 1, 2, 4, 8, 16, 32 };


    protected IFloatLongConcurrentMap getMap( final int size, final float ff )
    {
        return new FloatLongConcurrentChainedMap( size, ff );
    }

    /*
    Simple multithreaded insertion
     */
    public void testPut() throws InterruptedException {
        for ( int threads : THREADS )
            for ( float ff : FF )
                testPutHelper( threads, ff );
    }

    private void testPutHelper( final int threads, final float ff ) throws InterruptedException {
        System.out.println( "Running testPutHelper( threads = " + threads +  ", ff = " + ff + " )" );
        final IFloatLongConcurrentMap map = getMap( INITIAL_CAPACITY, ff );
        final int SECTION = PUT_MAP_SIZE / threads;
        //initial insertion section
        {
            final CountDownLatch start = new CountDownLatch(threads);
            final CountDownLatch end = new CountDownLatch(threads);
            for (int i = 0; i < threads; ++i) {
                final Thread t = new Thread(new Adder(i * SECTION, (i + 1) * SECTION, start, end, map));
                t.start();
            }
            //wait for the completion
            end.await(100, TimeUnit.SECONDS); //more than enough, needed if one of threads dies

            //now check the final state
            assertEquals(SECTION * threads, map.size());
            for (float n = 0; n < SECTION * threads; ++n)
                assertEquals( ( long ) n, map.get( n ) );
        }
        //update section
        {
            final CountDownLatch start = new CountDownLatch(threads);
            final CountDownLatch end = new CountDownLatch(threads);
            for (int i = 0; i < threads; ++i) {
                final Thread t = new Thread(new Updater(i * SECTION, (i + 1) * SECTION, start, end, map));
                t.start();
            }
            //wait for the completion
            end.await(100, TimeUnit.SECONDS); //more than enough, needed if one of threads dies

            //now check the final state
            assertEquals(SECTION * threads, map.size());
            for (float n = 0; n < SECTION * threads; ++n)
                assertEquals( ONE, map.get( n ) );
        }
    }

    private static class Adder implements Runnable
    {
        private final float m_from;
        private final float m_to;
        private final CountDownLatch m_startGate;
        private final CountDownLatch m_endGate;
        private final IFloatLongConcurrentMap m_map;

        public Adder( float from, float to, CountDownLatch startGate, CountDownLatch endGate, IFloatLongConcurrentMap map ) {
            m_from = from;
            m_to = to;
            m_startGate = startGate;
            m_endGate = endGate;
            m_map = map;
        }

        @Override
        public void run() {
            try {
                m_startGate.countDown();
                m_startGate.await();
                for ( float n = m_from; n < m_to; ++n ) {
                    assertEquals( ZERO, m_map.put( n, (long) n ) );
                    assertEquals( ( long ) n, m_map.get( n ) );
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
            m_endGate.countDown();
        }
    }

    private static class Updater implements Runnable
    {
        private final float m_from;
        private final float m_to;
        private final CountDownLatch m_startGate;
        private final CountDownLatch m_endGate;
        private final IFloatLongConcurrentMap m_map;

        public Updater( float from, float to, CountDownLatch startGate, CountDownLatch endGate, IFloatLongConcurrentMap map ) {
            m_from = from;
            m_to = to;
            m_startGate = startGate;
            m_endGate = endGate;
            m_map = map;
        }

        @Override
        public void run() {
            try {
                m_startGate.countDown();
                m_startGate.await();
                for ( float n = m_from; n < m_to; ++n ) {
                    assertEquals( (long) n, m_map.put(n, ONE));
                    assertEquals( ONE, m_map.get( n ) );
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
            m_endGate.countDown();
        }
    }

    /*
    Thread N adds a set of entries, updates them and removes half of its own values.
    We can not remove other thread entries because they may not be present in the map at that time.
     */
    public void testAddUpdateRemove() throws InterruptedException {
        for ( int threads : THREADS )
            for ( float ff : FF )
                testAddUpdateRemoveHelper( threads, ff );
    }

    private void testAddUpdateRemoveHelper( final int threads, final float ff ) throws InterruptedException {
        System.out.println( "Running testAddUpdateRemoveHelper( threads = " + threads + ", ff = " + ff + " )" );
        final IFloatLongConcurrentMap map = getMap( INITIAL_CAPACITY, ff );
        final int SECTION = PUT_MAP_SIZE / threads;
        {
            final CountDownLatch start = new CountDownLatch( threads );
            final CountDownLatch end = new CountDownLatch( threads );
            for (int i = 0; i < threads; ++i) {
                final Thread t = new Thread(new AddUpdateRemover(i * SECTION, (i + 1) * SECTION, start, end, map));
                t.start();
            }
            //wait for the completion
            end.await(100, TimeUnit.SECONDS); //more than enough, needed if one of threads dies
        }

        //now check the final state
        int totalSize = 0;
        for ( int i = 0; i < threads; ++i )
        {
            //emulate counters
            float add = i * SECTION, remove = add;
            while ( add < ( i + 1 ) * SECTION )
            {
                add += 2;
                remove++;
            }

            totalSize += add - remove;
            for ( float j = remove; j < add; ++j )
                assertEquals( "Failed for j = " + j, TWO, map.get( j ) );
        }
        assertEquals( totalSize, map.size());

        //now remove everything twice
        {
            final CountDownLatch start = new CountDownLatch( threads );
            final CountDownLatch end = new CountDownLatch( threads );
            for (int i = 0; i < threads; ++i) {
                final Thread t = new Thread(new Remover(i * SECTION, i == threads - 1 ? PUT_MAP_SIZE : (i + 1) * SECTION, start, end, map));
                t.start();
            }
            //wait for the completion
            end.await(100, TimeUnit.SECONDS); //more than enough, needed if one of threads dies
        }

        assertEquals( 0, map.size() );
    }

    private static class AddUpdateRemover implements Runnable
    {
        private final float m_from;
        private final float m_to;
        private final CountDownLatch m_startGate;
        private final CountDownLatch m_endGate;
        private final IFloatLongConcurrentMap m_map;

        public AddUpdateRemover( float from, float to, CountDownLatch startGate, CountDownLatch endGate, IFloatLongConcurrentMap map ) {
            m_from = from;
            m_to = to;
            m_startGate = startGate;
            m_endGate = endGate;
            m_map = map;
        }

        @Override
        public void run() {
            try {
                m_startGate.countDown();
                m_startGate.await();
                float add = m_from, remove = m_from;
                while ( add < m_to )
                {
                    assertEquals( ZERO, m_map.put( add, ONE ) );
                    assertEquals( ONE, m_map.get( add++ ) );
                    assertEquals( ZERO, m_map.put( add, ONE ) );
                    assertEquals( ONE, m_map.get( add++ ) );
                    final long preRemove = m_map.get( remove );
                    final long removeRes = m_map.remove(remove++);
                    if ( removeRes != ONE )
                        fail( "failed for remove = " + ( remove - 1 ) + ", map.get(" + (remove - 1) + ") = " + m_map.get( remove-1) + ", preRemove = " + preRemove + ", removeRes = " + removeRes );
                }

                for ( float n = remove; n < add; ++n ) {
                    assertEquals( "failed for n = " + n, ONE, m_map.put( n, TWO ) );
                    assertEquals( TWO, m_map.get( n ) );
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
            m_endGate.countDown();
        }
    }

    private static class Remover implements Runnable
    {
        private final float m_from;
        private final float m_to;
        private final CountDownLatch m_startGate;
        private final CountDownLatch m_endGate;
        private final IFloatLongConcurrentMap m_map;

        public Remover( float from, float to, CountDownLatch startGate, CountDownLatch endGate, IFloatLongConcurrentMap map ) {
            m_from = from;
            m_to = to;
            m_startGate = startGate;
            m_endGate = endGate;
            m_map = map;
        }

        @Override
        public void run() {
            try {
                m_startGate.countDown();
                m_startGate.await();
                for ( float n = m_from; n < m_to; ++n )
                    m_map.remove( n );
                for ( float n = m_from; n < m_to; ++n )
                    assertEquals( ZERO, m_map.remove( n ) );
            } catch (Throwable e) {
                e.printStackTrace();
            }
            m_endGate.countDown();
        }
    }
}
