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

package info.javaperformance.buckets;

import junit.framework.TestCase;

public class BucketsTest extends TestCase {
    public void testSimple()
    {
        final Buckets buckets = new Buckets( 1000, false );
        for ( int i = 0; i < buckets.length(); i += 2 )
        {
            buckets.set( i, i, 1, 2 );
        }

        for ( int i = 0; i < buckets.length(); ++i )
        {
            if ( i % 2 != 0 )
                assertFalse( buckets.select( i ) );
            else
            {
                assertTrue( buckets.select( i ) );
                assertEquals( i, buckets.getBlockIndex() );
                assertEquals( 1, buckets.getOffset() );
                assertEquals( 2, buckets.getBlockLength() );
            }
        }
    }

    public void testIntToLongMigration()
    {
        final Buckets buckets = new Buckets( 100, false );
        buckets.set( 0, 1, 0, 2 ); //block idx = 1
        assertEquals( IntBucketEncoding.MAX_ENCODED_LENGTH, buckets.maxEncodedLength() ); //check we are still in int mode
        buckets.set( 1, 2, 0, buckets.maxEncodedLength() ); //block idx = 2, int max length is used here
        buckets.set( 2, 1000 * 1000, 0, buckets.maxEncodedLength() ); //block idx = 1M, check that int->long max length is correctly migrated here
        buckets.set( 3, 1000 * 1000 + 1, 0, 2 ); //block idx = 1M + 1

        assertTrue( buckets.select( 0 ) );
        assertEquals( 1, buckets.getBlockIndex() );
        assertEquals( 0, buckets.getOffset() );
        assertEquals( 2, buckets.getBlockLength() );

        assertTrue( buckets.select( 1 ) );
        assertEquals( 2, buckets.getBlockIndex() );
        assertEquals( 0, buckets.getOffset() );
        assertEquals( LongBucketEncoding.MAX_ENCODED_LENGTH, buckets.getBlockLength() );

        assertTrue( buckets.select( 2 ) );
        assertEquals( 1000 * 1000, buckets.getBlockIndex() );
        assertEquals( 0, buckets.getOffset() );
        assertEquals( LongBucketEncoding.MAX_ENCODED_LENGTH, buckets.getBlockLength() );

        assertTrue( buckets.select( 3 ) );
        assertEquals( 1000 * 1000 + 1, buckets.getBlockIndex() );
        assertEquals( 0, buckets.getOffset() );
        assertEquals( 2, buckets.getBlockLength() );
    }
}
