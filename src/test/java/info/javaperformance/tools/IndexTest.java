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

import junit.framework.TestCase;

public class IndexTest extends TestCase
{
    public void testOver16M()
    {
        final Index idx = new Index( 100 );
        idx.set(0, 100);
        idx.set(1, 101);
        assertEquals(100, idx.get(0));
        assertEquals(101, idx.get(1));
        assertTrue(idx.isEmpty(2));
        assertTrue( idx.isEmpty( 3 ));

        idx.set(2, 100 * 1000 * 1000);
        assertEquals( 100, idx.get(0));
        assertEquals( 101, idx.get( 1 ));
        assertEquals( 100 * 1000 * 1000, idx.get( 2 ));
        assertTrue( idx.isEmpty( 3 ));
    }

    public void testEmptyFlag()
    {
        final Index idx = new Index( 100 );
        idx.set(1, 100);
        idx.set(2, 0xFF0000 ); //fine 3 byte value
        idx.set(3, 0xFFFFFF ); //fine 3 byte value

        assertTrue(idx.isEmpty(0));
        assertFalse(idx.isEmpty(1));
        assertFalse(idx.isEmpty(2));
        assertFalse(idx.isEmpty(3));
        assertTrue(idx.isEmpty(4));
    }
}
