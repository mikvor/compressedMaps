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

package info.javaperformance.serializers;

import junit.framework.TestCase;

import java.math.BigDecimal;

public class MoneyDoubleSerializerTest extends TestCase {
    public void testSimple()
    {
        final ByteArray bar = new ByteArray(  ).reset( new byte[ 1000 ] );
        for ( int point = 0; point <= 5; ++point ) {
            for ( int digits = 0; digits < MoneyDoubleSerializer.MAX_ALLOWED_PRECISION; ++digits ) {
                final MoneyDoubleSerializer serializer = new MoneyDoubleSerializer( digits );
                for ( int i = -100000; i <= 100000; ++i ) {
                    bar.position( 0 );
                    final BigDecimal bd = new BigDecimal( i ).movePointLeft( point );
                    final double v1 = bd.doubleValue();
                    serializer.write( v1, bar );

                    final double v2 = bd.multiply( new BigDecimal( 3 ) ).doubleValue();
                    serializer.writeDelta( v1, v2, bar, i > 0 );

                    final double v3 = bd.multiply( new BigDecimal( 12 ) ).doubleValue();
                    serializer.writeDelta( v2, v3, bar, i > 0 );

                    bar.position( 0 );
                    assertEquals( v1, serializer.read( bar ) );
                    assertEquals( v2, serializer.readDelta( v1, bar, i > 0 ) );
                    assertEquals( v3, serializer.readDelta( v2, bar, i > 0 ) );
                }
            }
        }
    }
}
