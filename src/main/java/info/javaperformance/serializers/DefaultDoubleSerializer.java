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

import info.javaperformance.tools.VarLen;

/**
 * Default serialization using varlen encoding
 */
public final class DefaultDoubleSerializer implements IDoubleSerializer
{
    public static final IDoubleSerializer INSTANCE = new DefaultDoubleSerializer();

    private DefaultDoubleSerializer(){}

    @Override
    public void write( final double v, final ByteArray buf ) {
        VarLen.writeDouble( v, buf );
    }

    @Override
    public double read( final ByteArray buf ) {
        return VarLen.readDouble( buf );
    }

    @Override
    public void writeDelta( final double prevValue, final double curValue, final ByteArray buf, final boolean sorted ) {
        write( curValue, buf );
    }

    @Override
    public double readDelta( final double prevValue, final ByteArray buf, final boolean sorted ) {
        return read( buf );
    }

    @Override
    public int getMaxLength() {
        return 8;
    }
}
