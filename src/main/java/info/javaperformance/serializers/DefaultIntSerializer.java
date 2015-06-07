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
public final class DefaultIntSerializer implements IIntSerializer
{
    public static final IIntSerializer INSTANCE = new DefaultIntSerializer();

    private DefaultIntSerializer(){}

    @Override
    public void write(int v, ByteArray buf) {
        VarLen.writeSignedInt( v, buf );
    }

    @Override
    public int read(ByteArray buf) {
        return VarLen.readSignedInt( buf );
    }

    @Override
    public void writeDelta(int prevValue, int curValue, ByteArray buf, boolean sorted) {
        if ( sorted )
            VarLen.writeUnsignedInt( curValue - prevValue, buf );
        else
            VarLen.writeSignedInt( curValue - prevValue, buf );
    }

    @Override
    public int readDelta(int prevValue, ByteArray buf, boolean sorted) {
        return prevValue + (sorted ? VarLen.readUnsignedInt( buf ) : VarLen.readSignedInt(buf));
    }

    @Override
    public int getMaxLength() {
        return 5;
    }
}
