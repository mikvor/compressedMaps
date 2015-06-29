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

import java.nio.charset.Charset;

/**
 * String serializer accepting an encoding as an argument
 */
public class GenericStringSerializer implements IObjectSerializer<String> {
    private final Charset m_charset;
    private final int m_maxBytesPerChar;

    public GenericStringSerializer( final Charset charset ) {
        m_charset = charset;
        m_maxBytesPerChar = ( int ) Math.ceil( charset.newEncoder().maxBytesPerChar() );
    }

    @Override
    public void write( final String v, final ByteArray buf ) {
        if ( v == null )
            VarLen.writeSignedInt( -1, buf );
        else if ( v.isEmpty() )
            VarLen.writeSignedInt( 0, buf );
        else
        {
            final byte[] data = v.getBytes( m_charset );
            VarLen.writeSignedInt( data.length, buf );
            buf.put( data, 0, data.length );
        }
    }

    @Override
    public String read( final ByteArray buf ) {
        final int len = VarLen.readSignedInt( buf );
        switch ( len )
        {
            case -1:
                return null;
            case 0:
                return "";
            default:
                final byte[] data = new byte[ len ];
                buf.get( data, 0, data.length );
                return new String( data, m_charset );
        }
    }

    @Override
    public void skip( final ByteArray buf ) {
        final int len = VarLen.readSignedInt( buf );
        if ( len > 0 )
            buf.position( buf.position() + len );
    }

    @Override
    public int getMaxLength( final String obj ) {
        return obj == null || obj.isEmpty() ? 1 : ( 5 + obj.length() * m_maxBytesPerChar );
    }
}
