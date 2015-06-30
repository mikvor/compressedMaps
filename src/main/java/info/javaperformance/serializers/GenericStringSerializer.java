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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

/**
 * String serializer accepting an encoding as an argument
 * todo This is not a thread safe class! Thread safe version will be added once objects support will be added to MT maps.
 */
public class GenericStringSerializer implements IObjectSerializer<String> {
    private final int m_maxBytesPerChar;
    private final CharsetEncoder m_encoder;
    private final CharsetDecoder m_decoder;
    private ByteBuffer m_ar = ByteBuffer.allocate( 64 );
    private CharBuffer m_chars = CharBuffer.allocate( 64 );

    public GenericStringSerializer( final Charset charset ) {
        m_encoder = charset.newEncoder().onMalformedInput( CodingErrorAction.REPLACE ).onUnmappableCharacter( CodingErrorAction.REPLACE );
        m_decoder = charset.newDecoder().onMalformedInput( CodingErrorAction.REPLACE ).onUnmappableCharacter( CodingErrorAction.REPLACE );
        m_maxBytesPerChar = ( int ) Math.ceil( m_encoder.maxBytesPerChar() );
    }

    @Override
    public void write( final String v, final ByteArray buf ) {
        if ( v == null )
            VarLen.writeSignedInt( -1, buf );
        else if ( v.isEmpty() )
            VarLen.writeSignedInt( 0, buf );
        else
        {
            //extend buffers if needed
            if ( v.length() * m_maxBytesPerChar > m_ar.capacity() )
                m_ar = ByteBuffer.allocate( v.length() * m_maxBytesPerChar );
            if ( v.length() > m_chars.capacity() )
                m_chars = CharBuffer.allocate( v.length() );
            //put a string in the input buffer
            m_chars.clear();
            for ( int i = 0; i < v.length(); ++i )
                m_chars.put( v.charAt( i ) );
            m_chars.flip();
            m_ar.clear();
            //and convert it
            m_encoder.reset().encode( m_chars, m_ar, true );

            VarLen.writeSignedInt( m_ar.position(), buf );
            buf.put( m_ar.array(), 0, m_ar.position() );
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
                //extend buffers if needed
                if ( m_ar.capacity() < len )
                    m_ar = ByteBuffer.allocate( len );
                if ( m_chars.capacity() < len ) //we can not create more than 1 char out of 1 byte
                    m_chars = CharBuffer.allocate( len );
                //fill the input byte buffer with the data
                m_ar.clear();
                m_ar.put( buf.array(), buf.position(), len );
                m_ar.flip();
                buf.position( buf.position() + len );
                //now convert data
                m_chars.clear();
                m_decoder.decode( m_ar, m_chars, true );
                m_chars.flip();
                return m_chars.toString();
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
