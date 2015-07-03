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
 */
public class GenericStringSerializer implements IObjectSerializer<String> {
    private final int m_maxBytesPerChar;
    private final Charset m_charset;
    private final ThreadLocal<CharsetInfo> m_state = new ThreadLocal<CharsetInfo>(){
        @Override
        protected CharsetInfo initialValue() {
            return new CharsetInfo(
                m_charset.newEncoder().onMalformedInput( CodingErrorAction.REPLACE ).onUnmappableCharacter( CodingErrorAction.REPLACE ),
                m_charset.newDecoder().onMalformedInput( CodingErrorAction.REPLACE ).onUnmappableCharacter( CodingErrorAction.REPLACE )
            );
        }
    };

    public GenericStringSerializer( final Charset charset ) {
        m_charset = charset;
        m_maxBytesPerChar = ( int ) Math.ceil( m_charset.newEncoder().maxBytesPerChar() );
    }

    @Override
    public void write( final String v, final ByteArray buf ) {
        if ( v == null )
            VarLen.writeSignedInt( -1, buf );
        else if ( v.isEmpty() )
            VarLen.writeSignedInt( 0, buf );
        else
        {
            final CharsetInfo ci = m_state.get();
            //extend buffers if needed
            ci.ensureBufferSize( v.length() * m_maxBytesPerChar, v.length() );
            //put a string in the input buffer
            ci.addString( v );
            //and convert it
            ci.convertStringToByteArray( buf );
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
                final CharsetInfo ci = m_state.get();
                //extend buffers if needed
                ci.ensureBufferSize( len, len );
                //fill the input byte buffer with the data
                ci.addBytes( buf.array(), buf.position(), len );
                buf.position( buf.position() + len );
                //now convert data
                return ci.convertToString();
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

    private static class CharsetInfo
    {
        public final CharsetEncoder m_encoder;
        public final CharsetDecoder m_decoder;
        public ByteBuffer m_ar = ByteBuffer.allocate( 64 );
        public CharBuffer m_chars = CharBuffer.allocate( 64 );

        public CharsetInfo( CharsetEncoder m_encoder, CharsetDecoder m_decoder ) {
            this.m_encoder = m_encoder;
            this.m_decoder = m_decoder;
        }

        public void ensureBufferSize( final int bytesRequired, final int charsRequired )
        {
            if ( bytesRequired > m_ar.capacity() )
                m_ar = ByteBuffer.allocate( bytesRequired );
            if ( charsRequired > m_chars.capacity() )
                m_chars = CharBuffer.allocate( charsRequired );
        }

        public void addBytes( final byte[] ar, final int offset, final int len )
        {
            m_ar.clear();
            m_ar.put( ar, offset, len );
            m_ar.flip();
        }

        public void addString( final String v )
        {
            m_chars.clear();
            for ( int i = 0; i < v.length(); ++i )
                m_chars.put( v.charAt( i ) );
            m_chars.flip();
        }

        public String convertToString()
        {
            m_chars.clear();
            m_decoder.decode( m_ar, m_chars, true );
            m_chars.flip();
            return m_chars.toString();
        }

        public void convertStringToByteArray( final ByteArray buf )
        {
            m_ar.clear();
            m_encoder.reset().encode( m_chars, m_ar, true );

            VarLen.writeSignedInt( m_ar.position(), buf );
            buf.put( m_ar.array(), 0, m_ar.position() );
        }
    }
}
