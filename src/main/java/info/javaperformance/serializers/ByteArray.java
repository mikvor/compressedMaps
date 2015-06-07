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

/**
 * Byte array wrapper.
 * The main reason for introduction of this class is the fact that you can not reuse a JDK ByteBuffer on different
 * underlying byte arrays.
 * The concurrent map implementation has to iterate the buffers with a read lock on, which requires us duplicating
 * the original byte buffer. As a result, we had to allocate objects even in the 'get' method, which is rather unacceptable.
 */
public class ByteArray {
    private byte[] m_buf;
    private int m_ptr;

    public ByteArray()
    {
    }

    public ByteArray( final byte[] ar )
    {
        m_buf = ar;
    }

    public ByteArray( final int size )
    {
        m_buf = new byte[ size ];
    }

    public ByteArray reset( final byte[] buf )
    {
        m_buf = buf;
        return this;
    }

    public ByteArray reset( final byte[] buf, final int offset )
    {
        m_buf = buf;
        m_ptr = offset;
        return this;
    }

    public void clear()
    {
        m_buf = null;
    }

    public byte get()
    {
        return m_buf[m_ptr++];
    }

    public void put( final byte v )
    {
        m_buf[ m_ptr++ ] = v;
    }

    public byte[] array()
    {
        return m_buf;
    }

    public void position( final int offset )
    {
        m_ptr = offset;
    }

    public int position()
    {
        return m_ptr;
    }

    public void put( final byte[] ar, final int offset, final int length )
    {
        System.arraycopy( ar, offset, m_buf, m_ptr, length );
        m_ptr += length;
    }
}
