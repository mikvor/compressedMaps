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

import info.javaperformance.serializers.ByteArray;

/**
 * Variable length encoding methods.
 * Some of these methods were adopted from Google Protobuf implementation.
 */
public class VarLen {

/**
  * Some functionality extracted from Google Protobuf and updated to operate on byte buffers.
  * Protobuf license:
  *
  // Protocol Buffers - Google's data interchange format
  // Copyright 2008 Google Inc.  All rights reserved.
  // https://developers.google.com/protocol-buffers/
  //
  // Redistribution and use in source and binary forms, with or without
  // modification, are permitted provided that the following conditions are
  // met:
  //
  //     * Redistributions of source code must retain the above copyright
  // notice, this list of conditions and the following disclaimer.
  //     * Redistributions in binary form must reproduce the above
  // copyright notice, this list of conditions and the following disclaimer
  // in the documentation and/or other materials provided with the
  // distribution.
  //     * Neither the name of Google Inc. nor the names of its
  // contributors may be used to endorse or promote products derived from
  // this software without specific prior written permission.
  //
  // THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  // "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  // LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  // A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  // OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  // SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  // LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  // DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  // THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  // (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  // OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */

    /**
     * Encode a ZigZag-encoded 32-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n A signed 32-bit integer.
     * @return An unsigned 32-bit integer, stored in a signed int because
     *         Java has no explicit unsigned support.
     */
    static int encodeZigZag32(final int n) {
      // Note:  the right-shift must be arithmetic
      return (n << 1) ^ (n >> 31);
    }

    /**
     * Encode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n A signed 64-bit integer.
     * @return An unsigned 64-bit integer, stored in a signed int because
     *         Java has no explicit unsigned support.
     */
    static long encodeZigZag64(final long n) {
      // Note:  the right-shift must be arithmetic
      return (n << 1) ^ (n >> 63);
    }

    public static void writeSignedLong( final long v, final ByteArray buf )
    {
        writeRawVarint64( encodeZigZag64( v ), buf );
    }

    public static void writeSignedInt( final int v, final ByteArray buf )
    {
        writeRawVarint32(encodeZigZag32(v), buf);
    }

    public static void writeUnsignedInt( final int v, final ByteArray buf )
    {
        writeRawVarint32(v, buf);
    }
    public static void writeUnsignedLong( final long v, final ByteArray buf )
    {
        writeRawVarint64(v, buf);
    }

    public static long readSignedLong( final ByteArray buf )
    {
        return decodeZigZag64( readRawVarint64SlowPath(buf) );
    }

    public static int readSignedInt( final ByteArray buf )
    {
        return decodeZigZag32((int) readRawVarint64SlowPath(buf));
    }

    public static int readUnsignedInt( final ByteArray buf )
    {
        return (int) readRawVarint64SlowPath(buf);
    }
    public static long readUnsignedLong( final ByteArray buf )
    {
        return readRawVarint64SlowPath(buf);
    }

    /** Encode and write a varint. */
    private static void writeRawVarint64( long value, final ByteArray buf) {
      while (true) {
        if ((value & ~0x7FL) == 0) {
            buf.put((byte) value);
            return;
        } else {
            buf.put((byte) (((int)value & 0x7F) | 0x80));
            value >>>= 7;
        }
      }
    }

    /**
     * Encode and write a varint.  {@code value} is treated as
     * unsigned, so it won't be sign-extended if negative.
     */
    private static void writeRawVarint32(int value, final ByteArray buf)  {
      while (true) {
        if ((value & ~0x7F) == 0) {
          buf.put((byte) value);
          return;
        } else {
          buf.put((byte) ((value & 0x7F) | 0x80));
          value >>>= 7;
        }
      }
    }

    /**
     * Decode a ZigZag-encoded 32-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n An unsigned 32-bit integer, stored in a signed int because
     *          Java has no explicit unsigned support.
     * @return A signed 32-bit integer.
     */
    private static int decodeZigZag32(final int n) {
      return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Decode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n An unsigned 64-bit integer, stored in a signed int because
     *          Java has no explicit unsigned support.
     * @return A signed 64-bit integer.
     */
    private static long decodeZigZag64(final long n) {
      return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Read a raw Varint from the stream.  If larger than 32 bits, discard the
     * upper bits.
     */
    private static int readRawVarint32( final ByteArray buf ) {
      // See implementation notes for readRawVarint64
        int x;
        if ((x = buf.get()) >= 0) {
          return x;
        } else if ((x ^= (buf.get() << 7)) < 0) {
          x ^= (~0 << 7);
        } else if ((x ^= (buf.get() << 14)) >= 0) {
          x ^= (~0 << 7) ^ (~0 << 14);
        } else if ((x ^= (buf.get() << 21)) < 0) {
          x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21);
        } else {
          int y = buf.get();
          x ^= y << 28;
          x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21) ^ (~0 << 28);
        }
        return x;
    }

    /** Read a raw Varint from the stream. */
    private static long readRawVarint64( final ByteArray buf ) {
        // Implementation notes:
        //
        // Optimized for one-byte values, expected to be common.
        // The particular code below was selected from various candidates
        // empirically, by winning VarintBenchmark.
        //
        // Sign extension of (signed) Java bytes is usually a nuisance, but
        // we exploit it here to more easily obtain the sign of bytes read.
        // Instead of cleaning up the sign extension bits by masking eagerly,
        // we delay until we find the final (positive) byte, when we clear all
        // accumulated bits with one xor.  We depend on javac to constant fold.
        long x;
        int y;
        if ((y = buf.get()) >= 0) {
          return y;
        } else if ((y ^= (buf.get() << 7)) < 0) {
          x = y ^ (~0 << 7);
        } else if ((y ^= (buf.get() << 14)) >= 0) {
          x = y ^ ((~0 << 7) ^ (~0 << 14));
        } else if ((y ^= (buf.get() << 21)) < 0) {
          x = y ^ ((~0 << 7) ^ (~0 << 14) ^ (~0 << 21));
        } else if ((x = ((long) y) ^ ((long) buf.get() << 28)) >= 0L) {
          x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
        } else if ((x ^= ((long) buf.get() << 35)) < 0L) {
          x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
        } else if ((x ^= ((long) buf.get() << 42)) >= 0L) {
          x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42);
        } else if ((x ^= ((long) buf.get() << 49)) < 0L) {
          x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42)
              ^ (~0L << 49);
        } else {
          x ^= ((long) buf.get() << 56);
          x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42)
              ^ (~0L << 49) ^ (~0L << 56);
          if ( buf.get() < 0 )
              throw new RuntimeException("Malformed input!");
        }
        return x;
    }

    private static long readRawVarint64SlowPath(final ByteArray buf ) {
       long result = 0;
       for (int shift = 0; shift < 64; shift += 7) {
           final byte b = buf.get();
           result |= (long) (b & 0x7F) << shift;
           if ((b & 0x80) == 0) {
               return result;
           }
       }
       throw new RuntimeException("Can't happen");
   }

}
