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
 * A serializer which uses a shorter integral representation for monetary values and falls back to double if it can not convert
 * a value to an integer.
 *
 * This serializer tries to multiply the {@code double} number by {@code 10^decimalPoints} and checks if the result
 * can be loselessly converted to a {@code long} value. If so, the {@code long} value is stored.
 *
 * Set the {@code decimalPoints} equal to a number of digits in the smallest currency unit (2 for most currencies, 3 for very few).
 * In case of processing the stock exchange data, check the smallest tick size (you seldom need more than 4 decimal digits
 * even for ticks).
 * In general case do not set the number of decimal digits higher than the most possible number of decimal digits
 * in your data, because every extra decimal digits means 3 or 4 extra bits used for the encoding.
 *
 * We use a simple encoding for this serializer:
 * byte 0 = 0 for long values
 *        = 1 for double values
 * It is followed by varlen-encoded long or 8 bytes for double.
 *
 * In order to deal with the possible rounding errors while calculating the delta, we use delta encoding only
 * for 2 {@code double} values successfully converted into {@code long}. We store the current number itself in all
 * other cases thus avoiding any rounding errors in this serializer.
 */
public class MoneyDoubleSerializer implements IDoubleSerializer {
    /** Maximal supported number of digits after decimal point.
     * We do not use too high values here because we want to gain some storage space. */
    public static final int MAX_ALLOWED_PRECISION = 8;

    private static final long NOT_PARSED = Long.MIN_VALUE;
    private static final byte LONG = 0;
    private static final byte DOUBLE = 1;

    private static final long[] MULTIPLIERS = new long[ MAX_ALLOWED_PRECISION + 1 ];
    static
    {
        long val = 1;
        for ( int i = 0; i <= MAX_ALLOWED_PRECISION; ++i )
        {
            MULTIPLIERS[ i ] = val;
            val *= 10;
        }
    }

    /** Multiplier used for double->long conversions = 10^decimalPoints */
    private final long m_multiplier;
    /** Previously converted {@code long} value */
    private long m_prev = NOT_PARSED;

    /**
     * Create the serializer. Note that you should be careful about the number of decimal points you request.
     * For example, if most of your values have 2 decimal digits (cents, for example) and only a few have 3 or 4 digits,
     * it may be wiser to set {@code decimalPoints=2} and have shorter encoding for most of your values (remember that
     * each extra digit adds 3 or 4 bits to <b>each</b> encoded value).
     *
     * @param decimalPoints Number of decimal points
     */
    public MoneyDoubleSerializer( final int decimalPoints )
    {
        if ( decimalPoints < 0 || decimalPoints > MAX_ALLOWED_PRECISION )
            throw new IllegalArgumentException( "decimalPoints should be between 0 and " + MAX_ALLOWED_PRECISION );
        m_multiplier = MULTIPLIERS[ decimalPoints ];
    }

    @Override
    public void write( final double v, final ByteArray buf )
    {
        m_prev = fromDouble0( v, m_multiplier );
        if ( m_prev != NOT_PARSED )
        {
            buf.put( LONG );
            VarLen.writeSignedLong( m_prev, buf );
        }
        else
        {
            buf.put( DOUBLE );
            VarLen.writeDouble( v, buf );
        }
    }

    @Override
    public double read( final ByteArray buf ) {
        if ( buf.get() == LONG )
        {
            m_prev = VarLen.readSignedLong( buf );
            return ( ( double ) m_prev ) / m_multiplier;
        }
        else
        {
            m_prev = NOT_PARSED;
            return VarLen.readDouble( buf );
        }
    }

    @Override
    public void writeDelta( final double prevValue, final double curValue, final ByteArray buf, final boolean sorted ) {
        if ( m_prev == NOT_PARSED )
        {
            //this call writes the current value and saves m_prev, which may allow delta encoding on the next step
            write( curValue, buf );
        }
        else
        {
            final long lCur = fromDouble0( curValue, m_multiplier );
            if ( lCur == NOT_PARSED )
            {
                m_prev = NOT_PARSED; //can't write delta on next step
                buf.put( DOUBLE );
                VarLen.writeDouble( curValue, buf );
            }
            else
            {
                buf.put( LONG );
                if ( sorted )
                    VarLen.writeUnsignedLong( lCur - m_prev, buf );
                else
                    VarLen.writeSignedLong( lCur - m_prev, buf );
                m_prev = lCur;
            }
        }
    }

    @Override
    public double readDelta( final double prevValue, final ByteArray buf, final boolean sorted ) {
        if ( m_prev == NOT_PARSED )
        {
            if ( buf.get() == LONG )
            {
                m_prev = VarLen.readSignedLong( buf );
                return ( ( double ) m_prev ) / m_multiplier;
            }
            else
            {
                //m_prev stays NOT_PARSED
                return VarLen.readDouble( buf );
            }
        }
        else
        {
            if ( buf.get() == LONG )
            {
                final long diff;
                if ( sorted )
                    diff = VarLen.readUnsignedLong( buf );
                else
                    diff = VarLen.readSignedLong( buf );
                final double res = ( ( double ) ( m_prev + diff ) ) / m_multiplier;
                m_prev += diff;
                return res;
            }
            else
            {
                m_prev = NOT_PARSED;
                return VarLen.readDouble( buf );
            }
        }
    }

    @Override
    public int getMaxLength() {
        return 11; //max(long) + 1 byte for flags
    }

    private static long fromDouble0( final double value, final long multiplier )
    {
        //this operation does not guarantee the exact result. We can gain a little more by testing multiplied+-ULP here too.
        final double multiplied = value * multiplier;
        final long converted = ( long ) multiplied;
        if ( multiplied == converted ) //here is an implicit conversion from long to double
            return converted;

        //ulp up
        final double multipliedUp = Math.nextAfter( multiplied, Double.MAX_VALUE );
        final long convertedUp = ( long ) multipliedUp;
        if ( multipliedUp == convertedUp ) //here is an implicit conversion from long to double
            return convertedUp;

        //ulp down
        final double multipliedDown = Math.nextAfter( multiplied, -Double.MAX_VALUE );
        final long convertedDown = ( long ) multipliedDown;
        if ( multipliedDown == convertedDown ) //here is an implicit conversion from long to double
            return convertedDown;

        return NOT_PARSED;
    }
}
