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
 * (De)Serialization logic for map keys/values.
 * Exposing this interface allows you to create more space efficient methods if you know some special properties
 * of your data, like being non-negative.
 *
 * All classes implementing this interface must be thread safe.
 */
public interface IObjectSerializer<V> {
    /**
     * Write a value into a buffer.
     * The method must hold an invariant: a value=N written by {@code write} and then read by {@code read} should be equal to N again.
     * This method allows you to gain extra storage savings for composite keys.
     * @param v Value to write
     * @param buf Output buffer
     */
    public void write( final V v, final ByteArray buf );

    /**
     * Read a value previously written by {@code write}
     * The method must hold an invariant: a value=N written by {@code write} and then read by {@code read} should be equal to N again.
     * This method allows you to gain extra storage savings for composite keys.
     * @param buf Input buffer
     * @return A value
     */
    public V read( final ByteArray buf );


    /**
     * Skip the current value in the buffer. This method should work faster than actual reading.
     * @param buf Input buffer
     */
    public void skip( final ByteArray buf );

    /**
     * Get the maximal length of a value binary representation in this encoding. You must not return too low results
     * from this method. Slightly bigger results are tolerable.
     * @param obj The object to serialize.
     * @return The maximal length of a value binary representation in this encoding
     */
    public int getMaxLength( final V obj );

}
