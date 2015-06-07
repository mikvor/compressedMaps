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

/**
 * Map state in the concurrent maps.
 * There are 2 valid states:
 * 1) stable (old==null)
 * 2) rehash (old!=null)
 *
 * We always know what's the current map version (changes every time cur or old changes) and what's the
 * next stable version (which we should wait during rehashing).
 *
 * Each thread participating in rehashing takes a ticket from {@code resizeWorkers}.
 *
 * We also carry the map size threshold in this structure because it changes every time we get into rehashing state.
 *
 * {@code cur} and {@code old} carry {@code long} numbers encoded using {@code LongBucketEncoding}
 */
public class Buffers
{
    /** Currently used table (it is populated during rehash) */
    public final long[] cur;
    /** Previous table (not null only during rehash) */
    public final long[] old;
    /**
     * Map size threshold - next rehashing happens after we exceed the threshold.
     * This field is {@code long} because we can use fill factors > 1.
     */
    public final long threshold;
    /** Current map version - increased before and after each rehashing */
    public final int version;
    /** Next stable version */
    public final int nextStableVersion;
    /** Number of threads working on map rehash */
    public final int resizeWorkers;

    public Buffers( long[] cur, long[] old, long threshold, int version, int nextStableVersion)
    {
        this( cur, old, threshold, version, nextStableVersion, 0);
    }

    public Buffers( long[] cur, long[] old, long threshold, int version, int nextStableVersion, int resizeWorkers) {
        this.cur = cur;
        this.old = old;
        this.threshold = threshold;
        this.version = version;
        this.nextStableVersion = nextStableVersion;
        this.resizeWorkers = resizeWorkers;
    }

    public String toString()
    {
        return "cur.len = " + cur.length + ", old" + ( old == null ? " == null" : ".len = " + old.length ) +
        ", threshold = " + threshold + ", version = " + version + ", nextStableVersion = " + nextStableVersion +
                ", resizeWorkers = " + resizeWorkers;
    }

    /**
     * Create a new object with the incremented number of workers
     * @return A new object with all fields equal to the current one except the incremented number of workers
     */
    public Buffers addWorker()
    {
        return new Buffers( cur, old, threshold, version, nextStableVersion, resizeWorkers + 1 );
    }

    /**
     * Either reduce the number of workers or move the object into the next stable state
     * @return An updated object
     */
    public Buffers removeWorker()
    {
        if ( resizeWorkers == 1 )
            return new Buffers( cur, null, threshold,
                    nextStableVersion,     //no need to rely on the current version, we are entering the stable state
                    nextStableVersion + 2  //new stable version is 2 versions further ahead
                    );
        else
            //just reduce number of workers
            return new Buffers( cur, old, threshold, version, nextStableVersion, resizeWorkers - 1 );
    }
}
