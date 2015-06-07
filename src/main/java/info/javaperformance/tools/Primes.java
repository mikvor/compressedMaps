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

import java.util.Arrays;
import java.util.BitSet;

/**
 * Prime number generator and lookup table
 */
public class Primes {
    /**
     * Prime number table. It is constructed using an algorithm below.
     */
    private static final int[] PRIMES = {
            5, 7, 13, 19, 31, 43, 61, 73, 103, 109, 139, 151, 181, 193, 199, 229, 241, 271, 283, 313, 349, 421, 463, 523,
            571, 601, 643, 811, 859, 1021, 1063, 1153, 1231, 1279, 1321, 1429, 1483, 1609, 1669, 1723, 1789, 1873, 1933,
            1999, 2083, 2239, 2311, 2383, 2551, 2659, 2791, 2971, 3121, 3253, 3361, 3463, 3583, 3769, 3919, 4051, 4219,
            4423, 4639, 4789, 4933, 5101, 5281, 5443, 5641, 5851, 6091, 6301, 6553, 6763, 7129, 7351, 7591, 7879, 8221,
            8539, 8821, 9241, 9631, 9931, 10273, 10711, 11059, 11491, 11941, 12379, 12823, 13219, 13681, 14251, 14869,
            15331, 15889, 16453, 16981, 17491, 18043, 18913, 19543, 20149, 20773, 21493, 22159, 22861, 23563, 24373,
            25171, 25933, 26713, 27529, 28411, 29389, 30271, 31183, 32119, 33151, 34159, 35281, 36343, 37549, 38713,
            40039, 41389, 42643, 43963, 45319, 46681, 48121, 49669, 51199, 52861, 54499, 56209, 57901, 59671, 61471,
            63391, 65449, 67429, 69493, 71713, 74101, 76369, 78781, 81199, 83641, 86293, 89071, 91813, 94651, 97501,
            100519, 103813, 106963, 110323, 113719, 117193, 120739, 124429, 128203, 132331, 136399, 140551, 144889,
            149251, 153889, 158749, 163861, 168901, 174019, 179383, 184831, 190579, 196501, 202639, 208891, 215353,
            221953, 228619, 235663, 243121, 250501, 258109, 265873, 273901, 282241, 290839, 299569, 308641, 317923,
            327493, 337369, 347563, 358159, 369079, 380203, 391693, 403549, 415801, 428299, 441193, 454453, 468109,
            482233, 496891, 511963, 527701, 543553, 560083, 577009, 594403, 612319, 630901, 649879, 669379, 689461,
            710221, 731683, 753679, 776563, 799993, 824191, 848923, 874459, 900763, 928099, 955993, 984703, 1014259,
            1044739, 1076113, 1108489, 1141969, 1176601, 1211923, 1248349, 1285813, 1324513, 1364329, 1405363, 1447561,
            1491001, 1535971, 1582081, 1629559, 1678603, 1729129, 1781029, 1834603, 1889653, 1946473, 2005021, 2065573,
            2127649, 2191951, 2257861, 2326021, 2395849, 2467903, 2541943, 2618263, 2696923, 2777839, 2861233, 2947099,
            3035581, 3126733, 3220579, 3317203, 3416893, 3519601, 3625213, 3734413, 3846463, 3961963, 4080943, 4203373,
            4329979, 4460713, 4594621, 4732603, 4875253, 5021773, 5172451, 5327713, 5487763, 5652403, 5822083, 5997163,
            6177673, 6363061, 6554461, 6751111, 6953719, 7162699, 7377613, 7599043, 7827301, 8062183, 8304421, 8553913,
            8810959, 9075361, 9347749, 9628711, 9917599, 10215349, 10522093, 10837831, 11162971, 11497963, 11843173, 12198631,
            12564733, 12942331, 13330729, 13730809, 14142763, 14567173, 15004543, 15454741, 15918439, 16396273, 16888303,
            17395453, 17917843, 18455431, 19009261, 19579753, 20167519, 20772601, 21395851, 22037863, 22699009, 23380081,
            24081523, 24804643, 25549171, 26315671, 27105373, 27918601, 28756801, 29619523, 30508351, 31424401, 32367199,
            33338533, 34338991, 35369221, 36430423, 37523443, 38649241, 39809059, 41003491, 42234109, 43501399, 44806459,
            46151461, 47536339, 48962623, 50431651, 51945001, 53503393, 55109149, 56762803, 58465753, 60219769, 62026429,
            63887269, 65804071, 67779619, 69813019, 71907679, 74065153, 76287403, 78576163, 80933563, 83361781, 85863079,
            88439161, 91092349, 93825133, 96640003, 99539389, 102525679, 105601933, 108770023, 112033423, 115394959,
            118857241, 122423113, 126096109, 129879163, 133775701, 137789293, 141923401, 146181109, 150566641, 155083933,
            159736639, 164529091, 169465159, 174549283, 179785981, 185179669, 190735681, 196458133, 202352083, 208422793,
            214675693, 221116369, 227749939, 234582709, 241620229, 248869471, 256336603, 264027013, 271947919, 280106461,
            288510463, 297165853, 306081301, 315263779, 324722161, 334463839, 344498599, 354833749, 365479033, 376443649,
            387737101, 399369331, 411350449, 423691111, 436402093, 449494261, 462979219, 476868883, 491175301, 505910791,
            521088289, 536721049, 552822859, 569407693, 586490479, 604085551, 622208401, 640874713, 660101371, 679904653,
            700301809, 721311391, 742950919, 765240211, 788197771, 811844263, 836199811, 861285949, 887124781, 913738543,
            941150851, 969385603, 998467231, 1028421421, 1059274159, 1091052421, 1123784023, 1157498473, 1192223491,
            1227990901, 1264830859, 1302776779, 1341860593, 1382116861, 1423580581, 1466288713, 1510277413, 1555585909,
            1602253729, 1650322591, 1699832341, 1750827343, 1803352351, 1857453421, 1913177041, 1970572399, 2029689793, 2090581333,
            2147483629
     };

    /**
     * Find the next prime for the given number. We may return the argument too because the return value is used
     * as a capacity in the chained map implementation, which feels fine in case of size reaching capacity.
     *
     * There is a special property of returned values (except Integer.MAX_VALUE) - N-2 is also a prime number.
     *
     * @param num A positive int
     * @return Next prime number
     */
    public static int findNextPrime( final int num )
    {
        final int pos = Arrays.binarySearch(PRIMES, num);
        if ( pos >= 0 )
            return PRIMES[ pos ]; //it is fine to return the current value for the fully chained maps
        else
            return PRIMES[ -pos - 1 ];
    }

    /**
     * Extended version of lookup supporting arguments greater than {@code Integer.MAX_VALUE}. In case of such arguments
     * it returns the maximal prime number less than {@code Integer.MAX_VALUE}
     * @param num A positive int
     * @return Next / maximal allowed prime number
     */
    public static int findNextPrime( final long num )
    {
        if ( num >= getMaxIntPrime() )
           return getMaxIntPrime();
        else
            return findNextPrime( (int) num );
    }

    public static int getMaxIntPrime()
    {
        return PRIMES[ PRIMES.length - 1 ];
    }

    /*
    In order to build a prime number table (offline) we make the following steps:
    1) Calculate all positive primes in the Integer range using sieve of Eratosthenes
    2) Keep only primes = N where N-2 is also a prime
    3) Keep only primes which are at least 3% bigger than previous kept prime number (this step leaves
    a pretty sparse list, which is, nevertheless sufficient for map resize purposes).
    */

    public static void main(String[] args) {
        buildPrimeList();
    }

    private static void buildPrimeList()
    {
        final BitSet bs = new BitSet( Integer.MAX_VALUE );

        //build total prime list
        bs.set(2, Integer.MAX_VALUE);
        for ( int i = 2; i <= Integer.MAX_VALUE / 2; ++i )
        {
            if ( bs.get( i ) )
            {
                int p = i + i;
                while ( p > 0 )
                {
                    bs.clear( p );
                    p += i;  //wrap around
                }
            }
        }

        for ( int i = Integer.MAX_VALUE; i >= 0; --i )
            if ( bs.get( i ) ) {
                System.out.println("Max prime = " + i);
                System.exit(0);
            }

        //leave only such primes = N where N-2 is also prime
        int prev = Integer.MIN_VALUE;
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
            if ( i - 2 != prev )
                bs.clear( i );
            prev = i;
        }

        //now leave only primes such that N_i >= 1.03 * N_(i-1) (at least 3% step between numbers) - this does not
        //leave too large gaps between the numbers
        double start = 0;
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
            if ( i >= start )
                start = i * 1.03;
            else
                bs.clear( i );
        }

        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1))
            System.out.print( i + ", " );
    }

}
