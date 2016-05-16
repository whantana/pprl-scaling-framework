package gr.upatras.ceid.pprl.combinatorics;

import org.apache.commons.math3.util.Combinations;

import java.math.BigInteger;
import java.util.Iterator;

/**
 * Combinatorics and raking utility class.
 */
public class CombinatoricsUtil {

    /**
     * Recursive Factorial calculation.
     *
     * @param start input long number.
     * @param n n long number.
     * @return a <class>BigInteger</class> number as the result of calculation.
     */
    public static BigInteger recfact(long start, long n) {
        long i;
        if (n <= 16) {
            BigInteger r = new BigInteger(String.valueOf(start));
            for (i = start + 1; i < start + n; i++) r = r.multiply(new BigInteger(String.valueOf(i)));
            return r;
        }
        i = n / 2;
        return recfact(start, i).multiply(recfact(start + i, n - i));
    }

    /**
     * Recursive Factorial calculation.
     *
     * @param n long number.
     * @return a <class>BigInteger</class> number as the result of calculation.
     */
    public static BigInteger factorial(long n) { return recfact(1, n); }

    /**
     * Returns the count of pairs (2-combinations) in the range [0...n]
     *
     * @param n long number.
     * @return the count calculated count.
     */
    public static long twoCombinationsCount(long n) {
        assert n >= 2;
        BigInteger nFactorial = factorial(n);
        BigInteger nm2Factorial = factorial(n-2);
        BigInteger nc2 = nFactorial.divide(nm2Factorial.multiply(new BigInteger("2")));
        return nc2.longValue();
    }

    /**
     * Returns the count of pairs (2-combinations) in the range [0...n]
     *
     * @param n long number.
     * @return the count calculated count.
     */
    public static long combinationsCount(long n,int k) {
        assert n >= 2;
        BigInteger nFactorial = factorial(n);
        BigInteger nmkFactorial = factorial(n-k);
        BigInteger kFactorial =  factorial(k);
        BigInteger nck = nFactorial.divide(nmkFactorial.multiply(kFactorial));
        return nck.longValue();
    }

    /**
     * Returns the rank of a 2-combination.
     *
     * @param c  int pair, where c[0] < c[1].
     * @return the rank.
     */
    public static long rankTwoCombination(final int[] c) {
        assert c.length == 2;
        assert c[0] >= 0 || c[1] >= 0;
        long rank = (c[1] >= 2)?(c[1]*(c[1]-1)/2):0;
        rank += (c[0] >= 1)?c[0]:0;
        return rank;
    }

    /**
     * Returns the rank of a k-combination.
     *
     * @param c int array. where c[0] < c[1] < c[2] ... < c[k].
     * @param k int k.
     * @return the rank.
     */
    public static int rankCombination(final int[] c, final int k) {
        assert c.length == k;
        int rank = 0;
        for (int i = k; i >= 1 ; i--) {
            if(c[i-1] >= i) {
                rank += org.apache.commons.math3.util.CombinatoricsUtils.binomialCoefficient(c[i - 1], i);
            }
        }
        return rank;
    }

    /**
     * Returns a lexicographical iterator of pairs in range [0,1,...,n-1,n).
     *
     * @param n int n.
     * @return a lexicographical iterator of pairs.
     */
    public static Iterator<int[]> getPairs(final int n) {
        assert n >=2;
        return (new Combinations(n,2)).iterator();
    }

    /**
     * Returns an ordered array of the ranks of pairs in range [0,1,...,n-1,n) containg e.
     *
     * @param e int e.
     * @param n int n.
     * @return  an ordered array of the ranks of pairs containing e
     */
    public static long[] ranksContaining(final int e, final int n) {
        long rr[][] = ranksArraysContaining(e,n);
        long[] ranks = new long[n-1];
        int r = 0;
        if(rr[0].length == 2) for (long i = rr[0][0]; i <= rr[0][1]; i++) ranks[r++] = i;
        for (int i = (rr[0].length == 2)?1:0 ; i < rr.length; i++) ranks[r++] = rr[i][0];
        return ranks;
    }

    /**
     * Returns a two dimensional array of the ranks of pairs in range [0,1,...,n-1,n) containg e.
     *
     * @param e int e.
     * @param n int n.
     * @return a two dimensional array of the ranks of pairs containing e.
     */
    public static long[][] ranksArraysContaining(final int e, final int n) {
        assert e < n && e >= 0 && n >= 0;
        long[][] ranks;
        int ranksLeft = n - 1;
        int i = 0;
        if (e > 0) {
            int[] p0 = new int[]{0, e};         // for pairs that e is the greater element.
            long rs = rankTwoCombination(p0);   // we only need to calculate the rank of [0,e]
            long re = rs + e - 1;               // and then add 1 up to e for ranks [1,e],[2,e],...[e-1,e]
            ranksLeft -= e;
            ranks = new long[1 + ranksLeft][];
            ranks[i++] = new long[]{rs, re};
        } else ranks = new long[ranksLeft][];

        if(ranksLeft > 0) {
            int element = e + 1;                                // for pairs that e is the lesser element.
            long r = rankTwoCombination(new int[]{e, element}); // we only need to calculate the [e,element = e+1]
            while (ranksLeft > 0) {                             // and then add element up to n-1 for ranks [e,element]
                ranks[i++] = new long[]{r};
                r += element;
                element++;
                ranksLeft--;
            }
        }
        return ranks;
    }

    /**
     * Ranks iterator for the ranks of pairs in range [0,1,...,n-1,n) containing e.
     * @param e int e.
     * @param n int n.
     * @return iterator for the ranks of pairs containing e.
     */
    @Deprecated
    public static Iterator<Long> oldranksOfElementIterator(final int e, final int n) {
        return new OldElementLexicographicIterator(e,n);
    }

    @Deprecated
    private static class OldElementLexicographicIterator implements Iterator<Long> {

        private boolean more;
        private long[][] ranks;
        private int ranksLeft;
        private long rank;
        private boolean sequencial;
        private long rstart;
        private long rend;
        private int i;

        /**
         * Iterator  for the ranks of pairs in range [0,1,...,n-1,n) containing e.
         *
         * @param e int e.
         * @param n int n.
         */
        public OldElementLexicographicIterator(final int e, final int n) {
            ranks = ranksArraysContaining(e, n); // calculate ranks
            ranksLeft = n - 1;
            more = true;
            rank = 0;
            i = 0;
            if(ranks[0].length == 2) {
                rstart = ranks[i][0];
                rend = ranks[i][1];
                i = 1;
                sequencial = true;    // sequncial mode
            }
            else { rank = ranks[0][0] ; } // start with jump mode
        }

        public boolean hasNext() {
            return more;
        }

        public Long next() {
            if(sequencial) {
                rank = rstart++; // sequencial mode
                sequencial = !(rstart > rend);
            } else {
                rank = ranks[i++][0]; // jump mode
            }
            ranksLeft--;
            if(ranksLeft <= 0 || i == ranks.length ) { more = false ;} // no more ranks left
            return rank;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Ranks iterator for the ranks of pairs in range [0,1,...,n-1,n) containing e.
     * @param e int e.
     * @param n int n.
     * @return iterator for the ranks of pairs containing e.
     */
    public static Iterator<Long> ranksOfElementIterator(final int e, final int n) {
        assert e < n && e >= 0 && n >= 0;
        return new ElementLexicographicIterator(e,n);
    }

    private static class ElementLexicographicIterator implements Iterator<Long> {
        private int ranksLeft;
        private long baseRank;
        private int e;
        private int other;
        private long rank;
        private boolean sequencial;
        private boolean more;

        /**
         * Iterator  for the ranks of pairs in range [0,1,...,n-1,n) containing e.
         *
         * @param e int e.
         * @param n int n.
         */
        public ElementLexicographicIterator(final int e, final int n) {
            // Essential same iterator with the above with
            // the rank calculation taking place during iteration
            // not at the constructor.
            ranksLeft = n - 1;
            this.e = e;
            more = true;
            other =  (e > 0) ? 0 : 1;
            baseRank = rankTwoCombination(
                    (e > 0) ? new int[]{other,this.e} :
                              new int[]{this.e,other}
            );    // establish base rank (first rank that e is present)
            sequencial = (e > 0); // start sequencial mode for e > 0
        }

        public boolean hasNext() {
            return more;
        }

        public Long next() {
            if(sequencial) {                  // sequencial mode
                rank = baseRank + other;
                other++;
                ranksLeft--;
                more = !(ranksLeft == 0);
                sequencial=!(other == e);
                return rank;
            } else {                         // jump mode
                if(other == e){
                    other++;
                    baseRank = rankTwoCombination(new int[]{e,other});    // re-establish base rank once
                    rank = baseRank;
                } else {
                    rank = rank + (other-1);            // calculate rank in the jump mode
                }
                other++;
                ranksLeft--;
                more = !(ranksLeft == 0);
                return rank;
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
