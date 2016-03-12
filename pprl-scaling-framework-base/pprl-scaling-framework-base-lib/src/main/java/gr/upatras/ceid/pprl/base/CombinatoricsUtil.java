package gr.upatras.ceid.pprl.base;

import org.apache.commons.math3.util.Combinations;

import java.math.BigInteger;
import java.util.Iterator;

public class CombinatoricsUtil {
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

    public static BigInteger factorial(long n) { return recfact(1, n); }

    public static long twoCombinationsCount(long n) {
        assert n >= 2;
        BigInteger nFactorial = factorial(n);
        BigInteger nm2Factorial = factorial(n-2);
        BigInteger nc2 = nFactorial.divide(nm2Factorial.multiply(new BigInteger("2")));
        return nc2.longValue();
    }

    public static long rankTwoCombination(final int[] c) {
        assert c.length == 2;
        assert c[0] >= 0 || c[1] >= 0;
        long rank = (c[1] >= 2)?(c[1]*(c[1]-1)/2):0;
        rank += (c[0] >= 1)?c[0]:0;
        return rank;
    }

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

    public static Iterator<int[]> getPairs(final int n) {
        assert n >=2;
        return (new Combinations(n,2)).iterator();
    }


    public static long[] ranksContaining(final int e, final int n) {
        long rr[][] = ranksArraysContaining(e,n);
        long[] ranks = new long[n-1];
        int r = 0;
        if(rr[0].length == 2) for (long i = rr[0][0]; i <= rr[0][1]; i++) ranks[r++] = i;
        for (int i = (rr[0].length == 2)?1:0 ; i < rr.length; i++) ranks[r++] = rr[i][0];
        return ranks;
    }

    public static long[][] ranksArraysContaining(final int e, final int n) {
        assert e < n && e >= 0 && n >= 0;
        long[][] ranks;
        int ranksLeft = n - 1;
        int i = 0;
        if (e > 0) {
            int[] p0 = new int[]{0, e};
            long rs = rankTwoCombination(p0);
            long re = rs + e - 1;
            ranksLeft -= e;
            ranks = new long[1 + ranksLeft][];
            ranks[i++] = new long[]{rs, re};
        } else ranks = new long[ranksLeft][];

        if(ranksLeft > 0) {
            int element = e + 1;
            long r = rankTwoCombination(new int[]{e, element});
            while (ranksLeft > 0) {
                ranks[i++] = new long[]{r};
                r += element;
                element++;
                ranksLeft--;
            }
        }
        return ranks;
    }

    public static Iterator<Long> oldranksOfElementIterator(final int e, final int n) {
        return new OldElementLexicographicIterator(e,n);
    }

    private static class OldElementLexicographicIterator implements Iterator<Long> {

        private boolean more;
        private long[][] ranks;
        private int ranksLeft;
        private long rank;
        private boolean sequencial;
        private long rstart;
        private long rend;
        private int i;

        public OldElementLexicographicIterator(final int e, final int n) {
            ranks = ranksArraysContaining(e, n);
            ranksLeft = n - 1;
            more = true;
            rank = 0;
            i = 0;
            if(ranks[0].length == 2) {
                rstart = ranks[i][0];
                rend = ranks[i][1];
                i = 1;
                sequencial = true;
            }
            else { rank = ranks[0][0] ; }
        }

        public boolean hasNext() {
            return more;
        }

        public Long next() {
            if(sequencial) {
                rank = rstart++;
                sequencial = !(rstart > rend);
            } else {
                rank = ranks[i++][0];
            }
            ranksLeft--;
            if(ranksLeft <= 0 || i == ranks.length ) { more = false ;}
            return rank;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

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

        public ElementLexicographicIterator(final int e, final int n) {
            ranksLeft = n - 1;
            this.e = e;
            more = true;
            other =  (e > 0) ? 0 : 1;
            baseRank = rankTwoCombination(
                    (e > 0) ? new int[]{other,this.e} :
                              new int[]{this.e,other}
            );
            sequencial = (e > 0);
        }

        public boolean hasNext() {
            return more;
        }

        public Long next() {
            if(sequencial) {
                rank = baseRank + other;
                other++;
                ranksLeft--;
                more = !(ranksLeft == 0);
                sequencial=!(other == e);
                return rank;
            } else {
                if(other == e){
                    other++;
                    baseRank = rankTwoCombination(new int[]{e,other});
                    rank = baseRank;
                } else {
                    rank = rank + (other-1);
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
