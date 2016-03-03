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

    public static int twoCombinationsCount(int n) {
        BigInteger nFactorial = factorial(n);
        BigInteger nm2Factorial = factorial(n-2);
        BigInteger nc2 = nFactorial.divide(nm2Factorial.multiply(new BigInteger("2")));
        return nc2.intValue();
    }

    public static int rankTwoCombination(final int[] c) {
        int rank = (c[1] >= 2)?(c[1]*(c[1]-1)/2):0;
        rank += (c[0] >= 1)?c[0]:0;
        return rank;
    }

    public static int rankCombination(final int[] c, final int k) {
        int rank = 0;
        for (int i = k; i >= 1 ; i--) {
            if(c[i-1] >= i) {
                rank += org.apache.commons.math3.util.CombinatoricsUtils.binomialCoefficient(c[i - 1], i);
            }
        }
        return rank;
    }

    public static Iterator<int[]> getPairs(final int n) {
        return (new Combinations(n,2)).iterator();
    }



    public static int[] ranksContaining(final int e, final int n) {
        int rr[][] = ranksArraysContaining(e,n);
        int[] ranks = new int[n-1];
        int r = 0;
        if(rr[0].length == 2) for (int i = rr[0][0]; i <= rr[0][1]; i++) ranks[r++] = i;
        for (int i = (rr[0].length == 2)?1:0 ; i < rr.length; i++) ranks[r++] = rr[i][0];
        return ranks;
    }

    public static int[][] ranksArraysContaining(final int e, final int n) {
        int[][] ranks;
        int ranksLeft = n - 1;
        int i = 0;
        if (e > 0) {
            int[] p0 = new int[]{0, e};
            int rs = rankTwoCombination(p0);
            int re = rs + e - 1;
            ranksLeft -= e;
            ranks = new int[1 + ranksLeft][];
            ranks[i++] = new int[]{rs, re};
        } else ranks = new int[ranksLeft][];

        if(ranksLeft > 0) {
            int element = e + 1;
            int r = rankTwoCombination(new int[]{e, element});
            while (ranksLeft > 0) {
                ranks[i++] = new int[]{r};
                r += element;
                element++;
                ranksLeft--;
            }
        }
        return ranks;
    }

    public static Iterator<Integer> ranksOfElementIterator(final int e, final int n) {
        return new ElementLexicographicIterator(e,n);
    }

    private static class ElementLexicographicIterator implements Iterator<Integer> {

        private boolean more;
        private int[][] ranks;
        private int ranksLeft;
        private int rank;
        private boolean sequencial;
        private int rstart;
        private int rend;
        private int i;

        public ElementLexicographicIterator(final int e,final int n) {
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
            else { rank = ranks[i][0] ; }
        }

        public boolean hasNext() {
            return more;
        }

        public Integer next() {
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
}
