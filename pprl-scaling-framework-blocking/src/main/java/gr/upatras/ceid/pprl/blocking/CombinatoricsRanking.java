package gr.upatras.ceid.pprl.blocking;

import org.apache.commons.math3.util.CombinatoricsUtils;

import java.util.Iterator;

    /**
     * Utility class for two-combinations on natural numbers.
     * Using also
     *
     * @see org.apache.commons.math3.util.Combinations
     * @see org.apache.commons.math3.util.CombinatoricsUtils
     * @see <a href="http://en.wikipedia.org/wiki/Combinatorial_number_system">Combinatorial Number System</a>
     * @see <a href="http://www.site.uottawa.ca/~lucia/courses/5165-09/GenCombObj.pdf">Generating Elementary Combinatorial Objects</a>
     */
    public class CombinatoricsRanking {

        /**
         * Lexicographical rank of c in the 2-combination order.
         *
         * @param c combination
         * @return rank;
         */
        public static int rank2Combination(final int[] c) {
            int rank = (c[1] >= 2)?(c[1]*(c[1]-1)/2):0;
            rank += (c[0] >= 1)?c[0]:0;
            return rank;
        }

        /**
         * Lexicographical rank of c in the k-combination order.
         *
         * @param c combination
         * @param k k
         * @return rank;
         */
        public static int rankCombination(final int[] c, final int k) {
            int rank = 0;
            for (int i = k; i >= 1 ; i--) {
                if(c[i-1] >= i) {
                    rank += CombinatoricsUtils.binomialCoefficient(c[i - 1], i);
                }
            }
            return rank;
        }

        /**
         * Ranks of an element as an array of int.
         * @param e element
         * @param n number of elements
         * @return ranks in array
         */
        public static int[] ranksContaining(final int e, final int n) {
            int rr[][] = ranksArraysContaining(e,n);
            int[] ranks = new int[n-1];
            int r = 0;
            if(rr[0].length == 2) for (int i = rr[0][0]; i <= rr[0][1]; i++) ranks[r++] = i;
            for (int i = (rr[0].length == 2)?1:0 ; i < rr.length; i++) ranks[r++] = rr[0][0];
            return ranks;
        }

        /**
         * Ranks of an element returned in a array of int arrays.
         * First element usually holds (not the case of 0 element) an array of two
         * ints indicating a rank incremental sequence (r[0][0]:r[0][1]).
         * Other elements hold only one rank (r[i][0])
         *
         *
         * @param e element
         * @param n number of elements
         * @return ranks in
         */
        public static int[][] ranksArraysContaining(final int e, final int n) {
            int[][] ranks;
            int ranksLeft = n - 1;
            int i = 0;
            if (e > 0) {
                int[] p0 = new int[]{0, e};
                int rs = rank2Combination(p0);
                int re = rs + e - 1;
                ranksLeft -= e;
                ranks = new int[1 + ranksLeft][];
                ranks[i++] = new int[]{rs, re};
            } else ranks = new int[ranksLeft][];

            if(ranksLeft > 0) {
                int element = e + 1;
                int r = rank2Combination(new int[]{e, element});
                while (ranksLeft > 0) {
                    ranks[i++] = new int[]{r};
                    r += element;
                    element++;
                    ranksLeft--;
                }
            }
            return ranks;
        }

        /**
         * Get the rank of element e iterator
         * @param e element
         * @param n max element.
         * @return iterator
         */
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