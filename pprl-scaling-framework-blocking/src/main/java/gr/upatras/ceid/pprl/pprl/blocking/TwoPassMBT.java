package gr.upatras.ceid.pprl.pprl.blocking;

import org.apache.commons.math3.util.Combinations;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Two Pass Single bit tree algorithm implementation.
 *
 * ! more here !
 */
public class TwoPassMBT implements BTAlgorithm {

    private final int N;
    private final int L;
    private final int MIN_COUNT;
    private final int MAX_COUNT;
    private TwoPassMBTNode ROOT_NODE = null;
    private List<Bucket> BUCKETS = null;
    private int[][] BIT_2_COMBINATIONS_COUNTERS;
    private int[] TOTAL_BIT_COUNTER;


    public TwoPassMBT(int n, int l) {
        this(n, l, 10, 20);
    }

    public TwoPassMBT(int n, int l, int min, int max) {
        N = n;
        L = l;
        long BIT_2_COMBINATIONS_COUNT = (long) (L * (L - 1) / 2);
        BIT_2_COMBINATIONS_COUNTERS = new int[(int) BIT_2_COMBINATIONS_COUNT][4];
        TOTAL_BIT_COUNTER = new int[L];
        MIN_COUNT = min;
        MAX_COUNT = max;
    }

    /**
     * DEBUG - dump counters into a file
     *
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    public void dumpCounters() throws FileNotFoundException, UnsupportedEncodingException {

        assert ROOT_NODE != null;
        assert BUCKETS != null;

        PrintWriter writer = new PrintWriter("debug_ccs.txt", "UTF-8");
        writer.println(Arrays.toString(TOTAL_BIT_COUNTER) + "\n");
        Combinations CL2 = new Combinations(L, 2);
        for (int[] c : CL2) {
            int rank = CombinatoricsRanking.rank2Combination(c);

            String s = String.format("%d.{%d,%d} \t 00=%d , 01=%d , 10=%d , 11 =%d",
                    rank, c[1], c[0],
                    BIT_2_COMBINATIONS_COUNTERS[rank][0],
                    BIT_2_COMBINATIONS_COUNTERS[rank][1],
                    BIT_2_COMBINATIONS_COUNTERS[rank][2],
                    BIT_2_COMBINATIONS_COUNTERS[rank][3]);
            writer.println(s);
        }
        writer.close();
    }

    /**
     * PREPARE - Update counters, set root node and bucket list
     *
     * @param bitSets input as an array of bit sets
     */
    public void prepare(final BitSet[] bitSets) {

        assert bitSets.length == N;

        // parse matrix and update TOTAL_BIT_COUNTER , BIT_2_COMBINATIONS_COUNTERS
        for (int i = 0; i < N; i++) {
            updateCounters(bitSets[i]);
            System.out.print(String.format("\rUpdating counters :  %2d%% ", (int) ((double) i / N * 100)));
        }
        System.out.print("\r\tUpdating counters :  100% \n");

        ROOT_NODE = new TwoPassMBTNode(bitSets.length);
        BUCKETS = new ArrayList<Bucket>();
    }

    /**
     * PREPARE - Update counters (TOTAL_BIT_COUNTER and , BIT_2_COMBINATIONS_COUNTERS)
     *
     *  @param bitset a bitset
     */
    public void updateCounters(final BitSet bitset) {
        int bit = 0;
        Combinations CL2 = new Combinations(L, 2);
        update_TOTAL_BIT_COUNTER(bit, bitset.get(bit));
        for (int[] c : CL2) {
            if(c[1] > bit) {
                bit = c[1];
                update_TOTAL_BIT_COUNTER(bit,bitset.get(bit));
            }
            update_BIT_2_COMBINATIONS_COUNTERS(c,new boolean[]{bitset.get(c[0]),bitset.get(c[1])});
        }
    }

    /**
     * PREPARE - Update total bit counter
     *
     * @param bit bit
     * @param bitValue bit value
     */
    private void update_TOTAL_BIT_COUNTER(final int bit, final boolean bitValue) {
        TOTAL_BIT_COUNTER[bit] += (bitValue)?1:0;
    }

    /**
     * PREPARE - Update bit 2 combinations counters
     *
     * @param bits bit pair
     * @param bitValues bit pair values
     */
    private void update_BIT_2_COMBINATIONS_COUNTERS(final int[] bits, final boolean bitValues[]) {
        int rank = CombinatoricsRanking.rank2Combination(bits);
        int counterToUpdate = selectCounter(bitValues);
        BIT_2_COMBINATIONS_COUNTERS[rank][counterToUpdate]++ ;
    }

    /**
     * RUN - Build MBT from the counters
     */
    public void run() {
        assert ROOT_NODE != null;
        assert BUCKETS != null;
        final Queue<Node> Q = new LinkedList<Node>();
    }

    /**
     * RESULT - Return buckets.
     *
     * @return buckets
     */
    public List<Bucket> getBUCKETS() {
        return BUCKETS;
    }

    /**
     * Select counter based on the bit value pair.
     *
     * |{bitValues[1],bitValues[0]}| r |
     * |-------------|-------------|---|
     * |    false    |      false  | 0 |
     * |    false    |      true   | 1 |
     * |    true     |      false  | 2 |
     * |    true     |      true   | 3 |
     * @param bitValues a pair of bitvalues
     * @return 0 or 1 or 2 or 3
     */
    private static int selectCounter(final boolean[] bitValues) {
        return (bitValues[0] ? 1 : 0) + (bitValues[1] ? 2 : 0);
    }

    public int[] getTOTAL_BIT_COUNTER() {
        return TOTAL_BIT_COUNTER;
    }

    public int[][] getBIT_2_COMBINATIONS_COUNTERS() {
        return BIT_2_COMBINATIONS_COUNTERS;
    }

    /**
     * Multi Bit Tree node for the TwoPassMBT algorithm
     */
    public class TwoPassMBTNode extends Node {

        public TwoPassMBTNode(int count) {
            super(count);
        }

        public TwoPassMBTNode(int count, List<Integer> splitBits, List<Boolean> splitBitValues) {
            super(count,splitBits,splitBitValues);
        }

        @Override
        public String toString() {
            return "TwoPassMBTNode{" +
                    super.toString() +
                    '}';
        }
    }
}
