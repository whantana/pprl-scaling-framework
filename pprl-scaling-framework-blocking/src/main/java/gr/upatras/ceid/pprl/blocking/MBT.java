package gr.upatras.ceid.pprl.blocking;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Single Bit Tree algorithm implementation.
 *
 * @see <a href="http://cs.anu.edu.au/~Peter.Christen/publications/ranbaduge2014multiparty.pdf">Tree Based Scalable Indexing for Multi-Party Privacy-Preserving Record Linkage</a>
 * @see <a href="http://www.almob.org/content/pdf/1748-7188-5-9.pdf">A tree-based method for the rapid screening of chemical fingerprints</a>
 */
public class MBT implements BTAlgorithm{

    private final int N;
    private final int L;
    private final int MIN_COUNT;
    private final int MAX_COUNT;
    private SBTNode ROOT_NODE = null;
    private List<Bucket> BUCKETS = null;

    public MBT(int N, int L, int MIN_COUNT, int MAX_COUNT) {
        this.N = N;
        this.L = L;
        this.MIN_COUNT = MIN_COUNT;
        this.MAX_COUNT = MAX_COUNT;
    }

    private static int[] createBitCounter(final SBTNode node, final int length) {
        final int[] counter = new int[length];
        for (BitSet bs : node.getBitSets()) {
            for (int i = 0; i < length; i++) {
                if (bs.get(i)) counter[i]++;
            }
        }
        return counter;
    }

    private static int chooseBestBit(final int bitCounter[], final int count) {
        double minf = (double) UNDEFINED;
        double f[] = new double[bitCounter.length];
        int chosenBit = UNDEFINED;

        for (int i = 0; i < bitCounter.length; i++) {
            if (bitCounter[i] != IGNORE_BIT) {
                double cardinality = (double) bitCounter[i];
                f[i] = Math.abs(0.5d - cardinality / count);
                if (minf > f[i] || minf == -1.0d) {
                    minf = f[i];
                    chosenBit = i;
                }
            } else {
                f[i] = -1d;
            }
        }
        return chosenBit;
    }

    /**
     * PREPARE - Set root node and buckets list(TOTAL_BIT_COUNTER and , BIT_2_COMBINATIONS_COUNTERS)
     *
     * @param bitSets input as an array of bit sets
     */
    public void prepare(BitSet[] bitSets) {
        assert bitSets.length == N;
        ROOT_NODE = new SBTNode(bitSets);
        BUCKETS = new ArrayList<Bucket>();
    }

    /**
     * RUN - Build an MBT from the nodes
     */
    public void run() {
        assert ROOT_NODE != null;
        assert BUCKETS != null;

        final Queue<SBTNode> Q = new LinkedList<SBTNode>();
        Q.add(ROOT_NODE);
        while (!Q.isEmpty()) {
            final SBTNode n = Q.remove();

            // find best split bit
            int[] bitCounter = createBitCounter(n, L);
            int splitBit = chooseBestBit(bitCounter, n.getCount());

            if(splitBit == UNDEFINED) {
                // all bits are used apparently so no split happening.
                // Marking node as bucket
                BUCKETS.add(new Bucket(n));
                continue;
            }

            BitSet[][] splitBitSets = n.getBitSetSplit(
                    splitBit,                           // splitbit
                    bitCounter[splitBit] ,              // bitset count with split bit == 1
                    n.getCount() - bitCounter[splitBit] // bitset count with split bit == 0
            );

            // node containg bitsets with splitBit == 0
            SBTNode n0 = new SBTNode(splitBitSets[0]);
            n0.getSplitBits().addAll(n.getSplitBits());
            n0.getSplitBits().add(splitBit);
            n0.getSplitBitValues().addAll(n.getSplitBitValues());
            n0.getSplitBitValues().add(false);

            // node containg bitsets with splitBit == 1
            SBTNode n1 = new SBTNode(splitBitSets[1]);
            n1.getSplitBits().addAll(n.getSplitBits());
            n1.getSplitBits().add(splitBit);
            n1.getSplitBitValues().addAll(n.getSplitBitValues());
            n1.getSplitBitValues().add(true);

            if(n0.getCount() >= MIN_COUNT && n1.getCount() >= MIN_COUNT) {
                if(n0.getCount() > MAX_COUNT) Q.add(n0); else BUCKETS.add(new Bucket(n0));
                if(n1.getCount() > MAX_COUNT) Q.add(n1); else BUCKETS.add(new Bucket(n1));
            }
        }
    }

    public List<Bucket> getBUCKETS() {
        return BUCKETS;
    }

    public class SBTNode extends Node {

        private BitSet[] bitSets;

        public SBTNode(int count) {
            super(count);
            bitSets = new BitSet[count];
        }

        public SBTNode(final BitSet[] bitSets) {
            super(bitSets.length);
            this.bitSets = bitSets;
        }

        public SBTNode(int count, List<Integer> splitBits, List<Boolean> splitBitValues) {
            super(count,splitBits,splitBitValues);
            bitSets = new BitSet[count];
        }

        public SBTNode(final BitSet[] bitSets, List<Integer> splitBits, List<Boolean> splitBitValues) {
            super(bitSets.length,splitBits,splitBitValues);
            this.bitSets = bitSets;
        }

        public BitSet[] getBitSets() {
            return bitSets;
        }

        public BitSet[][] getBitSetSplit(final int splitBit, final int ones, final int zeros) {
            BitSet[][] splittedBitSets = new BitSet[2][];
            splittedBitSets[0] = new BitSet[zeros];
            splittedBitSets[1] = new BitSet[ones];
            int o=0;
            int z=0;
            for(BitSet b : bitSets) {
                if(b.get(splitBit)) splittedBitSets[1][o++] = b;
                else splittedBitSets[0][z++] = b;
            }
            return splittedBitSets;
        }

        @Override
        public String toString() {
            return "TwoPassMBTNode{" +
                    super.toString() +
                    '}';
        }
    }
}
