package gr.upatras.ceid.pprl.matching;

import gr.upatras.ceid.pprl.encoding.BloomFilter;

import java.util.Arrays;

/**
 * Private similarity utility class.
 */
public class PrivateSimilarityUtil {

    public static final String[] SIMILARITY_METHOD_NAMES = { // Available private similarity methods.
            "jaccard",
            "hamming",
            "dice"
    };

    public static final String DEFAULT_SIMILARITY_METHOD_NAME = SIMILARITY_METHOD_NAMES[0]; // Default method.

    /**
     * Returns true if the similarity satisfies threshold.
     *
     * @param bf1 bloom filter 1.
     * @param bf2 bloom filter 2.
     * @param threshold threshold (depends on the similarity method).
     * @return true if bloomfilters are similar, false other wise.
     */
    public static boolean similarity(final BloomFilter bf1, final BloomFilter bf2,
                                     final double threshold) {
        return similarity(DEFAULT_SIMILARITY_METHOD_NAME,bf1,bf2,threshold);
    }

    /**
     * Does nothing if method name is supported, throws exception othewise.
     *
     * @param name method name.
     */
    public static void methodNameSupported(final String name) {
        if(!Arrays.asList(SIMILARITY_METHOD_NAMES).contains(name))
            throw new UnsupportedOperationException("Method name \"" + name +"\" does not belong in available methods.");
    }

    /**
     * Returns true if the similarity threshold.
     *
     * @param name similiarity method name
     * @param bf1 bloom filter 1.
     * @param bf2 bloom filter 2.
     * @param threshold threshold (depends on the similarity method).
     * @return true if surpess or respects threshold.
     */
    public static boolean similarity(final String name, final BloomFilter bf1, final BloomFilter bf2,
                                     final double threshold) {
        if(name.equals("jaccard")) {
            assert threshold > 0.0 && threshold <= 1.0;
            return jaccard(bf1,bf2) >= threshold;
        }
        if(name.equals("hamming")) {
            assert threshold > 1.0;
            return hamming(bf1,bf2) <= threshold;
        }
        if(name.equals("dice")) {
            assert threshold > 0.0 && threshold <= 1.0;
            return dice(bf1,bf2) >= threshold;
        }
        throw new UnsupportedOperationException("Unsupported Matching for name " + name + " .");
    }

    /**
     * Calculate the Jaccard coefficient.
     *
     * @param bf1 bloom filter 1.
     * @param bf2 bloom filter 2.
     * @return the jaccard coefficient.
     */
    public static double jaccard1(final BloomFilter bf1, final BloomFilter bf2) {
        final int N = bf1.getN();
        final int[] m = new int[4]; /* m[0] = m00 number of both 0 at any bit i on bf1 and bf2.
                                       m[1] = m01 number of 1 in bf1 and 0 in bf2 at any bit i.
                                       m[2] = m10 number of 0 in bf1 and 1 in bf2 at any bit i.
                                       m[3] = m11 number of both 1 at any bit i on bf1 and bf2.
                                     */
        for (int i = 0; i < N; i++)
            m[vector2Index(new boolean[]{bf1.getBit(i),bf2.getBit(i)})]++;

        return (double) m[3]/((double)(m[1] + m[2] + m[3]));
    }

    /**
     * Calculate the Jaccard coefficient.
     *
     * @param bf1 bloom filter 1.
     * @param bf2 bloom filter 2.
     * @return the jaccard coefficient.
     */
    public static double jaccard(final BloomFilter bf1, final BloomFilter bf2) {
        byte[] ba1 = bf1.getByteArray();
        byte[] ba2 = bf2.getByteArray();
        assert ba1.length == ba2.length;
        int interCardinality = 0;
        int unionCardinality = 0;
        for (int i = 0; i < ba1.length; i++) {
            byte b1 = ba1[i];
            byte b2 = ba2[i];
            byte and = (byte)(0xff & (b1 & b2));
            byte or = (byte)(0xff & (b1 | b2));
            for (int j = 0; j < 8; j++) {
                if(((1 << j) & and) != 0) interCardinality++;
                if(((1 << j) & or) != 0) unionCardinality++;
            }
        }

        return (double) interCardinality/(double) unionCardinality;
    }

    /**
     * Calculate the Hamming distance.
     *
     * @param bf1 bloom filter 1.
     * @param bf2 bloom filter 2.
     * @return the hamming distance.
     */
    public static int hamming(final BloomFilter bf1, final BloomFilter bf2) {
        byte[] ba1 = bf1.getByteArray();
        byte[] ba2 = bf2.getByteArray();
        assert ba1.length == ba2.length;
        int h = 0;
        for (int i = 0; i < ba1.length; i++) {
            byte b1 = ba1[i];
            byte b2 = ba2[i];
            byte xor= (byte)(0xff & (b1 ^ b2));
            for (int j = 0; j < 8; j++) {
                if(((1 << j) & xor) != 0) h++;
            }
        }
        return h;
    }

    /**
     * Calculate the Hamming distance.
     *
     * @param bf1 bloom filter 1.
     * @param bf2 bloom filter 2.
     * @return the hamming distance.
     */
    public static int hamming1(final BloomFilter bf1, final BloomFilter bf2){
        int N = bf1.getN();
        int h = 0;
        for (int i = 0; i < N; i++) {
            if(bf1.getBit(i) && !bf2.getBit(i) ||
                    !bf1.getBit(i) && bf2.getBit(i)) {
                h++;
            }
        }
        return h;
    }

    /**
     * Calculate the Dice coefficient.
     *
     * @param bf1 bloom filter 1.
     * @param bf2 bloom filter 2.
     * @return the Dice coefficient.
     */
    public static double dice1(BloomFilter bf1, BloomFilter bf2) {
        final int[] cardinalities = new int[2];
        cardinalities[0] = bf1.countOnes();
        cardinalities[1] = bf2.countOnes();
        int m11 = 0; // m11 number of both 1 at any bit i on bf1 and bf2.
        int N = bf1.getN();
        for (int i = 0; i < N; i++)
            if(bf1.getBit(i) && bf2.getBit(i)) m11++;

        return (double) (2*m11) / ((double)cardinalities[0] + cardinalities[1]);
    }

    /**
     * Calculate the Dice coefficient.
     *
     * @param bf1 bloom filter 1.
     * @param bf2 bloom filter 2.
     * @return the Dice coefficient.
     */
    public static double dice(BloomFilter bf1, BloomFilter bf2) {
        final int[] cardinalities = new int[2];
        byte[] ba1 = bf1.getByteArray();
        byte[] ba2 = bf2.getByteArray();
        assert ba1.length == ba2.length;
        int interCardinality = 0;
        for (int i = 0; i < ba1.length; i++) {
            byte b1 = ba1[i];
            byte b2 = ba2[i];
            byte and = (byte) (0xff & (b1 & b2));
            for (int j = 0; j < 8; j++) {
                if (((1 << j) & and) != 0) interCardinality++;
                if (((1 << j) & b1) != 0) cardinalities[0]++;
                if (((1 << j) & b2) != 0) cardinalities[1]++;
            }
        }
        return (double) 2*interCardinality / ((double) cardinalities[0] + cardinalities[1]);
    }

    public static int interCardinality(BloomFilter bf1, BloomFilter bf2) {
        int interCardinality = 0;
        byte[] ba1 = bf1.getByteArray();
        byte[] ba2 = bf2.getByteArray();
        assert ba1.length == ba2.length;
        for (int i = 0; i < ba1.length; i++) {
            byte b1 = ba1[i];
            byte b2 = ba2[i];
            byte and = (byte) (0xff & (b1 & b2));
            for (int j = 0; j < 8; j++) {
                if (((1 << j) & and) != 0) interCardinality++;
            }
        }
        return interCardinality;
    }

        /**
         * Returns index of the vector in the frequency array.
         *
         * @param row boolean array (a similarity vector).
         * @return index of the vector in the frequency array.
         */
    private static int vector2Index(final boolean[] row) {
        int index = 0;
        for(int i = 0; i < row.length; i ++) if(row[i]) index |= (1 << i);
        return index;
    }
}
