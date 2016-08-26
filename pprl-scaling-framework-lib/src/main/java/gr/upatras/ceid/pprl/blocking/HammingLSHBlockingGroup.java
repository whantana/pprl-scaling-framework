package gr.upatras.ceid.pprl.blocking;

import gr.upatras.ceid.pprl.encoding.BloomFilter;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * Hamming LSH Blocking Group class.
 */
class HammingLSHBlockingGroup {
    private String id;
    private Integer[] bits;

    /**
     * Constructor.
     *
     * @param id group id.
     * @param K number of hashes.
     * @param N length of bloom filter.
     */
    public HammingLSHBlockingGroup(final String id, final int K, final int N) {
        assert K >= 1 && K < N;
        this.id = id;
        final List<Integer> bitList = new ArrayList<Integer>(N);
        for (int i = 0; i < N; i++) bitList.add(i,i);
        Collections.shuffle(bitList, new SecureRandom());
        bits = bitList.subList(0,K).toArray(new Integer[K]);
    }

    /**
     * Constructor.
     *
     * @param id group id.
     * @param bits bits.
     */
    public HammingLSHBlockingGroup(final String id, final Integer[] bits) {
        this.id = id;
        this.bits = bits;
    }

    /**
     * Hash the bloomfilter.
     *
     * @param bloomFilter <code>BloomFitler</code> instance.
     * @return a bitset as the hash value of bf
     */
    public BitSet hash(final BloomFilter bloomFilter) {
        final BitSet key = new BitSet(bits.length);
        for (int i=0; i < bits.length ;i++)
            key.set(i,bloomFilter.getBit(bits[i]));
        return key;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(id);
        for (Integer bit : bits) {
            sb.append(" ").append(bit);
        }
        return sb.toString();
    }
}
