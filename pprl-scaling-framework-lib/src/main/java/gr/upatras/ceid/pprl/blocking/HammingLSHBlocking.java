package gr.upatras.ceid.pprl.blocking;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hamming LSH blocking class.
 */
public class HammingLSHBlocking {

    private static Logger LOG = LoggerFactory.getLogger(HammingLSHBlocking.class);

    private HammingLSHBlockingGroup[] blockingGroups;

    private Map<BitSet,List<Integer>>[] aliceKeys;
    private Map<BitSet,List<Integer>>[] bobKeys;

    private String[] aliceEncodingFieldNames;
    private String[] bobEncodingFieldNames;


    /**
     * Constructor.
     *
     * @param L number of blocking groups.
     * @param K number of K
     * @param aliceEncoding alice encoding.
     * @param bobEncoding bob encoding.
     * @throws BlockingException
     */
    public HammingLSHBlocking(final int L, final int K,
                              final BloomFilterEncoding aliceEncoding,
                              final BloomFilterEncoding bobEncoding)
            throws BlockingException {
        // TODO a constructor that imposes order on the fields
        final int N = setup(aliceEncoding, bobEncoding);
        LOG.debug("Blocking Groups Count : {}.",L);
        LOG.debug("Number of hashes : {}.",K);
        LOG.debug("Size of BF : {}.",N);
        LOG.debug("Alice encoding fields : {}.", Arrays.toString(aliceEncodingFieldNames));
        LOG.debug("Bob encoding fields : {}.",Arrays.toString(bobEncodingFieldNames));
        blockingGroups = new HammingLSHBlockingGroup[L];
        for (int i = 0; i < L; i++)
            blockingGroups[i] = new HammingLSHBlockingGroup(String.format("Block#%d", i), K, N);
    }

    /**
     * Initialization method.
     */
    public void initializeBlockingBuckets() {
        assert aliceEncodingFieldNames != null;
        assert bobEncodingFieldNames != null;
        int L = blockingGroups.length;
        aliceKeys = new HashMap[L];
        bobKeys = new HashMap[L];
        for (int i = 0; i < L; i++) {
            aliceKeys[i] = new HashMap<BitSet,List<Integer>>();
            bobKeys[i] = new HashMap<BitSet,List<Integer>>();
        }
        LOG.debug("Blocking buckets now initialized");
    }

    /**
     * Returns the blocking groups.
     *
     * @return the blocking groups.
     */
    public HammingLSHBlockingGroup[] getBlockingGroups() {
        return blockingGroups;
    }

    /**
     * Setup blocking.
     *
     * @param aliceEncoding A-lice encoding.
     * @param bobEncoding B-ob encoding.
     * @return length of bloom filter to block.
     * @throws BlockingException
     */
    private int setup(final BloomFilterEncoding aliceEncoding,
                      final BloomFilterEncoding bobEncoding) throws BlockingException {
        final String schemeName = aliceEncoding.schemeName();
        if(!schemeName.equals(bobEncoding.schemeName()))
            throw new BlockingException("Encoding scheme names do not match.");

        aliceEncodingFieldNames = aliceEncoding.getEncodingFieldNames();
        bobEncodingFieldNames = bobEncoding.getEncodingFieldNames();
        LOG.debug("Alice : " + Arrays.toString(aliceEncodingFieldNames));
        LOG.debug("Bob : " + Arrays.toString(bobEncodingFieldNames));
        assert aliceEncodingFieldNames.length == bobEncodingFieldNames.length;

        final int fieldNameCount = aliceEncodingFieldNames.length;
        if(fieldNameCount == 1) {
            if(!(schemeName.equals("CLK") || schemeName.equals("RBF")))
                throw new BlockingException("Unsupported encoding scheme.");
            int aliceN = (schemeName.equals("CLK")) ? ((CLKEncoding)aliceEncoding).getCLKN() :
                    ((RowBloomFilterEncoding)aliceEncoding).getRBFN();
            int bobN = (schemeName.equals("CLK")) ? ((CLKEncoding)bobEncoding).getCLKN() :
                    ((RowBloomFilterEncoding)bobEncoding).getRBFN();
            if(aliceN != bobN) throw new BlockingException("Encoding length does not match.");
            return aliceN;
        } else {
            if (!schemeName.equals("FBF")) throw new BlockingException("Unsupported encoding scheme name.");

            int aliceN = 0;
            for (String fieldName : aliceEncodingFieldNames)
                aliceN += BloomFilterEncodingUtil.extractNFromEncodingField(fieldName);

            int bobN = 0;
            for (String fieldName : bobEncodingFieldNames)
                bobN += BloomFilterEncodingUtil.extractNFromEncodingField(fieldName);
            if(aliceN != bobN) throw new BlockingException("Encoding length does not match.");
            return aliceN;
        }
    }


    /**
     * Hamming LSH Blocking Group class.
     */
    public class HammingLSHBlockingGroup {
        private String id;
        private Integer[] bits;

        /**
         * Constructor.
         *
         * @param id group id.
         * @param K number of hashes.
         * @param N length of bloom filter.
         */
        public HammingLSHBlockingGroup(final String id,final int K, final int N) {
            assert K >= 1 && K < N;
            this.id = id;
            final List<Integer> bitList = new ArrayList<Integer>(N);
            for (int i = 0; i < N; i++) bitList.add(i,i);
            Collections.shuffle(bitList,new SecureRandom());
            bits = bitList.subList(0,K).toArray(new Integer[K]);
            LOG.debug("Blocking Group Id : {} , bits : {}",this.id, Arrays.toString(bits));
        }
    }
}
