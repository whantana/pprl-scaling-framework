package gr.upatras.ceid.pprl.blocking;

import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import org.apache.avro.generic.GenericRecord;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Hamming LSH blocking class.
 */
public class HammingLSHBlocking {

    private HammingLSHBlockingGroup[] blockingGroups;

    private Map<BitSet,List<Integer>>[] buckets; // blocking buckets for bob records.

    private String aliceEncodingName;
    private String bobEncodingName;

    private String aliceEncodingFieldName; //encoding field names for A and B
    private String bobEncodingFieldName;

    private int N; // bloom filter length

    private int K; // number of keys
    private int L; // number of groups;

    /**
     * Constructor.
     *
     * @param L number of blocking groups.
     * @param K number of hash functions.
     * @param aliceEncoding alice encoding.
     * @param bobEncoding bob encoding.
     * @throws BlockingException
     */
    public HammingLSHBlocking(final int L, final int K,
                              final BloomFilterEncoding aliceEncoding,
                              final BloomFilterEncoding bobEncoding)
            throws BlockingException {
        setup(aliceEncoding, bobEncoding);
        this.L = L;
        this.K = K;
        blockingGroups = new HammingLSHBlockingGroup[L];
        for (int i = 0; i < L; i++)
            blockingGroups[i] = new HammingLSHBlockingGroup(String.format("Block#%d", i), K, N);
    }

    /**
     * Constructor
     *
     * @param L number of blocking groups.
     * @param K number of hash functions.
     * @param N number of bits in bloom filter encoding.
     * @param aliceEncodingName  alice encoding schema name.
     * @param aliceEncodingFieldName alice encoding field name.
     * @param bobEncodingName bob encoding schema name.
     * @param bobEncodingFieldName bob encoding field name.
     * @throws BlockingException
     */
    public HammingLSHBlocking(final int L, final int K,
                              final int N,
                              final String aliceEncodingName,
                              final String aliceEncodingFieldName,
                              final String bobEncodingName,
                              final String bobEncodingFieldName)
            throws BlockingException {
        this.aliceEncodingName = aliceEncodingName;
        this.bobEncodingName = bobEncodingName;
        this.aliceEncodingFieldName = aliceEncodingFieldName;
        this.bobEncodingFieldName = bobEncodingFieldName;
        this.N = N;
        this.L = L;
        this.K = K;
        blockingGroups = new HammingLSHBlockingGroup[L];
        for (int i = 0; i < L; i++)
            blockingGroups[i] = new HammingLSHBlockingGroup(String.format("Block#%d", i), K, N);
    }

    /**
     * Constructor.
     *
     * @param blockingKeys string representation of blocking keys.
     * @param aliceEncoding alice encoding.
     * @param bobEncoding bob encoding.
     * @throws BlockingException
     */
    public HammingLSHBlocking(final String[] blockingKeys,
                              final BloomFilterEncoding aliceEncoding,
                              final BloomFilterEncoding bobEncoding) throws BlockingException {
        setup(aliceEncoding, bobEncoding);
        L = blockingKeys.length;
        blockingGroups = new HammingLSHBlockingGroup[blockingKeys.length];
        for (int i = 0; i < blockingKeys.length; i++) {
            String[] parts = blockingKeys[i].split(" ");
            if(K == 0) K = parts.length - 1;
            else assert K ==parts.length - 1;
            Integer[] bits = new Integer[parts.length - 1];
            for (int j = 0; j < (parts.length - 1); j++)
                bits[j] = Integer.valueOf(parts[j+1]);
            final String id = parts[0];
            blockingGroups[i] = new HammingLSHBlockingGroup(id,bits);
        }
    }

    /**
     * Constructor.
     *
     * @param blockingKeys string representation of blocking keys.
     * @param N number of bits in bloom filter encoding.
     * @param aliceEncodingName  alice encoding schema name.
     * @param aliceEncodingFieldName alice encoding field name.
     * @param bobEncodingName bob encoding schema name.
     * @param bobEncodingFieldName bob encoding field name.
     * @throws BlockingException
     */
    public HammingLSHBlocking(final String[] blockingKeys,
                              final int N,
                              final String aliceEncodingName,
                              final String aliceEncodingFieldName,
                              final String bobEncodingName,
                              final String bobEncodingFieldName)
            throws BlockingException {
        this.N = N;
        this.aliceEncodingName = aliceEncodingName;
        this.bobEncodingName = bobEncodingName;
        this.aliceEncodingFieldName = aliceEncodingFieldName;
        this.bobEncodingFieldName = bobEncodingFieldName;
        L = blockingKeys.length;
        blockingGroups = new HammingLSHBlockingGroup[blockingKeys.length];
        for (int i = 0; i < blockingKeys.length; i++) {
            String[] parts = blockingKeys[i].split(" ");
            if(K == 0) K = parts.length - 1;
            else assert K ==parts.length - 1;
            Integer[] bits = new Integer[parts.length - 1];
            for (int j = 0; j < (parts.length - 1); j++)
                bits[j] = Integer.valueOf(parts[j+1]);
            final String id = parts[0];
            blockingGroups[i] = new HammingLSHBlockingGroup(id,bits);
        }
    }

    /**
     * Initialization method.
     */
    public void initialize() {
        assert L == blockingGroups.length;
        buckets = new HashMap[L];
        for (int i = 0; i < L; i++) {
            buckets[i] = new HashMap<BitSet,List<Integer>>();
        }
    }

    /**
     * Run the FPS method.
     *
     * @param aliceRecords alice records.
     * @param bobRecords bob records.
     * @param C collision limit ( if >= C a pair is frequent).
     * @param similarityMethodName private similarity method name (supported hamming,jaccard,dice).
     * @param similarityThreshold hamming threshold for matching.
     * @return Matched record pair list.
     * @throws BlockingException
     */
    public HammingLSHBlockingResult runFPS(final GenericRecord[] aliceRecords,
                                           final String aliceUidFieldName,
                                           final GenericRecord[] bobRecords,
                                           final String bobUidFieldName,
                                           final short C,
                                           final String similarityMethodName,
                                           final double similarityThreshold) throws BlockingException {
        if(buckets == null) throw new BlockingException("FPS : Blocking buckets not initialized");
        // hash bob records into the buckets.
        for(int r=0; r < bobRecords.length; r++) {
            BitSet[] keys = hashRecord(bobRecords[r], bobEncodingFieldName);
            for(int l = 0 ; l < keys.length ; l++)
                addToBucket(keys[l],r, buckets[l]);
        }
        System.out.println("\rBlocking bob records...DONE");

        // count collisions of Alice's bloom filters in bobs buckets
        List<RecordIdPair> mathcedPairs = new LinkedList<RecordIdPair>();
        int matchedPairsCount = 0;
        int frequentPairsCount = 0;
        final short[] collisions = new short[bobRecords.length]; // use a HashBag?
        System.out.print("Counting colission with alice records (0/" + aliceRecords.length + ").");
        for(int aid=0; aid < aliceRecords.length; aid++) {
            System.out.print("\rCounting collisions with alice records (" + (aid+1)
                    + "/" + aliceRecords.length + ")." +
                    " Frequent pairs so far :" + frequentPairsCount + "." +
                    " Matched pairs so far : " + matchedPairsCount + ".");
            final BitSet[] keys = hashRecord(aliceRecords[aid], aliceEncodingFieldName);
            Arrays.fill(collisions, (short) 0);
            for (int l = 0; l < blockingGroups.length; l++) {
                final List<Integer> bobIds = buckets[l].get(keys[l]);
                if(bobIds == null) continue;
                for (int bid : bobIds) {
                    if(collisions[bid] < 0) continue;
                    collisions[bid]++;
                    if(collisions[bid] >= C) {
                        frequentPairsCount++;
                        final BloomFilter bf1 = BloomFilterEncodingUtil.retrieveBloomFilter(aliceRecords[aid],
                                aliceEncodingFieldName, N);
                        final BloomFilter bf2 = BloomFilterEncodingUtil.retrieveBloomFilter(bobRecords[bid],
                                bobEncodingFieldName, N);
                        if(PrivateSimilarityUtil.similarity(similarityMethodName,bf1,bf2, similarityThreshold)) {
                            mathcedPairs.add(
                                    new RecordIdPair(
                                            String.valueOf(aliceRecords[aid].get(aliceUidFieldName)),
                                            String.valueOf(bobRecords[bid].get(bobUidFieldName))
                                    )
                            );
                            matchedPairsCount++;
                        }
                        collisions[bid] = -1;
                    }
                }
            }
        }
        System.out.println("");

        return new HammingLSHBlockingResult(mathcedPairs,
                matchedPairsCount, frequentPairsCount);
    }

    /**
     * Returns a string array for the blocking groups.
     *
     * @return a string array for the blocking groups.
     */
    public String[] groupsAsStrings() {
        final String[] array = new String[blockingGroups.length];
        for (int i = 0; i < blockingGroups.length; i++) {
            array[i] = blockingGroups[i].toString();
        }
        return array;
    }


    /**
     * True if Alice's record. False otherwise.
     *
     * @param record a generic record
     * @return True if Alice's record. False otherwise.
     */
    public boolean isAliceRecord(final GenericRecord record) {
        return record.getSchema().getName().equals(aliceEncodingName);
    }

    /**
     * True if Bob's record. False otherwise.
     *
     * @param record a generic record
     * @return True if Bob's record. False otherwise.
     */
    public boolean isBobRecord(final GenericRecord record) {
        return record.getSchema().getName().equals(bobEncodingName);
    }

    /**
     * Hash a record according to its schema.
     *
     * @param record a generic record
     * @return keys from hashing.
     * @throws BlockingException
     */
    public BitSet[] hashRecord(final GenericRecord record) throws BlockingException {
        if(isAliceRecord(record)) return hashAliceRecord(record);
        else if(isBobRecord(record)) return hashBobRecord(record);
        else throw new BlockingException("No alice or bob record.");
    }


    /**
     * Hash an alice's record.
     *
     * @param record a generic record
     * @return keys from hashing.
     */
    public BitSet[] hashAliceRecord(final GenericRecord record) {
        return hashRecord(record,aliceEncodingFieldName);
    }

    /**
     * Hash bob's record.
     *
     * @param record a generic record
     * @return keys from hashing.
     */
    public BitSet[] hashBobRecord(final GenericRecord record) {
        return hashRecord(record,bobEncodingFieldName);
    }


    /**
     * Hash a record.
     *
     * @param record a generic record.
     * @param encodingFieldName encoding field name.
     * @return keys from hashing.
     */
    public BitSet[] hashRecord(final GenericRecord record,
                               final String encodingFieldName)  {
        final BloomFilter bf = BloomFilterEncodingUtil.retrieveBloomFilter(record, encodingFieldName, N);
        final BitSet[] keys = new BitSet[blockingGroups.length];
        for (int l = 0; l < blockingGroups.length; l++)
            keys[l] = blockingGroups[l].hash(bf);
        return keys;
    }

    /**
     * Add key and index to bucket.
     *
     * @param key key.
     * @param index index.
     * @param bucket bucket.
     */
    private static void addToBucket(final BitSet key, final int index,
                                    final Map<BitSet,List<Integer>> bucket) {
        if(!bucket.containsKey(key)) {
            bucket.put(key, new ArrayList<Integer>());
        }
        bucket.get(key).add(index);
    }

    /**
     * Returns K.
     *
     * @return K (number of keys).
     */
    public int getK() {
        return K;
    }

    /**
     * Returns L.
     *
     * @return L (number of keys).
     */
    public int getL() {
        return L;
    }

    /**
     * Return alice encoding name.
     *
     * @return alice encoding name.
     */
    public String getAliceEncodingName() {
        return aliceEncodingName;
    }

    /**
     * Return bob encoding name.
     *
     * @return bob encoding name.
     */
    public String getBobEncodingName() {
        return bobEncodingName;
    }

    /**
     * Return alice encoding field name
     *
     * @return alice encoding field name
     */
    public String getAliceEncodingFieldName() {
        return aliceEncodingFieldName;
    }

    /**
     * Return bob encoding field name
     *
     * @return bob encoding field name
     */
    public String getBobEncodingFieldName() {
        return bobEncodingFieldName;
    }

    /**
     * Setup blocking.
     *
     * @param aliceEncoding A-lice encoding.
     * @param bobEncoding B-ob encoding.
     * @throws BlockingException
     */
    private void setup(final BloomFilterEncoding aliceEncoding,
                      final BloomFilterEncoding bobEncoding)
            throws BlockingException {
        final String schemeName = aliceEncoding.schemeName();
        if(!schemeName.equals(bobEncoding.schemeName()))
            throw new BlockingException("Encoding scheme names do not match.");
        BloomFilterEncodingUtil.schemeNameSupported(schemeName);

        aliceEncodingName = aliceEncoding.getEncodingSchema().getName();
        bobEncodingName = bobEncoding.getEncodingSchema().getName();
        if(aliceEncodingName.equals(bobEncodingName))
            throw new BlockingException("Encodings share the same schema name. Must differ.");

        aliceEncodingFieldName = aliceEncoding.getEncodingFieldName();
        bobEncodingFieldName = bobEncoding.getEncodingFieldName();

        int aliceN = aliceEncoding.getBFN();
        int bobN = bobEncoding.getBFN();

        if(aliceN != bobN) throw new BlockingException("Encodings total bloom filter length is different.");

        N = aliceN;
    }

    public class HammingLSHBlockingResult {
        private final List<RecordIdPair> matchedPairs;
        private final int matchedPairsCount;
        private final int frequentPairsCount;

        /**
         * Constructor
         *
         * @param matchedPairs matched pair list.
         * @param matchedPairsCount matched pair count.
         * @param frequentPairsCount frequent pair count.
         */
        public HammingLSHBlockingResult(final List<RecordIdPair> matchedPairs,
                                        final int matchedPairsCount,
                                        final int frequentPairsCount) {
            this.matchedPairs = matchedPairs;
            this.matchedPairsCount = matchedPairsCount;
            this.frequentPairsCount = frequentPairsCount;
        }

        /**
         * Returns matched pair list.
         *
         * @return matched pair list.
         */
        public List<RecordIdPair> getMatchedPairs() {
            return matchedPairs;
        }

        /**
         * Returns matched pairs count.
         *
         * @return matched pair count.
         */
        public int getMatchedPairsCount() {
            return matchedPairsCount;
        }

        /**
         * Returns frequent pair count.
         *
         * @return frequent pair count.
         */
        public int getFrequentPairsCount() {
            return frequentPairsCount;
        }
    }


    /**
     * Hamming LSH Blocking Group class.
     */
    private class HammingLSHBlockingGroup {
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
}
