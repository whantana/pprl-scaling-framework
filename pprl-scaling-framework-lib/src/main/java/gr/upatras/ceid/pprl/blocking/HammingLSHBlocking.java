package gr.upatras.ceid.pprl.blocking;

import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import org.apache.avro.generic.GenericData;
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

    private Map<BitSet,List<Integer>>[] bobBuckets; // blocking buckets for bob records.

    private String aliceEncodingName;
    private String bobEncodingName;

    private boolean singleBfBlocking; // true if encoding carries a single bloom filter field, false for more than 1.

    private String[] aliceEncodingFieldNames; //encoding field names for A and B
    private String[] bobEncodingFieldNames;

    private int N; // bloom filter length

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
        N = setup(aliceEncoding, bobEncoding);
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
        N = setup(aliceEncoding, bobEncoding);
        blockingGroups = new HammingLSHBlockingGroup[blockingKeys.length];
        for (int i = 0; i < blockingKeys.length; i++) {
            String[] parts = blockingKeys[i].split(" ");
            Integer[] bits = new Integer[parts.length - 1];
            for (int j = 0; j < bits.length; j++)
                bits[i] = Integer.valueOf(parts[i+1]);
            final String id = parts[0];
            blockingGroups[i] = new HammingLSHBlockingGroup(id,bits);
        }
    }

    /**
     * Initialization method.
     */
    public void initialize() {
        int L = blockingGroups.length;
        bobBuckets = new HashMap[L];
        for (int i = 0; i < L; i++) {
            bobBuckets[i] = new HashMap<BitSet,List<Integer>>();
        }
    }

    /**
     * Run the FPS method.
     *
     * @param aliceRecords alice records.
     * @param bobRecords bob records.
     * @param C collision limit ( if >= C a pair is frequent).
     * @param threshold hamming threshold for matching.
     * @return Matched record pair list.
     * @throws BlockingException
     */
    public HammingLSHBlockingResult runFPS(final GenericRecord[] aliceRecords,
                                           final GenericRecord[] bobRecords,
                                           final int C,
                                           final int threshold) throws BlockingException {
        if(bobBuckets == null) throw new BlockingException("FPS : Blocking buckets not initialized");
        // hash bob records into the buckets.
        for(int r=0; r < bobRecords.length; r++) {
            BitSet[] keys = hashBobRecord(bobRecords[r]);
            for(int l = 0 ; l < keys.length ; l++)
                addToBucket(keys[l],r,bobBuckets[l]);
        }
        System.out.println("\rBlocking bob records...DONE");

        // count collisions of Alice's bloom filters in bobs buckets
        List<RecordIdPair> mathcedPairs = new LinkedList<RecordIdPair>();
        int matchedPairsCount = 0;
        int frequentPairsCount = 0;
        final byte[] collisions = new byte[aliceRecords.length];
        System.out.print("Counting colission with alice records (0/" + aliceRecords.length + ").");
        for(int aid=0; aid < aliceRecords.length; aid++) {
            System.out.print("\rCounting collisions with alice records (" + (aid+1)
                    + "/" + aliceRecords.length + ")." +
                    " Frequent pairs so far :" + frequentPairsCount + "." +
                    " Matched pairs so far : " + matchedPairsCount + ".");
            final BitSet[] keys = hashAliceRecord(aliceRecords[aid]);
            Arrays.fill(collisions, (byte) 0);
            for (int l = 0; l < blockingGroups.length; l++) {
                final List<Integer> bobIds = bobBuckets[l].get(keys[l]);
                if(bobIds == null) continue;
                for (int bid : bobIds) {
                    if(collisions[bid] < 0) continue;
                    collisions[bid]++;
                    if(collisions[bid] >= C) {
                        frequentPairsCount++;
                        if(matchRecords(aliceRecords[aid],bobRecords[bid],threshold)) {
                            mathcedPairs.add(new RecordIdPair(aid, bid));
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
        return hashRecord(record,aliceEncodingFieldNames);
    }

    /**
     * Hash bob's record.
     *
     * @param record a generic record
     * @return keys from hashing.
     */
    public BitSet[] hashBobRecord(final GenericRecord record) {
        return hashRecord(record,bobEncodingFieldNames);
    }


    /**
     * Hash a record.
     *
     * @param record a generic record.
     * @param encodingFieldNames encoding field names.
     * @return keys from hashing.
     */
    public BitSet[] hashRecord(final GenericRecord record,
                               final String[] encodingFieldNames)  {
        final BloomFilter bf = (singleBfBlocking) ?
                singleBloomFilter(record,encodingFieldNames,N) :
                multipleBloomFilters(record,encodingFieldNames,N);
        final BitSet[] keys = new BitSet[blockingGroups.length];
        for (int l = 0; l < blockingGroups.length; l++)
            keys[l] = blockingGroups[l].hash(bf);
        return keys;
    }

    public boolean matchRecords(final GenericRecord aliceRecord,
                                final GenericRecord bobRecord,
                                final int threshold) {
        final BloomFilter bf1 = (singleBfBlocking) ?
                singleBloomFilter(aliceRecord,aliceEncodingFieldNames,N) :
                multipleBloomFilters(aliceRecord,aliceEncodingFieldNames,N);
        final BloomFilter bf2 = (singleBfBlocking) ?
                singleBloomFilter(bobRecord,bobEncodingFieldNames,N) :
                multipleBloomFilters(bobRecord,bobEncodingFieldNames,N);
        return PrivateSimilarityUtil.similarity("hamming",bf1,bf2,threshold);
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
     * Compose a bloom filter from multiple byte arrays.
     *
     * @param record generic record.
     * @param encodingFieldNames encoding field names containing bfs.
     * @param N total size of bloom filter.
     * @return a <code>BloomFilter</code> instance
     */
    private static BloomFilter multipleBloomFilters(final GenericRecord record,
                                                    final String[] encodingFieldNames,
                                                    int N) {
        byte[][] allBytes = new byte[encodingFieldNames.length][];
        int totalLen = 0;
        for(int i = 0 ; i < encodingFieldNames.length; i++) {
            allBytes[i] = ((GenericData.Fixed) record.get(encodingFieldNames[i])).bytes();
            totalLen += allBytes[i].length;
        }
        byte[] array = new byte[totalLen];
        int p = 0;
        for(int i = 0; i < encodingFieldNames.length; i++) {
            System.arraycopy(allBytes[i],0,array,p,allBytes[i].length);
            p += allBytes[i].length;
        }
        return new BloomFilter(N, array);
    }

    /**
     * Retrieve bloom filter from the encoding field name.
     *
     * @param record generic record.
     * @param encodingFieldNames only one encoding field name.
     * @param N total size of bloom filter.
     * @return a <code>BloomFilter</code> instance
     */
    private static BloomFilter singleBloomFilter(final GenericRecord record,
                                                 final String[] encodingFieldNames,
                                                 int N) {
        final String encodingFieldName = encodingFieldNames[0];
        GenericData.Fixed fixed = (GenericData.Fixed) record.get(encodingFieldName);
        return new BloomFilter(N,fixed.bytes());
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

        aliceEncodingName = aliceEncoding.getEncodingSchema().getName();
        bobEncodingName = bobEncoding.getEncodingSchema().getName();
        if(aliceEncodingName.equals(bobEncodingName))
            throw new BlockingException("Encodings share the same schema name. Must differ.");

        aliceEncodingFieldNames = aliceEncoding.getEncodingFieldNames();
        bobEncodingFieldNames = bobEncoding.getEncodingFieldNames();
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
            singleBfBlocking = true;
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
            singleBfBlocking = false;
            return aliceN;
        }
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
