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
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hamming LSH blocking class.
 */
public class HammingLSHBlocking {

    private HammingLSHBlockingGroup[] blockingGroups;

    private Map<BitSet,List<Integer>>[] aliceBuckets; // the blocking buckets
    private Map<BitSet,List<Integer>>[] bobBuckets;

    private String[] aliceEncodingFieldNames; //encoding field names for A and B
    private String[] bobEncodingFieldNames;

    private Map<RecordIdPair,Integer> pairColisionCounter; // counter for pair colision

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
     * Initialization method.
     */
    public void initializeBlockingBuckets() {
        assert aliceEncodingFieldNames != null;
        assert bobEncodingFieldNames != null;
        int L = blockingGroups.length;
        aliceBuckets = new HashMap[L];
        bobBuckets = new HashMap[L];
        for (int i = 0; i < L; i++) {
            aliceBuckets[i] = new HashMap<BitSet,List<Integer>>();
            bobBuckets[i] = new HashMap<BitSet,List<Integer>>();
        }
        pairColisionCounter = new HashMap<RecordIdPair,Integer>();
    }

    /**
     * Block records of two encoded datasets.
     *
     * @param aliceRecords alice records.
     * @param bobRecords bob records.
     * @throws BlockingException
     */
    public void blockRecords(final GenericRecord[] aliceRecords,
                             final GenericRecord[] bobRecords)
            throws BlockingException {
        if(aliceBuckets == null || bobBuckets == null) throw new BlockingException("Blocking buckets not initialized");

        blockRecords(aliceRecords,blockingGroups,
                aliceBuckets,aliceEncodingFieldNames,N);

        blockRecords(bobRecords, blockingGroups,
                bobBuckets, bobEncodingFieldNames, N);

    }

    public List<RecordIdPair> matchFrequentPairs(final GenericRecord[] aliceRecords,
                                                 final GenericRecord[] bobRecords,
                                                 final List<RecordIdPair> frequentPairs,
                                                 final int threshold) throws BlockingException {
        List<RecordIdPair> matchedPairs = new ArrayList<RecordIdPair>();
        assert aliceEncodingFieldNames.length == bobEncodingFieldNames.length;
        boolean singleBf = (aliceEncodingFieldNames.length == 1);
        for(RecordIdPair pair : frequentPairs) {
            if(pair.aliceId >= aliceRecords.length || pair.bobId >= bobRecords.length)
                throw new BlockingException("Invalid index found.");
            final BloomFilter bf1 = (singleBf) ?
                    singleBloomFilter(aliceRecords[pair.aliceId],aliceEncodingFieldNames,N) :
                    multipleBloomFilters(aliceRecords[pair.aliceId],aliceEncodingFieldNames,N);
            final BloomFilter bf2 = (singleBf) ?
                    singleBloomFilter(bobRecords[pair.bobId],bobEncodingFieldNames,N) :
                    multipleBloomFilters(bobRecords[pair.bobId],bobEncodingFieldNames,N);
            if(PrivateSimilarityUtil.similarity("hamming",bf1,bf2,threshold))
                matchedPairs.add(pair);
        }
        return matchedPairs;
    }

    /**
     * Perform colision count.
     */
    public void countPairColisions() {
        final int L = blockingGroups.length;

        for(int i = 0; i < L ; i++) {
            Map<BitSet,List<Integer>> aliceBucket = aliceBuckets[i];
            Map<BitSet,List<Integer>> bobBucket = bobBuckets[i];

            final Set<BitSet> keys = new HashSet<BitSet>();
            keys.addAll(aliceBucket.keySet());
            keys.addAll(bobBucket.keySet());

            for(BitSet key : keys) {
                if(!aliceBucket.containsKey(key) || !bobBucket.containsKey(key))
                    continue;
                for (int aid : aliceBucket.get(key))
                    for (int bid : bobBucket.get(key))
                        increaseColisionCount(new RecordIdPair(aid, bid),pairColisionCounter);
            }
        }
    }

    /**
     * Retrieve frequent pairs (found in many buckets).
     *
     * @param C int collision threshhold
     * @return a list with frequent pairs (more or eq than C colisions)
     */
    public List<RecordIdPair> retrieveFrequentPairs(final int C) {
        List<RecordIdPair> retainedPairs = new ArrayList<RecordIdPair>();
        for(Map.Entry<RecordIdPair,Integer> pair : pairColisionCounter.entrySet())
            if(pair.getValue() >= C) retainedPairs.add(pair.getKey());
        return retainedPairs;
    }

    /**
     * Block records of an encoded dataset.
     *
     * @param records records.
     * @param blockingGroups blocking groups.
     * @param buckets blocking buckets.
     * @param encodingFieldNames encoding field names for bf.
     * @param N total length of bf.
     * @throws BlockingException
     */
    private static void blockRecords(final GenericRecord[] records,
                                     final HammingLSHBlockingGroup[] blockingGroups,
                                     final Map<BitSet,List<Integer>>[] buckets,
                                     final String[] encodingFieldNames,
                                     final int N) throws BlockingException {
        assert blockingGroups.length == buckets.length;
        boolean singleBf = (encodingFieldNames.length == 1);
        for(int r=0; r < records.length; r++) {
            final BloomFilter bf = (singleBf) ?
                    singleBloomFilter(records[r],encodingFieldNames,N) :
                    multipleBloomFilters(records[r],encodingFieldNames,N);
            for(int l = 0 ; l < blockingGroups.length ; l++)
                addToBucket(blockingGroups[l].hash(bf),r,buckets[l]);
        }
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
     * Increase colision count.
     * @param pair a record id pair instance.
     * @param pairCounter record pair counters
     */
    private static void increaseColisionCount(final RecordIdPair pair,
                                              final Map<RecordIdPair,Integer> pairCounter) {
        if(!pairCounter.containsKey(pair)) pairCounter.put(pair,0);
        pairCounter.put(pair, pairCounter.get(pair) + 1);
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
    private static class HammingLSHBlockingGroup {
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

        public BitSet hash(final BloomFilter bloomFilter) {
            final BitSet key = new BitSet(bits.length);
            for (int i=0; i < bits.length ;i++)
                key.set(i,bloomFilter.getBit(bits[i]));
            return key;
        }
    }
}
