package gr.upatras.ceid.pprl.blocking;

import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Hamming LSH blocking class.
 */
public class HammingLSHBlocking {

    private HammingLSHBlockingGroup[] blockingGroups;

    private Map<BitSet,BitSet>[] buckets; // blocking buckets for bob records.

    private String aliceEncodingName;
    private String bobEncodingName;

    private String aliceEncodingFieldName; //encoding field names for A and B
    private String bobEncodingFieldName;

    private int N; // bloom filter length
    private int K; // number of keys
    private int L; // number of groups;

    HammingLSHBlockingResult result; // resets with every initialization

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

    public void initialize(final GenericRecord[] bobRecords) throws BlockingException {
        if(result == null) result = new HammingLSHBlockingResult();
        if(buckets == null) {
            buckets = new HashMap[L];
            for (int i = 0; i < L; i++)
                buckets[i] = new HashMap<BitSet,BitSet>();
        }

        // hash bob records into the buckets.
        final long start = System.currentTimeMillis();
        System.out.print("Blocking bob records...(0%)");
        for(int r=0; r < bobRecords.length; r++) {
            System.out.format("\rBlocking bob records...(%d%%)",
                    Math.round(100*((double)r/(double)bobRecords.length)));
            BitSet[] keys = hashRecord(bobRecords[r], bobEncodingFieldName);
            for(int l = 0 ; l < L ; l++)
                addToBucket(l,keys[l],r,bobRecords.length );
        }
        System.out.println("\rBlocking bob records...(100%). DONE");
        final long stop = System.currentTimeMillis();
        result.setBobBlockingTime(stop-start);
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

        if(buckets == null) throw new BlockingException("Error at running FPS Bob's Blocking buckets not initialized");

        // count collisions of Alice's bloom filters in bobs buckets
        final long start = System.currentTimeMillis();
        final short[] collisions = new short[bobRecords.length];
        System.out.print("Counting collisions with alice records...(0%)");
        for(int aliceId=0; aliceId < aliceRecords.length; aliceId++) {
            System.out.format("\rCounting collisions with alice records...(%d%%) " +
                            "#Frequents Pairs=%d \t #Matched Pairs=%d.",
                    Math.round(100* ((aliceId+1)/ (double) aliceRecords.length)),
                    result.getFrequentPairsCount(),result.getMatchedPairsCount());
            final BitSet[] keys = hashRecord(aliceRecords[aliceId], aliceEncodingFieldName);
            Arrays.fill(collisions, (short) 0);
            for (int l = 0; l < blockingGroups.length; l++) {
                final BitSet bobIds = buckets[l].get(keys[l]);
                if(bobIds == null) continue;
                for (int bobId = bobIds.nextSetBit(0); bobId != -1; bobId = bobIds.nextSetBit(bobId + 1) ) {
                    if(collisions[bobId] < 0) continue;
                    collisions[bobId]++;
                    if(collisions[bobId] >= C) {
                        result.increaseFrequentPairsCount();
                        final BloomFilter bf1 = BloomFilterEncodingUtil.retrieveBloomFilter(aliceRecords[aliceId],
                                aliceEncodingFieldName, N);
                        final BloomFilter bf2 = BloomFilterEncodingUtil.retrieveBloomFilter(bobRecords[bobId],
                                bobEncodingFieldName, N);
                        if(PrivateSimilarityUtil.similarity(similarityMethodName,bf1,bf2, similarityThreshold)) {
                            result.addPair(
                                    String.valueOf(aliceRecords[aliceId].get(aliceUidFieldName)),
                                    String.valueOf(bobRecords[bobId].get(bobUidFieldName)));
                            result.increaseMatchedPairsCount();
                        }
                        collisions[bobId] = -1;
                    }
                }
            }
        }
        System.out.format("\rCounting collisions with alice records...(100%%) " +
                        "#Frequents Pairs=%d \t #Matched Pairs=%d. DONE\n",
                result.getFrequentPairsCount(),result.getMatchedPairsCount());
        final long stop = System.currentTimeMillis();
        result.setFpsTime(stop-start);

        return result;
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
     * Add this hash to backet
     *
     * @param blockingGroupId blocking group id
     * @param hash hash
     * @param bobIndex index
     * @param bobIdCount
     */
    private void addToBucket(final int blockingGroupId ,
                             final BitSet hash,
                             final int bobIndex, final int bobIdCount) {
        BitSet ids = buckets[blockingGroupId].get(hash);
        if(ids == null) {
            ids = new BitSet(bobIdCount);
            ids.set(bobIndex);
            buckets[blockingGroupId].put(hash, ids);
        } else ids.set(bobIndex);
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
}
