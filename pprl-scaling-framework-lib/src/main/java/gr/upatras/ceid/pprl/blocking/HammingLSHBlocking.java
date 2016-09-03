package gr.upatras.ceid.pprl.blocking;

import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import org.apache.avro.generic.GenericRecord;

import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hamming LSH blocking class.
 */
public class HammingLSHBlocking {

    private HammingLSHBlockingGroup[] blockingGroups;

    private Map<BitSet,LinkedList<char[]>>[] buckets; // blocking buckets for bob records.
    private Map<String,GenericRecord> bobRecordsMap;

    private String aliceEncodingName;
    private String bobEncodingName;

    private String aliceEncodingFieldName; //encoding field names for A and B
    private String bobEncodingFieldName;

    private int N; // bloom filter length
    private int K; // number of keys
    private int L; // number of groups;

    HammingLSHBlockingResult result;

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
     * Initialize FPS by blocking bob records.
     *
     * @param bobRecords encoded bob records.
     * @throws BlockingException
     */
    public void runHLSH(final GenericRecord[] bobRecords, final String bobUidFieldName) throws BlockingException {
        if(result == null) result = new HammingLSHBlockingResult();
        if(buckets == null) {
            buckets = new HashMap[L];
            for (int i = 0; i < L; i++) {
                buckets[i] = new HashMap<>();

            }
        }
        if(bobRecordsMap == null)
            bobRecordsMap = new HashMap<>((int) (bobRecords.length / 0.75f + 1), 0.75f);

        // get runtime
        final Runtime rt = Runtime.getRuntime();

        // hash bob records into the buckets.
        System.gc();
        final long umb = rt.totalMemory() - rt.freeMemory();
        final long start = System.currentTimeMillis();
        System.out.print("Blocking bob records...(0%)");
        for(int r=0; r < bobRecords.length; r++) {
            System.out.format("\rBlocking bob records...(%d%%)",
                    Math.round(100*((double)r/(double)bobRecords.length)));
            final String bobId = String.valueOf(bobRecords[r].get(bobUidFieldName));
            bobRecordsMap.put(bobId, bobRecords[r]);
            BitSet[] keys = hashRecord(bobRecords[r], bobEncodingFieldName);
            for(int l = 0 ; l < L ; l++)
                addToBucket(l, keys[l], bobId);
        }
        final long stop = System.currentTimeMillis();
        System.gc();
        final long uma = rt.totalMemory() - rt.freeMemory();
        final long umd = uma - umb;
        System.out.println("\rBlocking bob records...(100%). Size :" + umd/(1024*1024) + "MB.");
        result.setBobBlockingSize(umd);
        result.setBobBlockingTime(stop-start);
    }

    /**
     * Run the FPS method.
     *
     * @param aliceRecords alice records.
     * @param C collision limit ( if >= C a pair is frequent).
     * @param hammingThreshold hamming threshold for matching.
     * @return Matched record pair list.
     * @throws BlockingException
     */
    public void runFPS(final GenericRecord[] aliceRecords,
                       final String aliceUidFieldName,
                       final short C,
                       final int hammingThreshold) throws BlockingException {

        if(buckets == null) throw new BlockingException("Error at running FPS Bob's Blocking buckets not initialized");

        // count collisions of Alice's bloom filters in bobs buckets
        final long start = System.currentTimeMillis();
        final HashMap<String,Short> collisions = new HashMap<String,Short>((int)(bobRecordsMap.size()/ 0.75f + 1), 0.75f);
        System.out.print("Counting collisions with alice records...(0%)");
        for(int aliceId=0; aliceId < aliceRecords.length; aliceId++) {
            System.out.format("\rCounting collisions with alice records...(%d%%).",
                    Math.round(100 * ((aliceId + 1) / (double) aliceRecords.length)));
            final BitSet[] keys = hashRecord(aliceRecords[aliceId], aliceEncodingFieldName);
            collisions.clear();
            for (int l = 0; l < blockingGroups.length; l++) {
                final LinkedList<char[]> bobIds = buckets[l].get(keys[l]);
                if(bobIds == null) continue;
                for(char[] bobId : bobIds) {
                    final String bs = new String(bobId);
                    short count = collisions.containsKey(bs) ? collisions.get(bs) : 0;
                    count++;
                    collisions.put(bs,count);
                    if(count == C) {
                        final String as = String.valueOf(aliceRecords[aliceId].get(aliceUidFieldName));
                        result.increaseFrequentPairsCount();
                        final BloomFilter bf1 = BloomFilterEncodingUtil.retrieveBloomFilter(aliceRecords[aliceId],
                                aliceEncodingFieldName, N);
                        final BloomFilter bf2 = BloomFilterEncodingUtil.retrieveBloomFilter(bobRecordsMap.get(bs),
                                bobEncodingFieldName, N);
                        if (isTrullyMatchedPair(as,bs)) result.increaseTrullyMatchedCount();
                        if(PrivateSimilarityUtil.similarity("hamming", bf1, bf2, hammingThreshold)) {
                            result.addPair(as, bs);
                            result.increaseMatchedPairsCount();
                        }
                    }
                }
            }
        }
        System.out.format("\rCounting collisions with alice records...(100%%)");
        final long stop = System.currentTimeMillis();
        result.setFpsTime(stop-start);
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
     * @param bobId bob id
     */
    private void addToBucket(final int blockingGroupId ,
                             final BitSet hash,
                             final String bobId) {
        LinkedList<char[]> ids = buckets[blockingGroupId].get(hash);
        if(ids == null) {
            ids = new LinkedList<char[]>();
            ids.add(bobId.toCharArray());
            buckets[blockingGroupId].put(hash, ids);
        } else ids.add(bobId.toCharArray());
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

    /**
     * Returns result.
     * @return hamming result;
     */
    public HammingLSHBlockingResult getResult() {
        if(result == null) return new HammingLSHBlockingResult();
        return result;
    }

    /**
     * Returns result.
     *
     * @return hamming result;
     */
    public void resetResult() {
        result = new HammingLSHBlockingResult();
    }

    /**
     * Only for the voters dataset benchmarkk.
     */
    private static Pattern VOTER_ID_PATTERN = Pattern.compile("[a|b]([0-9]+)_{0,1}[0-9]*");
    private static boolean isTrullyMatchedPair(final String idA, final String idB) {
        Matcher matcherA = VOTER_ID_PATTERN .matcher(idA);
        Matcher matcherB = VOTER_ID_PATTERN.matcher(idB);
        if(!matcherA.matches() || !matcherB.matches()) throw new IllegalArgumentException("Wrong id format.");
        return matcherA.group(1).equals(matcherB.group(1));

    }
}
