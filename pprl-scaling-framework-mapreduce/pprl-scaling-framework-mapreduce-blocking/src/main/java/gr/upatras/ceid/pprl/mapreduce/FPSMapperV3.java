package gr.upatras.ceid.pprl.mapreduce;

import com.javamex.classmexer.MemoryUtil;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.*;

/**
 * FPS Mapper class (v3).
 */
public class FPSMapperV3 extends Mapper<AvroKey<GenericRecord>,NullWritable,Text,Text> {

    private String bobEncodingFieldName;
    private Map<BitSet,ArrayList<byte[]>>[] bobBuckets;
    private Map<String,Short> counters;

    private GenericRecord[] bobRecords;
    private Map<String,Integer> bobId2IndexMap;


    private HammingLSHBlocking blocking;
    private String uidFieldName;
    private String encodingFieldName;
    private char dataset;

    private long frequentPairsCount;
    private long matchedPairCount;
    private short C;
    private int N;
    private int hammingThreshold;

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        GenericRecord aliceRecord =  key.datum();
        final Text aliceId = new Text(String.valueOf(aliceRecord.get(uidFieldName)));
        final BitSet[] keys = blocking.hashRecord(aliceRecord, encodingFieldName);
        if(!counters.isEmpty()) counters.clear();
        for (int i = 0; i < keys.length; i++) {
            ArrayList<byte[]> bobIds = bobBuckets[i].get(keys[i]);
            if(bobIds == null) continue;
            for (byte[] bobId : bobIds) {
                boolean isFrequent = increaseFPSCount(bobId);
                if(isFrequent) {
                    frequentPairsCount++;
                    GenericRecord bobRecord = bobRecords[bobId2IndexMap.get(Text.decode(bobId))];
                    context.write(aliceId, new Text(bobId));
                    final BloomFilter bf1 = BloomFilterEncodingUtil.retrieveBloomFilter(aliceRecord,
                            encodingFieldName, N);
                    final BloomFilter bf2 = BloomFilterEncodingUtil.retrieveBloomFilter(bobRecord,
                            bobEncodingFieldName, N);
                    if (PrivateSimilarityUtil.similarity("hamming", bf1, bf2, hammingThreshold)) {
                        context.write(new Text(aliceId), new Text(bobId));
                        matchedPairCount++;
                    }
                }
            }
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setupBlocking(context);
        context.nextKeyValue();
        final Schema s = context.getCurrentKey().datum().getSchema();
        setupMapper(s,context);
        loadBobRecords(context);
        loadBobBuckets(context);
        initCounters(context);
        long recordCount = 0;
        frequentPairsCount = 0;
        try {
            do {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
                recordCount++;
            } while (context.nextKeyValue());
        } finally {
            increaseFrequentPairCounter(context,frequentPairsCount);
            increaseMatchedPairsCounter(context,matchedPairCount);
            increaseRecordCounter(context,dataset,recordCount);
            cleanup(context);
        }
    }

    /**
     * Setup mapper.
     *
     * @param schema schema.
     * @param context context.
     */
    private void setupMapper(final Schema schema,final Context context) {
        if(schema.getName().equals(blocking.getAliceEncodingName())){
            encodingFieldName = blocking.getAliceEncodingFieldName();
            uidFieldName = context.getConfiguration().get(CommonKeys.ALICE_UID);
            dataset = 'A';
        } else if(schema.getName().equals(blocking.getBobEncodingName())){
            encodingFieldName = blocking.getBobEncodingFieldName();
            uidFieldName = context.getConfiguration().get(CommonKeys.BOB_UID);
            dataset = 'B';

        } else throw new IllegalStateException("Unknown schema name : " + schema.getName());
        if(uidFieldName == null) throw new IllegalStateException("UID field name not set.");
    }


    /**
     * Setup blocking instance.
     *
     * @param context context.
     * @throws InterruptedException
     */
    private void setupBlocking(final Context context) throws InterruptedException {
        try {
            final String aliceSchemaString = context.getConfiguration().get(CommonKeys.ALICE_SCHEMA);
            if (aliceSchemaString == null) throw new IllegalStateException("Alice schema not set.");
            final String bobSchemaString = context.getConfiguration().get(CommonKeys.BOB_SCHEMA);
            if (bobSchemaString == null) throw new IllegalStateException("Bob schema not set.");
            final String[] blockingKeys = context.getConfiguration().getStrings(CommonKeys.BLOCKING_KEYS);
            if (blockingKeys == null) throw new IllegalStateException("Blocking keys not set.");
            BloomFilterEncoding aliceEncoding = BloomFilterEncodingUtil.setupNewInstance(
                    ((new Schema.Parser()).parse(aliceSchemaString)));
            BloomFilterEncoding bobEncoding = BloomFilterEncodingUtil.setupNewInstance(
                    ((new Schema.Parser()).parse(bobSchemaString)));
            blocking = new HammingLSHBlocking(blockingKeys, aliceEncoding, bobEncoding);
            C = (short) context.getConfiguration().getInt(CommonKeys.FREQUENT_PAIR_LIMIT, -1);
            if(C < 0) throw new InterruptedException("C is not set.");
            N = aliceEncoding.getBFN();
            hammingThreshold = context.getConfiguration().getInt(CommonKeys.HAMMING_THRESHOLD, 100);
        } catch (Exception e) {throw new InterruptedException(e.getMessage());}
    }

    /**
     * Load bob's records.
     *
     * @param context context
     * @throws IOException
     */
    private void loadBobRecords(Context context) throws IOException {
        final String bobSchemaString = context.getConfiguration().get(CommonKeys.BOB_SCHEMA);
        if (bobSchemaString == null) throw new IllegalStateException("Bob schema not set.");
        final Schema bobSchema = (new Schema.Parser()).parse(bobSchemaString);
        final String bobAvroPathUri = context.getConfiguration().get(CommonKeys.BOB_DATA_PATH,null);
        if (bobAvroPathUri == null) throw new IllegalStateException("Bob avro path not set.");
        final Path bobAvroPath;
        try {
            bobAvroPath = new Path(new URI(bobAvroPathUri));
        } catch (URISyntaxException e) {throw new IOException(e.getMessage());}

        final int bobRecordCount = context.getConfiguration().getInt(CommonKeys.BOB_RECORD_COUNT_COUNTER, -1);
        if(bobRecordCount < 0) throw new IllegalStateException("Bob record count not set.");

        bobEncodingFieldName = blocking.getBobEncodingFieldName();
        String bobUidFieldName = context.getConfiguration().get(CommonKeys.BOB_UID, null);
        if (bobUidFieldName == null) throw new IllegalStateException("Bob uid not set.");

        final FileSystem fs = FileSystem.get(context.getConfiguration());
        final DatasetsUtil.DatasetRecordReader reader =
                new DatasetsUtil.DatasetRecordReader(fs,bobSchema,bobAvroPath);

        System.out.println("Loading bob records...");
        int i = 0;
        bobRecords = new GenericRecord[bobRecordCount];
        bobId2IndexMap = new HashMap<String, Integer>((int)(bobRecordCount/0.75f + 1),0.75f);
        try {
            while (reader.hasNext()) {
                bobRecords[i] = reader.next();
                bobId2IndexMap.put(String.valueOf(bobRecords[i].get(bobUidFieldName)), i);
                i++;
            }
        } finally {
            reader.close();
        }

        long bobRecordsBytes = MemoryUtil.deepMemoryUsageOf(bobRecords) +
                MemoryUtil.deepMemoryUsageOf(bobId2IndexMap);
        System.out.println("Bob records memory footprint : " + bobRecordsBytes/(1024*1024) + " MB");
        increaseTotalByteCounter(context, bobRecordsBytes);
    }

    /**
     * Load bob's blocking buckets.
     *
     * @param context context
     * @throws IOException
     */
    private void loadBobBuckets(final Context context)
            throws IOException {
        final Configuration conf = context.getConfiguration();

        final int actualCapacity = conf.getInt(CommonKeys.BUCKET_INITIAL_CAPACITY,16);
        final float fillFactor = 0.75f;
        final int capacity = (int)(actualCapacity/fillFactor + 1);

        bobBuckets = new Map[blocking.getL()];
        for(int i=0 ; i < blocking.getL() ; i++)
            bobBuckets[i] = new HashMap<BitSet, ArrayList<byte[]>>(capacity,fillFactor);

        final SortedSet<Path> bucketPaths = new TreeSet<Path>();
        for(final URI uri : context.getCacheFiles()) {
            if(!uri.toString().endsWith("jar"))
            bucketPaths.add(new Path(uri));
        }

        System.out.println("Loading bob buckets...");
        int i = 1;
        System.out.format("Loading bob buckets...(%d/%d)\n",
                i, bucketPaths.size());
        for (Path bucketPath : bucketPaths) {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(bucketPath));
            BlockingKeyWritable key = new BlockingKeyWritable();
            TextArrayWritable bobIds = new TextArrayWritable();
            while(reader.next(key,bobIds)) {
                for (Text bobId : bobIds.get())
                    populateBobBuckets(key.blockingGroupId, key.hash,bobId);
            }
            reader.close();
            i++;
            System.out.format("Loading bob buckets...(%d/%d)\n", i, bucketPaths.size());
        }
        long bobBucketsBytes = MemoryUtil.deepMemoryUsageOf(bobBuckets);
        System.out.println("Bob buckets memory footprint : " + bobBucketsBytes/(1024*1024) + " MB");
        increaseTotalByteCounter(context, bobBucketsBytes);
    }

    /**
     * Populate bob buckets
     *
     * @param bgid blokcing group id.
     * @param hash hash value.
     * @param id record id.
     */
    private void populateBobBuckets(final int bgid,final BitSet hash, final Text id) {
        ArrayList<byte[]> ids = bobBuckets[bgid].get(hash);
        if(ids == null) {
            ids = new ArrayList<byte[]>();
            ids.add(Arrays.copyOf(id.getBytes(), id.getLength()));
            bobBuckets[bgid].put(hash,ids);
        } else ids.add(Arrays.copyOf(id.getBytes(), id.getLength()));
    }

    /**
     * Initializes counters.
     *
     * @param context context
     */
    private void initCounters(final Context context) {
        final int actualCapacity = context.getConfiguration().getInt(CommonKeys.BOB_RECORD_COUNT_COUNTER, 16);
        final float fillFactor = 0.75f;
        final int capacity = (int)(actualCapacity/fillFactor + 1);
        counters = new HashMap<String,Short>(capacity,fillFactor);
    }

    /**
     * Increase the counter for a specific id. Returns true
     * if the increase is equal to C (frequent pair collision limit), false
     * otherwise.
     *
     * @param bobId a bob record id.
     * @return true if the increase is equal to C (frequent pair collision limit), false otherwise.
     */
    private boolean increaseFPSCount(final byte[] bobId) throws CharacterCodingException {
        final String str = Text.decode(bobId);
        if(!counters.containsKey(str)) {
            counters.put(str,(short)1);
            return false;
        }
        short count = counters.get(str);
        count++;
        counters.put(str,count);
        return count == C;
    }
}
