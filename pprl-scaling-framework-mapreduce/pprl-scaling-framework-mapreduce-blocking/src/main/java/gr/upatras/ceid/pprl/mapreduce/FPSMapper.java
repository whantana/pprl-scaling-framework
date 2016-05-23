package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseFrequentPairCounter;
import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseRecordCounter;

public class FPSMapper extends Mapper<AvroKey<GenericRecord>,NullWritable,Text,Text> {


    private Map<BitSet,List<Text>>[] bobBuckets;
    private Map<Text,Short> counters;

    private HammingLSHBlocking blocking;
    private String uidFieldName;
    private String encodingFieldName;
    private char dataset;

    private long frequentPairsCount;
    private short C;

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        final Text aliceId = new Text(String.valueOf(key.datum().get(uidFieldName)));
        final BitSet[] keys = blocking.hashRecord(key.datum(), encodingFieldName);
        counters.clear();
        for (int i = 0; i < keys.length; i++) {
            List<Text> bobIds = bobBuckets[i].get(keys[i]);
            if(bobIds == null) continue;
            for (Text bobId : bobIds) {
                boolean isFrequent = increaseFPSCount(bobId);
                if(isFrequent) {
                    frequentPairsCount++;
                    context.write(aliceId,bobId);
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
        long recordCount = 0;
        frequentPairsCount = 0;
        try {
            do {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
                recordCount++;
            } while (context.nextKeyValue());
        } finally {
            increaseFrequentPairCounter(context,frequentPairsCount);
            increaseRecordCounter(context,dataset,recordCount);
            cleanup(context);
        }
    }

    /**
     * Increase the counter for a specific id. Returns true
     * if the increase is equal to C (frequent pair collision limit), false
     * otherwise.
     *
     * @param bobId a bob record id.
     * @return true if the increase is equal to C (frequent pair collision limit), false otherwise.
     */
    private boolean increaseFPSCount(final Text bobId) {
        if(!counters.containsKey(bobId)) {
            counters.put(bobId,(short)1);
            return false;
        }
        short count = counters.get(bobId);
        if(count < 0) return false;
        if(count + 1 == C) {
            counters.put(bobId,(short)-1);
            return true;
        }
        count++;
        counters.put(bobId,count);
        return false;
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
            bobBuckets = loadBlockingBuckets(context);
            final int bobRecordCounts = context.getConfiguration().getInt(CommonKeys.BOB_RECORD_COUNT_COUNTER,16);
            counters = new HashMap<Text,Short>(bobRecordCounts,1.0f);
            C = (short) context.getConfiguration().getInt(CommonKeys.FREQUENT_PAIR_LIMIT, -1);
            if(C < 0) throw new InterruptedException("C is not set.");
        } catch (Exception e) {throw new InterruptedException(e.getMessage());}
    }


    /**
     * Load bob's blocking buckets from the distributed cache. Returns an array of
     * big hashmaps as the buckets.
     *
     * @param context context
     * @return an array of big hashmaps as the buckets.
     * @throws IOException
     */
    private Map<BitSet,List<Text>>[] loadBlockingBuckets(final Context context)
            throws IOException {
        final Configuration conf = context.getConfiguration();

        final int capacity = conf.getInt(CommonKeys.BUCKET_INITIAL_CAPACITY,16);

        Map<BitSet,List<Text>>[] buckets = new Map[blocking.getL()];
        for(int i=0 ; i < blocking.getL() ; i++)
            buckets[i] = new HashMap<BitSet, List<Text>>(capacity+1,1.0f);

        final SortedSet<Path> bucketPaths = new TreeSet<Path>();
        for(final URI uri : context.getCacheFiles()) {
            if(!uri.toString().endsWith("jar"))
            bucketPaths.add(new Path(uri));
        }

        for (Path bucketPath : bucketPaths) {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(bucketPath));
            BlockingKeyWritable key = new BlockingKeyWritable();
            TextArrayWritable bobIds = new TextArrayWritable();
            while(reader.next(key,bobIds)) {
                if(!buckets[key.blockingGroupId].containsKey(key.hash))
                    buckets[key.blockingGroupId].put(key.hash,new LinkedList<Text>());
                for (Text bobId : bobIds.get())
                    buckets[key.blockingGroupId].get(key.hash).add(bobId);
            }
            reader.close();
        }
        return buckets;
    }
}
