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
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseFrequentPairCounter;
import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseRecordCounter;

/**
 * FPS Mapper class (v2).
 */
public class FPSMapperV2 extends Mapper<AvroKey<GenericRecord>,NullWritable,AvroKey<GenericRecord>,Text> {
    private Map<BitSet,ArrayList<byte[]>>[] bobBuckets;
    private Map<String,Short> counters;

    private HammingLSHBlocking blocking;
    private String uidFieldName;
    private String encodingFieldName;
    private char dataset;

    private long frequentPairsCount;
    private short C;

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        final BitSet[] keys = blocking.hashRecord(key.datum(), encodingFieldName);
        if(!counters.isEmpty()) counters.clear();
        for (int i = 0; i < keys.length; i++) {
            ArrayList<byte[]> bobIds = bobBuckets[i].get(keys[i]);
            if(bobIds == null) continue;
            for (byte[] bobId : bobIds) {
                boolean isFrequent = increaseFPSCount(bobId);
                if(isFrequent) {
                    frequentPairsCount++;
                    context.write(key, new Text(bobId));
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
            loadbobBuckets(context);
            initCounters(context);
            C = (short) context.getConfiguration().getInt(CommonKeys.FREQUENT_PAIR_LIMIT, -1);
            if(C < 0) throw new InterruptedException("C is not set.");
        } catch (Exception e) {throw new InterruptedException(e.getMessage());}
    }


    /**
     * Load bob's blocking buckets.
     *
     * @param context context
     * @throws IOException
     */
    private void loadbobBuckets(final Context context)
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

        for (Path bucketPath : bucketPaths) {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(bucketPath));
            BlockingKeyWritable key = new BlockingKeyWritable();
            TextArrayWritable bobIds = new TextArrayWritable();
            while(reader.next(key,bobIds)) {
                for (Text bobId : bobIds.get())
                    populateBobBuckets(key.blockingGroupId, key.hash,bobId);
            }
            reader.close();
        }
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
}
