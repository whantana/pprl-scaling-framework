package gr.upatras.ceid.pprl.mapreduce;

import com.javamex.classmexer.MemoryUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseTotalByteCounter;

/**
 * Make Record pairs Mapper.
 */
public class MakeRecordPairsMapper extends Mapper<AvroKey<GenericRecord>,NullWritable,TextPairWritable,AvroValue<GenericRecord>> {

    private String aliceEncodingName;
    private String bobEncodingName;
    private Map<String,ArrayList<byte[]>> frequentPairMap;
    private String uidFieldName;
    private boolean followsKeyValue;

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        final AvroValue<GenericRecord> avroValue = new AvroValue<GenericRecord>(record);
        final String uid = String.valueOf(record.get(uidFieldName));

        if(!frequentPairMap.containsKey(uid)) return;

        for(byte[] ouid : frequentPairMap.get(uid)) {
            final TextPairWritable keyPair = followsKeyValue ?
                    new TextPairWritable(new Text(uid),new Text(ouid)) :
                    new TextPairWritable(new Text(ouid),new Text(uid));
            context.write(keyPair,avroValue);
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setupNames(context);
        context.nextKeyValue();
        final Schema s = context.getCurrentKey().datum().getSchema();
        setupMapper(s,context);
        loadFrequentPairs(context, followsKeyValue);
        try {
            do {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            } while (context.nextKeyValue());
        } finally {
            cleanup(context);
        }
    }

    /**
     * Setup blocking instance.
     *
     * @param context context.
     * @throws InterruptedException
     */
    private void setupNames(final Context context) throws InterruptedException {
        try {
            final String aliceSchemaString = context.getConfiguration().get(CommonKeys.ALICE_SCHEMA);
            if (aliceSchemaString == null) throw new IllegalStateException("Alice schema not set.");
            final String bobSchemaString = context.getConfiguration().get(CommonKeys.BOB_SCHEMA);
            if (bobSchemaString == null) throw new IllegalStateException("Bob schema not set.");
            aliceEncodingName = ((new Schema.Parser()).parse(aliceSchemaString)).getName();
            bobEncodingName = ((new Schema.Parser()).parse(bobSchemaString)).getName();
            if(aliceEncodingName.equals(bobEncodingName)) throw new IllegalStateException("Encodings must have different names.");
        } catch (Exception e) {throw new InterruptedException(e.getMessage());}
    }

    /**
     * Setup mapper.
     *
     * @param schema schema.
     * @param context context.
     * @throws IOException exception.
     */
    private void setupMapper(final Schema schema,final Context context)
            throws IOException {
        if(schema.getName().equals(aliceEncodingName)){
            uidFieldName = context.getConfiguration().get(CommonKeys.ALICE_UID);
            followsKeyValue = true;
        } else if(schema.getName().equals(bobEncodingName)){
            uidFieldName = context.getConfiguration().get(CommonKeys.BOB_UID);
            followsKeyValue = false;
        } else throw new IllegalStateException("Unknown schema name : " + schema.getName());
    }

    /**
     * Loads frequent pairs as a Map<String,ArrayList<byte[]>). Uses populateFrequent
     *
     * @param context context.
     * @param followKeyValue if true the A Ids are put as keys to the map and the respect B Ids are set as values.
     *                       The opposite if false.
     * @throws IOException
     */
    private void loadFrequentPairs(final Context context, boolean followKeyValue)
            throws IOException {
        final Configuration conf = context.getConfiguration();

        // get pair paths
        final SortedSet<Path> frequentPairsPaths = new TreeSet<Path>();
        for (final URI uri : context.getCacheFiles()) {
            if (!uri.toString().endsWith("jar"))
                frequentPairsPaths.add(new Path(uri));
        }

        // construct map , estimate map capacity
        final int aliceRecordCount = conf.getInt(CommonKeys.ALICE_RECORD_COUNT_COUNTER, 16);
        final int bobRecordCount = conf.getInt(CommonKeys.BOB_RECORD_COUNT_COUNTER, 16);
        final int actualCapacity = followKeyValue ? aliceRecordCount : bobRecordCount;
        final float fillFactor = 0.75f;
        final int capacity = (int) (actualCapacity / fillFactor + 1);
        frequentPairMap = new HashMap<String, ArrayList<byte[]>>(capacity, fillFactor);
        System.out.format("Loading frequent paths...(0/%d)\n",frequentPairsPaths.size());
        int i = 1;
        System.out.format("Loading frequent paths...(%d/%d)\n",
                i, frequentPairsPaths.size());
        for (final Path path : frequentPairsPaths) {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
            Text key = new Text();
            Text value = new Text();
            while (reader.next(key, value)) {
                if (followKeyValue) populateFrequentPairMap(key, value);
                else populateFrequentPairMap(value, key);
            }
            reader.close();
            System.out.format("Loading frequent paths...(%d/%d) . Size so far : %d\n",
                    i, frequentPairsPaths.size(),MemoryUtil.deepMemoryUsageOf(frequentPairMap));
            i++;
        }
        long frequentPairBytes = MemoryUtil.deepMemoryUsageOf(frequentPairMap);
        System.out.println("Frequent pairs memory footprint : " + frequentPairBytes/(1024*1024) + " MB");
        increaseTotalByteCounter(context, frequentPairBytes);
    }


    /**
     * Populates frequent pair map with key and values.
     * @param key key
     * @param value value.
     */
    public void populateFrequentPairMap(final Text key,final Text value) {
        ArrayList<byte[]> list = frequentPairMap.get(key.toString());
        if(list == null) {
            list = new ArrayList<byte[]>();
            list.add(Arrays.copyOf(value.getBytes(), value.getLength()));
            frequentPairMap.put(key.toString(), list);
        } else list.add(Arrays.copyOf(value.getBytes(), value.getLength()));
    }
}
