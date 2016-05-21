package gr.upatras.ceid.pprl.mapreduce;

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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Form Record pairs Mapper.
 */
public class GenerateRecordPairsMapper extends Mapper<AvroKey<GenericRecord>,NullWritable,Text,AvroValue<GenericRecord>> {

    private String aliceEncodingName;
    private String bobEncodingName;
    private Map<String,List<String>> frequentPairMap;
    private String uidFieldName;
    private boolean followsKeyValue;

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        final AvroValue<GenericRecord> avroValue = new AvroValue<GenericRecord>(record);
        final String uid = String.valueOf(record.get(uidFieldName));
        if(!frequentPairMap.containsKey(uid)) return;
        for(String ouid : frequentPairMap.get(uid)) {
            final Text keyPair = new Text(followsKeyValue ?
                    uid + CommonKeys.RECORD_PAIR_DELIMITER + ouid :
                    ouid + CommonKeys.RECORD_PAIR_DELIMITER + uid);
            context.write(keyPair,avroValue);
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setupNames(context);
        context.nextKeyValue();
        final Schema s = context.getCurrentKey().datum().getSchema();
        setupMapper(s,context);
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

        frequentPairMap = loadFrequentPairs(context, followsKeyValue);
    }

    /**
     * Makes and returns a frequent pair map of keys.
     *
     * @param context context.
     * @param followKeyValue if true the A Ids are put as keys to the map and the respect B Ids are set as values.
     *                       The opposite if false.
     * @return a frequent pair map of keys.
     * @throws IOException
     */
    private Map<String,List<String>> loadFrequentPairs(final Context context, boolean followKeyValue)
            throws IOException {
        final Configuration conf = context.getConfiguration();
        final int aliceRecordCount = conf.getInt(CommonKeys.ALICE_RECORD_COUNT_COUNTER,16);
        final int bobRecordCount = conf.getInt(CommonKeys.BOB_RECORD_COUNT_COUNTER,16);
        final Map<String,List<String>> map = new HashMap<String,List<String>>(
                (followKeyValue ? bobRecordCount : aliceRecordCount) + 1,
                1.00f);

        final SortedSet<Path> frequentPairsPaths = new TreeSet<Path>();
        for(final URI uri : context.getCacheFiles())
            frequentPairsPaths.add(new Path(uri));

        for(final Path path : frequentPairsPaths) {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
            Text key = new Text();
            Text value = new Text();
            while(reader.next(key,value)) {
                if (followKeyValue) {
                    if(!map.containsKey(key.toString())) map.put(key.toString(),new LinkedList<String>());
                    map.get(key.toString()).add(value.toString());
                } else {
                    if(!map.containsKey(value.toString())) map.put(value.toString(),new LinkedList<String>());
                    map.get(value.toString()).add(key.toString());
                }
            }
            reader.close();
        }
        return map;
    }
}
