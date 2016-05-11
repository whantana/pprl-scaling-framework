package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.BitSet;

/**
 * Hamming LSH Blocking Mapper class.
 */
public class HammingLSHBlockingMapper extends Mapper<AvroKey<GenericRecord>,NullWritable,Text,Text> {

    private HammingLSHBlocking blocking;
    private String uidFieldName;
    private String encodingFieldName;
    private String keyFormat;

    public static String ALICE_SCHEMA_KEY = "alice.encoding.schema";
    public static String BOB_SCHEMA_KEY = "bob.encoding.schema";
    public static String BLOCKING_KEYS_KEY = "blocking.keys";
    public static String ALICE_UID_KEY = "alice.uid.field.name";
    public static String BOB_UID_KEY = "bob.uid.field.name";

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        final BitSet[] keys = blocking.hashRecord(record,encodingFieldName);
        final String uid = String.valueOf(record.get(uidFieldName));
        for (int i = 0; i < keys.length; i++) {
            final Text blockingKey = new Text(String.format(keyFormat, i, keyToString(keys[i],blocking.getK())));
            final Text val = new Text(uid);
            context.write(blockingKey,val);
        }
    }

    /**
     * Setup blocking instance.
     *
     * @param context context.
     * @throws InterruptedException
     */
    private void setupBlocking(final Context context) throws InterruptedException {
        try {
            final String aliceSchemaString = context.getConfiguration().get(ALICE_SCHEMA_KEY);
            if (aliceSchemaString == null) throw new IllegalStateException("Alice schema not set.");
            final String bobSchemaString = context.getConfiguration().get(BOB_SCHEMA_KEY);
            if (bobSchemaString == null) throw new IllegalStateException("Bob schema not set.");
            final String[] blockingKeys = context.getConfiguration().getStrings(BLOCKING_KEYS_KEY, null);
            if (blockingKeys == null) throw new IllegalStateException("Blocking keys not set.");
            BloomFilterEncoding aliceEncoding = BloomFilterEncodingUtil.setupNewInstance(
                    ((new Schema.Parser()).parse(aliceSchemaString)));
            BloomFilterEncoding bobEncoding = BloomFilterEncodingUtil.setupNewInstance(
                    ((new Schema.Parser()).parse(bobSchemaString)));
            blocking = new HammingLSHBlocking(blockingKeys, aliceEncoding, bobEncoding);
        } catch (Exception e) {throw new InterruptedException(e.getMessage());}
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
            uidFieldName = context.getConfiguration().get(ALICE_UID_KEY);
            keyFormat = "%05d_%s_A";
        } else if(schema.getName().equals(blocking.getBobEncodingName())){
            encodingFieldName = blocking.getAliceEncodingFieldName();
            uidFieldName = context.getConfiguration().get(BOB_UID_KEY);
            keyFormat = "%05d_%s_B";
        } else throw new IllegalStateException("Unknown schema name : " + schema.getName());
        if(uidFieldName == null) throw new IllegalStateException("UID field name not set.");

    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setupBlocking(context);
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

    private static String keyToString(final BitSet bitSet,int K) {
        StringBuilder sb = new StringBuilder();
        for (int i = K; i >= 0; i--)
            sb.append(bitSet.get(i)?"1":"0");
        return sb.toString();
    }
}
