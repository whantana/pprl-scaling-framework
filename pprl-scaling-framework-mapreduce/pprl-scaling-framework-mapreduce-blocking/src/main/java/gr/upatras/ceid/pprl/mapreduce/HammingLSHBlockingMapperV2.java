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


public class HammingLSHBlockingMapperV2 extends Mapper<AvroKey<GenericRecord>,NullWritable,BlockingKeyWritable,Text> {

    private HammingLSHBlocking blocking;
    private String uidFieldName;
    private String encodingFieldName;
    private char dataset;

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        final BitSet[] keys = blocking.hashRecord(record,encodingFieldName);
        final String uid = String.valueOf(record.get(uidFieldName));
        for (int i = 0; i < keys.length; i++) {
            final BlockingKeyWritable blockingKey = new BlockingKeyWritable(i,keys[i],dataset);
            final Text val = new Text(uid);
            context.write(blockingKey,val);
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setupBlocking(context);
        context.nextKeyValue();
        final Schema s = context.getCurrentKey().datum().getSchema();
        setupMapper(s,context);
        long recordCount = 0;
        try {
            do {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
                recordCount++;
            } while (context.nextKeyValue());
        } finally {
            context.getCounter(
                    CommonKeys.COUNTER_GROUP_NAME,
                    String.format("%s.%c",CommonKeys.RECORD_COUNT_COUNTER,dataset)).increment(recordCount);
            cleanup(context);
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
            final String aliceSchemaString = context.getConfiguration().get(CommonKeys.ALICE_SCHEMA_KEY);
            if (aliceSchemaString == null) throw new IllegalStateException("Alice schema not set.");
            final String bobSchemaString = context.getConfiguration().get(CommonKeys.BOB_SCHEMA_KEY);
            if (bobSchemaString == null) throw new IllegalStateException("Bob schema not set.");
            final String[] blockingKeys = context.getConfiguration().getStrings(CommonKeys.BLOCKING_KEYS_KEY);
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
        ;
        if(schema.getName().equals(blocking.getAliceEncodingName())){
            encodingFieldName = blocking.getAliceEncodingFieldName();
            uidFieldName = context.getConfiguration().get(CommonKeys.ALICE_UID_KEY);
            dataset = 'A';
        } else if(schema.getName().equals(blocking.getBobEncodingName())){
            encodingFieldName = blocking.getBobEncodingFieldName();
            uidFieldName = context.getConfiguration().get(CommonKeys.BOB_UID_KEY);
            dataset = 'B';

        } else throw new IllegalStateException("Unknown schema name : " + schema.getName());
        if(uidFieldName == null) throw new IllegalStateException("UID field name not set.");
    }

}
