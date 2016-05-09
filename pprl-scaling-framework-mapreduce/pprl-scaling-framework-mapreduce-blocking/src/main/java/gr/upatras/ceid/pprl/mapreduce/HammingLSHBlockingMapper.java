package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

public class HammingLSHBlockingMapper extends Mapper<AvroKey<GenericRecord>,NullWritable,Text,Text> {

    private HammingLSHBlocking blocking;
    private String aliceUidfieldName;
    private String bobUidfieldName;
    public static String ALICE_SCHEMA_KEY = "alice.encoding.schema";
    public static String BOB_SCHEMA_KEY = "bob.encoding.schema";
    public static String BLOCKING_KEYS_KEY = "blocking.keys";
    public static String ALICE_UID_KEY = "alice.uid.field.name";
    public static String BOB_UID_KEY = "bob.uid.field.name";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            final String aliceSchemaString = context.getConfiguration().get(ALICE_SCHEMA_KEY);
            if(aliceSchemaString == null ) throw new IllegalStateException("Alice schema not set.");
            final String bobSchemaString = context.getConfiguration().get(BOB_SCHEMA_KEY);
            if(bobSchemaString == null ) throw new IllegalStateException("Bob schema not set.");
            final String[] blockingKeys = context.getConfiguration().getStrings(BLOCKING_KEYS_KEY,null);
            if(blockingKeys == null) throw new IllegalStateException("Blocking keys not set.");
            aliceUidfieldName = context.getConfiguration().get(ALICE_UID_KEY);
            if(aliceUidfieldName == null) throw new IllegalStateException("Alice UID not set.");
            bobUidfieldName = context.getConfiguration().get(BOB_UID_KEY);
            if(bobUidfieldName == null) throw new IllegalStateException("Bob UID not set.");
            BloomFilterEncoding aliceEncoding = BloomFilterEncodingUtil.setupNewInstance(
                                ((new Schema.Parser()).parse(aliceSchemaString)));
            BloomFilterEncoding bobEncoding = BloomFilterEncodingUtil.setupNewInstance(
                                ((new Schema.Parser()).parse(bobSchemaString)));
            blocking = new HammingLSHBlocking(blockingKeys, aliceEncoding, bobEncoding);
        } catch (Exception e) {throw new InterruptedException(e.getMessage());}
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        final boolean isAliceRecord = blocking.isAliceRecord(record);
        final boolean isBobRecord = blocking.isBobRecord(record);
        if(isAliceRecord == isBobRecord) throw new IllegalStateException("Cannot be of the same schema.");

        final BitSet[] keys = isAliceRecord ?
                blocking.hashAliceRecord(record) :
                blocking.hashBobRecord(record);

        final String uid = isAliceRecord ?
                String.valueOf(record.get(aliceUidfieldName)) :
                String.valueOf(record.get(bobUidfieldName));

        for (int i = 0; i < keys.length; i++) {
            final Text blockingKey = new Text(
                    String.format("%05d" + "_%s_"+ (isAliceRecord ? "A" : "B"), i, keyToString(keys[i],blocking.getK())
                    ));
            final Text val = new Text(uid);
            context.write(blockingKey,val);
        }
    }

    private static String keyToString(final BitSet bitSet,int K) {
        StringBuilder sb = new StringBuilder();
        for (int i = K; i >= 0; i--)
            sb.append(bitSet.get(i)?"1":"0");
        return sb.toString();
    }
}
