package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BloomFilterEncodingMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

    public static final String INPUT_SCHEMA_KEY = "pprl.encoding.input.schema";
    public static final String OUTPUT_SCHEMA_KEY = "pprl.encoding.encoding.schema";
    public static final String SELECTED_FIELDS_KEY = "pprl.encoding.fields.selected";
    public static final String METHOD_NAME_KEY = "pprl.encoding.method.name";
    public static final String REST_FIELDS_KEY = "pprl.encoding.fields.rest";
    public static final String N_KEY = "pprl.encoding.bf.n";
    public static final String K_KEY = "pprl.encoding.bf.k";
    public static final String Q_KEY = "pprl.encoding.q";

    protected Schema inputSchema;
    protected Schema outputSchema;
    protected String methodName;
    protected int[] N;
    protected int K;
    protected int Q;
    protected String[] restFieldNames;
    protected String[] selectedFieldNames;

    protected BloomFilterEncoding encoding;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            final Configuration config = context.getConfiguration();
            inputSchema = (new Schema.Parser()).parse(config.get(INPUT_SCHEMA_KEY));
            outputSchema = (new Schema.Parser()).parse(config.get(OUTPUT_SCHEMA_KEY));
            methodName = config.get(METHOD_NAME_KEY);
            restFieldNames = config.getStrings(REST_FIELDS_KEY);
            selectedFieldNames = config.getStrings(SELECTED_FIELDS_KEY);
            final String[] Nstr = config.getStrings(N_KEY);
            N = new int[Nstr.length];
            for (int i = 0; i < N.length; i++) {
                N[i] = Integer.parseInt(Nstr[i]);
            }
            K = config.getInt(K_KEY, 30);
            Q = config.getInt(Q_KEY, 2);

            encoding = BloomFilterEncoding.newInstanceOfMethod(methodName);
        } catch (BloomFilterEncodingException e) {
            throw new InterruptedException(e.getMessage());
        }
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        final GenericRecord encodedRecord;
        try {
            encodedRecord = BloomFilterEncoding.encodeRecord(
                    record, encoding, inputSchema,
                    selectedFieldNames, restFieldNames, N, K, Q);
        } catch (BloomFilterEncodingException e) {
            throw new InterruptedException(e.getMessage());
        }
        context.write(new AvroKey<GenericRecord>(encodedRecord), NullWritable.get());
    }
}
