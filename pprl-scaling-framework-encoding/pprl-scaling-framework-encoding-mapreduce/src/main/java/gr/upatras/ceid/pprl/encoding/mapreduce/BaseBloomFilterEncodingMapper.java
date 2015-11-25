package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BaseBloomFilterEncoding;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BaseBloomFilterEncodingMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

    public static final String INPUT_SCHEMA_KEY = "pprl.encoding.input.schema";
    public static final String UID_COLUMN_KEY = "pprl.encoding.input.column";
    public static final String OUTPUT_SCHEMA_KEY = "pprl.encoding.encoding.schema";
    public static final String SELECTED_COLUMNS_KEY = "pprl.encoding.encoding.columns";
    public static final String N_KEY = "pprl.encoding.bf.n";
    public static final String K_KEY = "pprl.encoding.bf.k";
    public static final String Q_KEY = "pprl.encoding.q";

    protected Schema inputSchema;
    protected Schema outputSchema;
    protected String uidColumn;
    protected List<String> selectedColumnsNames;
    protected int N;
    protected int K;
    protected int Q;

    protected BaseBloomFilterEncoding encoding;
    protected List<Schema.Field> selectedColumns;
    protected List<Schema.Field> restColumns;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        final Configuration config = context.getConfiguration();
        inputSchema = (new Schema.Parser()).parse(config.get(INPUT_SCHEMA_KEY));
        outputSchema = (new Schema.Parser()).parse(config.get(OUTPUT_SCHEMA_KEY));
        uidColumn = config.get(UID_COLUMN_KEY);
        selectedColumnsNames = Arrays.asList(config.getStrings(SELECTED_COLUMNS_KEY));
        N = config.getInt(N_KEY,1024);
        K = config.getInt(K_KEY,30);
        Q = config.getInt(Q_KEY,2);
    }
}
