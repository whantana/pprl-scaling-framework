package gr.upatras.ceid.pprl.encoding.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class EncodeDatasetMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

    public static final String INPUT_SCHEMA_KEY = "pprl.encoding.input.schema";
    public static final String OUTPUT_SCHEMA_KEY = "pprl.encoding.input.schema";
    public static final String ENCODING_COLUMNS_KEY = "pprl.encoding.columns";
    public static final String ENCODING_METHOD_KEY = "pprl.encoding.method";
    public static final String N_KEY = "pprl.encoding.bf.n";
    public static final String K_KEY = "pprl.encoding.bf.k";
    public static final String Q_KEY = "pprl.encoding.q";

    private Schema inputSchema;
    private Schema outputSchema;
    private List<String> columns;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        final Configuration config = context.getConfiguration();
        inputSchema = (new Schema.Parser()).parse(config.get(INPUT_SCHEMA_KEY));
        outputSchema = (new Schema.Parser()).parse(config.get(OUTPUT_SCHEMA_KEY));
        columns = Arrays.asList(config.getStrings(ENCODING_COLUMNS_KEY));
    }

    @Override
    protected void map(AvroKey<GenericRecord> avroKey, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord inRecord = avroKey.datum();
        final GenericRecord outRecord = new GenericData.Record(outputSchema);
        for(Schema.Field f : inputSchema.getFields()) {
            if(columns.contains(f.name())) {
                // TODO
            } else {
                outRecord.put(f.name(), inRecord.get(f.name()));
            }
        }
        context.write(new AvroKey<GenericRecord>(outRecord),NullWritable.get());
    }
}
