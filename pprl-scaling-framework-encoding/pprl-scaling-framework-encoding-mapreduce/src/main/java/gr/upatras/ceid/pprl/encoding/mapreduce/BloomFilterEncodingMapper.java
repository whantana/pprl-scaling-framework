package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BloomFilterEncodingMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

    public static final String INPUT_SCHEMA_KEY = "input.schema";
    public static final String OUTPUT_SCHEMA_KEY = "output.schema";

    protected Schema inputSchema;
    protected Schema outputSchema;
    protected BloomFilterEncoding encoding;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            final Configuration config = context.getConfiguration();
            inputSchema = (new Schema.Parser()).parse(config.get(INPUT_SCHEMA_KEY));
            outputSchema = (new Schema.Parser()).parse(config.get(OUTPUT_SCHEMA_KEY));
            encoding = BloomFilterEncodingUtil.newInstance(
                    BloomFilterEncodingUtil.retrieveSchemeName(outputSchema));
            encoding.setupFromSchema(outputSchema);
            if(!encoding.isEncodingOfSchema(inputSchema))
                throw new BloomFilterEncodingException("Encoding schema does not match input schema");
            encoding.initialize();
        } catch (BloomFilterEncodingException e) {
            throw new InterruptedException(e.getMessage());
        }
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        try {
            final GenericRecord record = key.datum();
            final GenericRecord encodedRecord = encoding.encodeRecord(record);
            context.write(new AvroKey<GenericRecord>(encodedRecord), NullWritable.get());
        } catch (BloomFilterEncodingException e) {
            throw new InterruptedException(e.getMessage());
        }
    }
}
