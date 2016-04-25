package gr.upatras.ceid.pprl.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Sort Mapper class.
 */
public class SortMapper extends Mapper<AvroKey<GenericRecord>, NullWritable,
        AvroKey<GenericRecord>, NullWritable> {

    public static String SORTED_SCHEMA_KEY = "sorted.schema" ;
    private Schema sortedSchema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String inputSchemaStr;
        inputSchemaStr = context.getConfiguration().get(SORTED_SCHEMA_KEY,null);
        if(inputSchemaStr==null) throw new IllegalStateException("Must set sorted schema.");
        sortedSchema = (new Schema.Parser()).parse(inputSchemaStr);
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        final GenericRecord sortedRecord = new GenericData.Record(sortedSchema);
        for(Schema.Field field : sortedSchema.getFields()) {
            sortedRecord.put(field.name(),record.get(field.name()));
        }
        context.write(new AvroKey<GenericRecord>(sortedRecord), NullWritable.get());
    }
}
