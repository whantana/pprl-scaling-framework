package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BaseBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.SimpleBloomFilterEncoding;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class SimpleBloomFilterEncodingMapper extends BaseBloomFilterEncodingMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        encoding = new SimpleBloomFilterEncoding(inputSchema, outputSchema, selectedColumnsNames, N, K, Q);
        try {
            encoding.createEncodingFields();
            selectedColumns = encoding.getSelectedColumns();
            restColumns = encoding.getRestColumns();
        } catch (BloomFilterEncodingException e) {
            throw new InterruptedException(e.getMessage());
        }
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        final GenericRecord encodingRecord = new GenericData.Record(encoding.getEncodingSchema());

        // selected for encoding columns
        final Object[] objs = new Object[selectedColumns.size()];
        final Schema.Type[] types = new Schema.Type[selectedColumnsNames.size()];
        int i = 0;
        for (Schema.Field field : selectedColumns) {
            if(!BaseBloomFilterEncoding.SUPPORTED_TYPES.contains(field.schema().getType())) continue;
            objs[i] = record.get(field.name());
            types[i] = field.schema().getType();
            i++;
        }
        final String fieldName = ((SimpleBloomFilterEncoding) encoding).getEncodingColumnName();
        Schema fieldSchema = ((SimpleBloomFilterEncoding) encoding).getEncodingColumn().schema();
        encodingRecord.put(fieldName, encoding.encode(objs, types, fieldSchema));

        // rest of columns
        for(Schema.Field field : restColumns) {
            Object obj = record.get(field.name());
            encodingRecord.put(field.name(), obj);
        }

        context.write(new AvroKey<GenericRecord>(encodingRecord), NullWritable.get());
    }
}
