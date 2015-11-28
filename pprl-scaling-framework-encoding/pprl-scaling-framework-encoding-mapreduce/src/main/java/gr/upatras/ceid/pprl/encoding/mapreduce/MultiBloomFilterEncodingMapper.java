package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BaseBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.MultiBloomFilterEncoding;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class MultiBloomFilterEncodingMapper extends BaseBloomFilterEncodingMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        encoding = new MultiBloomFilterEncoding(inputSchema,outputSchema,selectedColumnsNames,N,K,Q);
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

        for(Schema.Field field : selectedColumns) {
            if(!BaseBloomFilterEncoding.SUPPORTED_TYPES.contains(field.schema().getType())) continue;
            Object obj = record.get(field.name());
            Schema.Type type = field.schema().getType();
            Schema.Field encodingField = ((MultiBloomFilterEncoding) encoding).getEncodingColumnForName(field.name());
            String encodingFieldName = encodingField.name();
            Schema fieldSchema = encodingField.schema();
            encodingRecord.put(encodingFieldName, encoding.encode(obj, type, fieldSchema));
        }

        // rest of columns
        for(Schema.Field field : restColumns) {
            Object obj = record.get(field.name());
            encodingRecord.put(field.name(), obj);
        }

        context.write(new AvroKey<GenericRecord>(encodingRecord),NullWritable.get());
    }
}
