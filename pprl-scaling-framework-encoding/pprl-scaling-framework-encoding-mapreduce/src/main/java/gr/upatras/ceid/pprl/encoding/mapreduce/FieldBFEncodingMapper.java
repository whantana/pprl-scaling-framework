package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BFEncodingException;
import gr.upatras.ceid.pprl.encoding.FieldBFEncoding;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class FieldBFEncodingMapper extends BaseBFEncodingMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            encoding = new FieldBFEncoding(inputSchema,outputSchema,uidColumn,encodingColumns,N,K,Q);
        } catch (BFEncodingException e) {
            throw new IOException(e.getCause());
        }
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        try {
            final GenericRecord record = key.datum();
            final GenericRecord encodingRecord = new GenericData.Record(encoding.getEncodingSchema());

            // selected for encoding columns
            for(Schema.Field field : encoding.getSelectedColumns()) {
                Object obj = record.get(field.name());
                Class<?> cls = SUPPORTED_TYPES.get(field.schema().getType());
                Schema.Field encodingField = ((FieldBFEncoding) encoding).getEncodingColumnForName(field.name());
                Schema fieldSchema = encodingField.schema();
                encodingRecord.put(encodingField.name(),encoding.encode(obj,cls,fieldSchema));
            }

            // rest of columns
            for(Schema.Field field : encoding.getRestColumns()) {
                Object obj = record.get(field.name());
                encodingRecord.put(field.name(),obj);
            }

            context.write(new AvroKey<GenericRecord>(encodingRecord),NullWritable.get());
        } catch (BFEncodingException e) {
            throw new InterruptedException(e.getMessage());
        }
    }
}
