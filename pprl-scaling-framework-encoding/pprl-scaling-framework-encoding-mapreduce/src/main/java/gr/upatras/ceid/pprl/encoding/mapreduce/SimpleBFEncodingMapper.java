package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BFEncodingException;
import gr.upatras.ceid.pprl.encoding.SimpleBFEncoding;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class SimpleBFEncodingMapper extends BaseBFEncodingMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            encoding = new SimpleBFEncoding(inputSchema, outputSchema, uidColumn, encodingColumns, N, K, Q);
        } catch (BFEncodingException e) {
            throw new IOException(e.getCause());
        }
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {

        final GenericRecord record = key.datum();
        final GenericRecord encodingRecord = new GenericData.Record(encoding.getEncodingSchema());

        // selected for encoding columns
        List<Object> objs = new ArrayList<Object>();
        List<Class<?>> clzz = new ArrayList<Class<?>>();
        final String fieldName = ((SimpleBFEncoding) encoding).getEncodingColumnName();
        Schema fieldSchema = ((SimpleBFEncoding) encoding).getEncodingColumn().schema();
        for (Schema.Field field : encoding.getSelectedColumns()) {
            objs.add(record.get(field.name()));
            clzz.add(SUPPORTED_TYPES.get(field.schema().getType()));
        }
        encodingRecord.put(fieldName, encoding.encode(objs, clzz, fieldSchema));

        // rest of columns
        for (Schema.Field field : encoding.getRestColumns()) {
            Object obj = record.get(field.name());
            encodingRecord.put(field.name(), obj);
        }

        context.write(new AvroKey<GenericRecord>(encodingRecord), NullWritable.get());
    }
}
