package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BaseBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RowBloomFilterEncodingMapper extends BaseBloomFilterEncodingMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        encoding = new RowBloomFilterEncoding(inputSchema,outputSchema,selectedColumnsNames,N,K,Q);
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
        List<Object> objs = new ArrayList<Object>();
        List<Class<?>> clzz = new ArrayList<Class<?>>();
        final String fieldName = ((RowBloomFilterEncoding) encoding).getEncodingColumnName();
        Schema fieldSchema = ((RowBloomFilterEncoding) encoding).getEncodingColumn().schema();
        for (Schema.Field field : selectedColumns) {
            objs.add(record.get(field.name()));
            clzz.add(BaseBloomFilterEncoding.SUPPORTED_TYPES.get(field.schema().getType()));
        }
        encodingRecord.put(fieldName, encoding.encode(objs, clzz, fieldSchema));

        // rest of columns
        for(Schema.Field field : restColumns) {
            Object obj = record.get(field.name());
            encodingRecord.put(field.name(), obj);
        }

        context.write(new AvroKey<GenericRecord>(encodingRecord), NullWritable.get());
    }
}
