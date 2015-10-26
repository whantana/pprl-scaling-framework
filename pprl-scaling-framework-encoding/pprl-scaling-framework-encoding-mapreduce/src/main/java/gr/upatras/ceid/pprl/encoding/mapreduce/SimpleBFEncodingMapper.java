package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.SimpleBFEncoding;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class SimpleBFEncodingMapper extends BaseEncodeMapper {

    private SimpleBFEncoding encoding;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
//        encoding = new SimpleBFEncoding(inputSchema,outputSchema,encodingColumns,N,K,Q);
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
//        final GenericRecord encodedRecord = encoding.encodeRecord(key.datum());
//        context.write(new AvroKey<GenericRecord>(encodedRecord),NullWritable.get());
    }
}
