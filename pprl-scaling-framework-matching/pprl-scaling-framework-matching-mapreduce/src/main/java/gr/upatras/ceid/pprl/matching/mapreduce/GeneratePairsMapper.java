package gr.upatras.ceid.pprl.matching.mapreduce;


import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;


public class GeneratePairsMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, LongWritable, AvroKey<GenericRecord>> {
    // TODO make a tool for this
    private int recordCount;
    private String uidFieldName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        recordCount = context.getConfiguration().getInt("record.count",-1);
        uidFieldName = context.getConfiguration().get("uid.field.name");
        if(recordCount <= 0) throw new InterruptedException("Record count is " + recordCount);
        if(uidFieldName == null) throw new InterruptedException("Must set the UID field");
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        GenericRecord record = key.datum();

        int keyInt = Integer.parseInt(String.valueOf(record.get(uidFieldName)));

        Iterator<Long> iterator = CombinatoricsUtil.ranksOfElementIterator(keyInt, recordCount);
        while(iterator.hasNext()) {
            long rank = iterator.next();
            context.write(new LongWritable(rank),key);
        }
    }
}
