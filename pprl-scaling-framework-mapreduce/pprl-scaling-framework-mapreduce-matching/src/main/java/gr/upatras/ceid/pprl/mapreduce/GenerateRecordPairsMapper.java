package gr.upatras.ceid.pprl.mapreduce;


import gr.upatras.ceid.pprl.combinatorics.CombinatoricsUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * Generate Pairs of Records Mapper class.
 */
public class GenerateRecordPairsMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, LongWritable, TextArrayWritable> {

    private int recordCount;
    private String uidFieldName;
    private String[] selectedFieldNames;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        recordCount = context.getConfiguration().getInt(CommonKeys.RECORD_COUNT_KEY, -1);
        if(recordCount <= 0) throw new InterruptedException("Record count is " + recordCount + ".");
        uidFieldName = context.getConfiguration().get("uid.field.name");
        if(uidFieldName == null) throw new InterruptedException("Must set the UID field.");
        selectedFieldNames = context.getConfiguration().getStrings(CommonKeys.FIELD_NAMES_KEY,null);
        if(selectedFieldNames == null) throw new InterruptedException("Must set the selected fields.");
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();

        final int keyInt = Integer.parseInt(String.valueOf(record.get(uidFieldName)));
        final TextArrayWritable taw = toWritableFieldValues(record,selectedFieldNames);

        Iterator<Long> iterator = CombinatoricsUtil.ranksOfElementIterator(keyInt, recordCount);

        while(iterator.hasNext()) {
            long rank = iterator.next();
            context.write(new LongWritable(rank),taw);
        }
    }

    /**
     * Records fields to a text array writable object.
     *
     * @param record a generic record.
     * @param fieldNames selected fields to be passed in the returned object
     * @return a text array writable object.
     */
    public static TextArrayWritable toWritableFieldValues(final GenericRecord record, final String[] fieldNames) {
        final Text[] values = new Text[fieldNames.length];
        for (int i = 0; i < fieldNames.length ; i++)
            values[i] = new Text(String.valueOf(record.get(fieldNames[i])));
        TextArrayWritable taw = new TextArrayWritable();
        taw.set(values);
        return taw;
    }
}
