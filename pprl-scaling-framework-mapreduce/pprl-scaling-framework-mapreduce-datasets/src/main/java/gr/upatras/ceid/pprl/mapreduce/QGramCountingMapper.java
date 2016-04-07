package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.qgram.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Q-Gram Counting Mapper class.
 */
public class QGramCountingMapper extends Mapper<AvroKey<GenericRecord>,NullWritable,NullWritable,NullWritable> {
    public static final String SCHEMA_KEY = "schema";
    public static final String FIELD_NAMES_KEY = "field.names";
    public static final String RECORD_COUNT_KEY = "record.count";

    public static final String[] STATISTICS = {
            "length",
            "2grams.count",
            "3grams.count",
            "4grams.count",
            "unique.2grams.count",
            "unique.3grams.count",
            "unique.4grams.count"
    };

    String[] fieldNames;   // field names
    Schema.Type[] types;   // field types


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fieldNames = context.getConfiguration().getStrings(FIELD_NAMES_KEY,new String[0]);
        if(fieldNames == null || fieldNames.length == 0)
            throw new IllegalStateException("Field names cannot be null.");
        final String schemaJson = context.getConfiguration().get(SCHEMA_KEY,null);
        if(schemaJson == null) throw new IllegalStateException("Schema cannot be null.");
        Schema schema = (new Schema.Parser()).parse(schemaJson);
        types = new Schema.Type[fieldNames.length];
        for (int i = 0; i < fieldNames.length ; i++)
            types[i] = schema.getField(fieldNames[i]).schema().getType();
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        context.getCounter("",RECORD_COUNT_KEY).increment(1);
        for (int i = 0; i < fieldNames.length;  i++) {     // for each field we calculate the count of {2,3,4}-grams(unique and not).
            final Object obj = record.get(fieldNames[i]);
            final Schema.Type type = types[i];
            final String counterGroupName = "f." + fieldNames[i];
            context.getCounter(counterGroupName, STATISTICS[0]).increment(String.valueOf(obj).length());
            context.getCounter(counterGroupName, STATISTICS[1]).increment(QGramUtil.calcQgramsCount(obj, type, 2));
            context.getCounter(counterGroupName, STATISTICS[2]).increment(QGramUtil.calcQgramsCount(obj, type, 3));
            context.getCounter(counterGroupName, STATISTICS[3]).increment(QGramUtil.calcQgramsCount(obj, type, 4));
            context.getCounter(counterGroupName, STATISTICS[4]).increment(QGramUtil.calcUniqueQgramsCount(obj, type, 2));
            context.getCounter(counterGroupName, STATISTICS[5]).increment(QGramUtil.calcUniqueQgramsCount(obj, type, 3));
            context.getCounter(counterGroupName, STATISTICS[6]).increment(QGramUtil.calcUniqueQgramsCount(obj, type, 4));
        }
    }
}
