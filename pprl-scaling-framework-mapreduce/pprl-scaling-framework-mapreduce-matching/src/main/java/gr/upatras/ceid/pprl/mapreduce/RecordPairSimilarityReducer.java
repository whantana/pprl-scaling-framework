package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Record-Pair similarity reducer class.
 */
public class RecordPairSimilarityReducer extends Reducer<LongWritable, AvroValue<GenericRecord>,NullWritable,NullWritable> {
    public static String SIMILARITY_VECTORS_KEY = "similarity.vectors";
    public static String FIELD_NAMES_KEY = "field.names";
    public static String PAIRS_DONE_KEY = "pairs.done";
    public static String SCHEMA_KEY = "schema";

    private String[] fieldNames;
    private Schema schema;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        schema = (new Schema.Parser()).parse(context.getConfiguration().get(SCHEMA_KEY));
        fieldNames = context.getConfiguration().getStrings(FIELD_NAMES_KEY);
        if(fieldNames == null) throw new InterruptedException("Fields names are not set.");
    }

    @Override
    protected void reduce(LongWritable key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {

        final List<GenericRecord> list = new ArrayList<GenericRecord>();
        for(AvroValue<GenericRecord> value : values) {
            GenericRecord record = new GenericData.Record(schema);
            for (String fieldName : fieldNames)
                record.put(fieldName,value.datum().get(fieldName));
            list.add(record);
        }

        switch (list.size()) {
            case 2:
                final GenericRecord[] recordPair = {list.get(0),list.get(1)};
                boolean[] vector = SimilarityUtil.recordPairSimilarity(recordPair, fieldNames);
                context.getCounter(
                        RecordPairSimilarityReducer.SIMILARITY_VECTORS_KEY ,
                        String.valueOf(SimilarityVectorFrequencies.vector2Index(vector))).increment(1);
                context.getCounter(RecordPairSimilarityReducer.PAIRS_DONE_KEY,"reduce").increment(1);
                break;
            default:
                throw new IllegalStateException("No record pair!.");
        }
    }
}
