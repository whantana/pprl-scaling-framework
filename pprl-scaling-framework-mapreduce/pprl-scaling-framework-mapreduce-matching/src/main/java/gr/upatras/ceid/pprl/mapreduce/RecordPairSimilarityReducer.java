package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RecordPairSimilarityReducer extends Reducer<LongWritable, AvroValue<GenericRecord>,NullWritable,NullWritable> {
    public static String SIMILARITY_VECTORS_KEY = "similarity.vectors";
    public static String FIELD_NAMES_KEY = "field.names";
    public static String PAIRS_DONE_KEY = "pairs.done";
    private String[] fieldNames;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fieldNames = context.getConfiguration().getStrings(FIELD_NAMES_KEY);
        if(fieldNames == null) throw new InterruptedException("Fields names are not set.");
    }

    @Override
    protected void reduce(LongWritable key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        final GenericRecord[] recordPair = getRecordPair(values);

        if(!pairComplete(recordPair)) throw new IllegalStateException("Pair must be complete at this point");

        boolean[] vector = SimilarityMatrix.recordPairSimilarity(recordPair,fieldNames);
        context.getCounter(
                SIMILARITY_VECTORS_KEY ,
                String.valueOf(SimilarityMatrix.vector2Index(vector))).increment(1);

        context.getCounter(PAIRS_DONE_KEY,"reduce").increment(1);
    }

    public static GenericRecord[] getRecordPair(Iterable<AvroValue<GenericRecord>> values) {
        int i = 0;
        final GenericRecord[] pair = new GenericRecord[2];
        pair[0] = null;
        pair[1] = null;
        for (AvroValue<GenericRecord> val : values) {
            if(i > 2) throw new IllegalStateException("Values must be have size of 2.");
            pair[i++] = val.datum();
        }
        if ((pair[0] == null) && (pair[1] == null)) throw new IllegalStateException("Both records are null");
        return pair;
    }

    public static boolean pairComplete(final GenericRecord[] pair) {
        return (pair[0] != null) && (pair[1] != null);
    }
}
