package gr.upatras.ceid.pprl.matching.mapreduce;

import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairSimilarityReducer extends Reducer<LongWritable, AvroKey<GenericRecord>,NullWritable,NullWritable> {

    private String[] fieldNames;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fieldNames = context.getConfiguration().getStrings("field.names",null);
        if(fieldNames == null) throw new InterruptedException("Fields names are not set.");
    }

    @Override
    protected void reduce(LongWritable key, Iterable<AvroKey<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        final GenericRecord[] recordPair = new GenericRecord[2];
        for (AvroKey<GenericRecord> val : values) {
            if(i > 2) throw new IllegalStateException("Values must be have size of 2.");
            recordPair[i++] = val.datum();
        }

        if(i == 1) throw new IllegalStateException("Values must be have size of 2.");

        final boolean[] vector = new boolean[fieldNames.length];
        i = 0;
        for (String fieldName : fieldNames) {
            final String v0 =  (String) recordPair[0].get(fieldName);
            final String v1 =  (String) recordPair[1].get(fieldName);
            vector[i++] =  SimilarityMatrix.similarity(SimilarityMatrix.DEFAULT_SIMILARITY_METHOD_NAME,v0,v1);
        }

        context.getCounter(
                "similarity.vectors",
                String.valueOf(SimilarityMatrix.vector2Index(vector))).increment(1);
    }
}