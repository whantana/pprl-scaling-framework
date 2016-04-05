package gr.upatras.ceid.pprl.mapreduce;


import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static gr.upatras.ceid.pprl.mapreduce.RecordPairSimilarityReducer.getRecordPair;
import static gr.upatras.ceid.pprl.mapreduce.RecordPairSimilarityReducer.pairComplete;

public class RecordPairSimilarityCombiner extends Reducer<LongWritable, AvroValue<GenericRecord>,LongWritable,AvroValue<GenericRecord>> {

    private String[] fieldNames;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fieldNames = context.getConfiguration().getStrings(RecordPairSimilarityReducer.FIELD_NAMES_KEY);
        if(fieldNames == null) throw new InterruptedException("Fields names are not set.");
    }

    @Override
    protected void reduce(LongWritable key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        final GenericRecord[] recordPair = getRecordPair(values);

        if(!pairComplete(recordPair)) {
            final GenericRecord record = (recordPair[0] != null) ? recordPair[0] : recordPair[1];
            context.write(key, new AvroValue<GenericRecord>(record));
            return;
        }

        boolean[] vector = SimilarityMatrix.recordPairSimilarity(recordPair, fieldNames);
        context.getCounter(
                RecordPairSimilarityReducer.SIMILARITY_VECTORS_KEY ,
                String.valueOf(SimilarityMatrix.vector2Index(vector))).increment(1);

        context.getCounter(RecordPairSimilarityReducer.PAIRS_DONE_KEY,"combine").increment(1);
    }
}
