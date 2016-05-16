package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Record-Pair similarity reducer class.
 */
public class RecordPairSimilarityReducer extends Reducer<LongWritable, TextArrayWritable,NullWritable,NullWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

        final List<TextArrayWritable> list = new ArrayList<TextArrayWritable>();
        for(TextArrayWritable value : values) {
            TextArrayWritable copy = new TextArrayWritable();
            copy.set(value.get());
            list.add(copy);
        }

        switch (list.size()) {
            case 1:
                // ignore
                break;
            case 2:
                final TextArrayWritable[] pair = {list.get(0),list.get(1)};
                assert pair[0].getStrings().length == pair[1].getStrings().length;
                final String[][] strings = new String[2][pair[0].getStrings().length];
                System.arraycopy(pair[0].getStrings(),0,strings[0],0,pair[0].getStrings().length);
                System.arraycopy(pair[1].getStrings(),0,strings[1],0,pair[1].getStrings().length);
                boolean[] vector = SimilarityUtil.stringPairSimilarity(strings);
                context.getCounter(
                        CommonKeys.SIMILARITY_VECTORS_KEY ,
                        String.valueOf(SimilarityVectorFrequencies.vector2Index(vector))).increment(1);
                context.getCounter(CommonKeys.PAIRS_DONE_KEY,"reduce").increment(1);
                break;
            default:
                throw new IllegalStateException("No record pair!.");
        }
    }
}
