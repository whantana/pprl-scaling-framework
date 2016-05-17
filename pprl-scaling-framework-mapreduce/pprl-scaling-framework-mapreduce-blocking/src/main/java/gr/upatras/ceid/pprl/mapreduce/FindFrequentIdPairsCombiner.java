package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Find Frequent Id Pairs Combiner class.
 */
public class FindFrequentIdPairsCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    private short C;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        C = (short) context.getConfiguration().getInt(CommonKeys.FREQUENT_PAIR_LIMIT, -1);
        if(C < 0) throw new InterruptedException("C is not set.");
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v : values) {
            sum += v.get();
            if(sum >= C) { context.write(key,new IntWritable(sum)); return; }
        }
        context.write(key,new IntWritable(sum));
    }
}
