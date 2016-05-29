package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Find Frequent Id Pairs Combiner class.
 */
public class FindFrequentIdPairsCombiner extends Reducer<TextPairWritable,ShortWritable,TextPairWritable,ShortWritable> {
    private short C;
    private ShortWritable CW;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        C = (short) context.getConfiguration().getInt(CommonKeys.FREQUENT_PAIR_LIMIT, -1);
        if(C < 0) throw new InterruptedException("C is not set.");
        CW = new ShortWritable(C);
    }

    @Override
    protected void reduce(TextPairWritable key, Iterable<ShortWritable> values, Context context) throws IOException, InterruptedException {
        short sum = 0;
        for(ShortWritable v : values) {
            sum += v.get();
            if(sum == C) { context.write(key,CW); return; }
        }
        context.write(key,new ShortWritable(sum));
    }
}
