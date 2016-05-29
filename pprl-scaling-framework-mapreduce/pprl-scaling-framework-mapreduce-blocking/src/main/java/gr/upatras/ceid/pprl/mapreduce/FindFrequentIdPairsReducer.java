package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseFrequentPairCounter;


/**
 * Find frequent id pair reducer class.
 */
public class FindFrequentIdPairsReducer extends Reducer<TextPairWritable,ShortWritable,Text,Text> {
    private short C;
    private long frequentPairCount = 0;
    private ShortWritable CW;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        C = (short) context.getConfiguration().getInt(CommonKeys.FREQUENT_PAIR_LIMIT, -1);
        if(C < 0) throw new InterruptedException("C is not set.");
        CW = new ShortWritable(C);
    }

    @Override
    protected void reduce(TextPairWritable key, Iterable<ShortWritable> values, Context context)
            throws IOException, InterruptedException {
        short sum = 0;
		for(ShortWritable v : values) {
		   sum += v.get();
            if(v.equals(CW)  || sum >= C) {
		        context.write(new Text(key.getFirst()),new Text(key.getSecond()));
                frequentPairCount++;
                return;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        increaseFrequentPairCounter(context, frequentPairCount);
    }
}
