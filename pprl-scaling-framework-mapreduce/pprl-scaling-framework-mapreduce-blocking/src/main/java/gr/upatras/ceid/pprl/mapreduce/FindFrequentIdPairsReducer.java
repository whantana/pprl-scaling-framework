package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Find frequent id pair reducer class.
 */
public class FindFrequentIdPairsReducer extends Reducer<Text,IntWritable,Text,Text> {
    private short C;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        C = (short) context.getConfiguration().getInt(CommonKeys.FREQUENT_PAIR_LIMIT_KEY, -1);
        if(C < 0) throw new InterruptedException("C is not set.");
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v : values) {
            sum += v.get();
            System.out.println(key + " " + sum);
            if(v.get() >= C || sum >= C) {
                String[] splitKeys = key.toString().split(CommonKeys.RECORD_PAIR_DELIMITER);
                context.write(new Text(splitKeys[0]),new Text(splitKeys[1]));
                return;
            }
        }
    }
}
