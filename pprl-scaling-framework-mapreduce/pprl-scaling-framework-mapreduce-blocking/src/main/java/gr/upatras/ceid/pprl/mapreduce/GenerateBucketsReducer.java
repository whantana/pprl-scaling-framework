package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static gr.upatras.ceid.pprl.mapreduce.BlockingKeyStatistics.setMaxMinBlockingKeysCounters;
import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.increaseTotalBlockingKeyCount;

/**
 * Generate blocking buckets reducer class.
 */
public class GenerateBucketsReducer extends Reducer<BlockingKeyWritable,Text,BlockingKeyWritable,TextArrayWritable> {

    private BlockingKeyStatistics statistics;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        statistics = new BlockingKeyStatistics();
    }

    @Override
    protected void reduce(BlockingKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        statistics.refreshOnBlockingGroupUpdate(key.blockingGroupId);
        final List<Text> valuesList = new ArrayList<Text>();
        for (Text v : values)
            valuesList.add(new Text(v));
        final TextArrayWritable taw = new TextArrayWritable();
        taw.set(valuesList.toArray(new Text[valuesList.size()]));
        context.write(key,taw);
        statistics.increaseTotalBlockingKeysCount();
        statistics.increaseCurrentBlockingKeysCount();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        final int id = context.getTaskAttemptID().getTaskID().getId();
        statistics.refresh();
        increaseTotalBlockingKeyCount(context, statistics.getTotalBlockingKeysCount());
        setMaxMinBlockingKeysCounters(context, id,
                statistics.getMaxBlockingKeys(),
                statistics.getMinBlockingKeys()
        );
    }
}
