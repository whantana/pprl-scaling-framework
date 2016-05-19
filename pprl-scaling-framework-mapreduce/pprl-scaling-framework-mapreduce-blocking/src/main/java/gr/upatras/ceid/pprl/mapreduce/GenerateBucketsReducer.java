package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generate blocking buckets reducer class.
 */
public class GenerateBucketsReducer extends Reducer<BlockingKeyWritable,Text,BlockingKeyWritable,TextArrayWritable> {

    private long maxBlockingKeys = -1;
    private long minBlockingKeys = -1;
    private long totalBlockingKeysCount = 0;
    private int currentBlockingGroupId = -1;
    private long currentBlockingKeysCount = 0;


    @Override
    protected void reduce(BlockingKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        checkIfCurrentBlockingGroupIdChanged(key);
        final List<Text> valuesList = new ArrayList<Text>();
        for (Text v : values)
            valuesList.add(new Text(v));
        final TextArrayWritable taw = new TextArrayWritable();
        taw.set(valuesList.toArray(new Text[valuesList.size()]));
        context.write(key,taw);
        currentBlockingKeysCount++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        final int id = context.getTaskAttemptID().getTaskID().getId();
        if(currentBlockingKeysCount > 0) {
            totalBlockingKeysCount += currentBlockingKeysCount;
            maxBlockingKeys = (maxBlockingKeys == -1 || currentBlockingKeysCount > maxBlockingKeys) ?
                    currentBlockingKeysCount : maxBlockingKeys;
            minBlockingKeys = (minBlockingKeys == -1 || currentBlockingKeysCount < minBlockingKeys) ?
                    currentBlockingKeysCount : minBlockingKeys;
        }
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME,
                CommonKeys.TOTAL_BLOCKING_KEYS_COUNTER).increment(totalBlockingKeysCount);
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME,
                String.format("%d.%s",id,CommonKeys.MAX_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER)
            ).setValue(maxBlockingKeys);
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME,
                String.format("%d.%s",id,CommonKeys.MIN_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER)
            ).setValue(minBlockingKeys);
    }

    /**
     * If reducer moved on to the next blocking group we can evaluate if the
     * previous groupd had the max/min number of blocking keys for this reducer blocking keys.
     *
     * @param key a key.
     */
    private void checkIfCurrentBlockingGroupIdChanged(final BlockingKeyWritable key) {
        final int blockingGroupId = key.blockingGroupId;
        if(currentBlockingGroupId == -1) {
            currentBlockingGroupId = blockingGroupId;
        } else if(currentBlockingGroupId != blockingGroupId) {
            totalBlockingKeysCount += currentBlockingKeysCount;
            maxBlockingKeys = (maxBlockingKeys == -1 || currentBlockingKeysCount > maxBlockingKeys) ?
                    currentBlockingKeysCount : maxBlockingKeys;
            minBlockingKeys = (minBlockingKeys == -1 || currentBlockingKeysCount < minBlockingKeys) ?
                    currentBlockingKeysCount : minBlockingKeys;
            currentBlockingKeysCount = 0;
            currentBlockingGroupId = blockingGroupId;
        }
    }
}
