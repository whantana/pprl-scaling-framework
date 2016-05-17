package gr.upatras.ceid.pprl.mapreduce;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Hamming LSH Blocking Reducer class.
 */
public class GenerateIdPairsReducer extends Reducer<Text,Text,Text,Text> {

    private final Queue<Text> keyQ = new LinkedList<Text>();
    private final Queue<List<Text>> valQ = new LinkedList<List<Text>>();
    private long maxBlockingKeys = -1;
    private long minBlockingKeys = -1;
    private String currentBlockingGroup = null;
    private long currentBlockingKeysCount = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        evaluateMinMaxOnBlockingGroupChange(key);

        if(keyQ.isEmpty()) {
            if(key.toString().endsWith("_B")) return;
            keyQ.add(key);
            valQ.add(formValuesList(values));
            return;
        } else if(keyQ.size() == 2 && valQ.size() == 2) {
            keyQ.remove();
            valQ.remove();
        }

        final boolean isAliceIds = key.toString().endsWith("_A");
        final boolean isBobIds = key.toString().endsWith("_B");
        assert isAliceIds != isBobIds;

        if(isAliceIds) {
            keyQ.add(key);
            valQ.add(formValuesList(values));
        } else {
            Text previousKey = keyQ.remove();
            List<Text> previousValues = valQ.remove();
            if(!previousKey.toString().endsWith("_A"))
                throw new IllegalStateException("This must be an Alice entry.");
            if(equalBlockingKeys(previousKey,key)) {
                int pairsCounter = 0;
                for (Text bv : values) {
                    for (Text av : previousValues) {
                        context.write(av, bv);
                        pairsCounter++;
                    }
                }
                increaseTotalPairCounter(context,pairsCounter);
                currentBlockingKeysCount++;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        final int id = context.getTaskAttemptID().getTaskID().getId();
        if(currentBlockingKeysCount > 0) {
            maxBlockingKeys = (maxBlockingKeys == -1 || currentBlockingKeysCount > maxBlockingKeys) ?
                    currentBlockingKeysCount : maxBlockingKeys;
            minBlockingKeys = (minBlockingKeys == -1 || currentBlockingKeysCount < minBlockingKeys) ?
                    currentBlockingKeysCount : minBlockingKeys;
        }
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME,
                String.format("%d.%s",id,CommonKeys.MAX_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER)
            ).setValue(maxBlockingKeys);
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME,
                String.format("%d.%s",id,CommonKeys.MIN_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER)
            ).setValue(minBlockingKeys);
    }

    private void evaluateMinMaxOnBlockingGroupChange(final Text key) {
        final String blockingGroupId = extractBlockingGroupId(key);
        if(currentBlockingGroup == null) {
            currentBlockingGroup = blockingGroupId;
        } else if(!currentBlockingGroup.equals(blockingGroupId)){
            maxBlockingKeys = (maxBlockingKeys == -1 || currentBlockingKeysCount > maxBlockingKeys) ?
                    currentBlockingKeysCount : maxBlockingKeys;
            minBlockingKeys = (minBlockingKeys == -1 || currentBlockingKeysCount < minBlockingKeys) ?
                    currentBlockingKeysCount : minBlockingKeys;
            currentBlockingKeysCount = 0;
            currentBlockingGroup = blockingGroupId;
        }
    }

    /**
     * Returns the blocking group id.
     *
     * @param key key
     * @return the blocking group id.
     */
    private static String extractBlockingGroupId(final Text key) {
        return key.toString().substring(0,key.toString().indexOf('_'));
    }

    /**
     * Form a values list from the iterable.
     *
     * @param values iterable.
     * @return a values list from the iterable.
     */
    private static List<Text> formValuesList(final Iterable<Text> values) {
        List<Text> vlist = new ArrayList<Text>();
        for (Text v : values) vlist.add(new Text(v));
        return vlist;
    }

    /**
     * True if blocking keys are equal, false otherwise.
     *
     * @param key1 key 1.
     * @param key2 key 2.
     * @return true if blocking keys are equal, false otherwise.
     */
    private static boolean equalBlockingKeys(final Text key1, final Text key2) {
        final String s1 = key1.toString();
        final String s2 = key2.toString();
        return s1.substring(0,s1.lastIndexOf("_")).equals(s2.substring(0,s2.lastIndexOf("_")));
    }

    /**
     * Increate the total pair counter.
     * @param context <code>Context</code> instance.
     * @param value value to increase.
     */
    private static void increaseTotalPairCounter(final Context context, long value) {
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME, CommonKeys.TOTAL_PAIR_COUNTER).increment(value);
    }
}
