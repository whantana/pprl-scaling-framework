package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * Blocking key statistics class.
 */
public class BlockingKeyStatistics {
    private long maxBlockingKeys ;
    private long minBlockingKeys;
    private int currentBlockingGroupId;
    private long currentBlockingKeysCount;
    private long totalBlockingKeysCount;

    /**
     * Constructor
     */
    public BlockingKeyStatistics() {
        maxBlockingKeys = -1;
        minBlockingKeys = -1;
        currentBlockingGroupId = -1;
        currentBlockingKeysCount = 0;
        totalBlockingKeysCount = 0;
    }

    /**
     * Set the max min blocking keys counters in every reducer.
     *
     * @param context <code>Context</code> instance.
     * @param id a reducer task id (partition id 0,...,R).
     * @param maxValue max value to set.
     * @param minValue min value to set.
     */
    public static void setMaxMinBlockingKeysCounters(final Reducer.Context context, final int id,
                                                     final long maxValue, final long minValue) {
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME,
                String.format("%d.%s",id,CommonKeys.MAX_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER)
            ).setValue(maxValue);
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME,
                String.format("%d.%s",id,CommonKeys.MIN_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER)
            ).setValue(minValue);
    }

    /**
     * Increase current blocking key count.
     */
    public void increaseCurrentBlockingKeysCount() {
        currentBlockingKeysCount++;
    }


    /**
     * Increase current blocking key count.
     */
    public void increaseTotalBlockingKeysCount() {
        totalBlockingKeysCount++;
    }

    /**
     * Refreshes on blocking group id.
     *
     * @param blockingGroupId a blocking group id (int).
     */
    public void refreshOnBlockingGroupUpdate(final int blockingGroupId) {
        if(currentBlockingGroupId == -1) {
            currentBlockingGroupId = blockingGroupId;
        } else if(currentBlockingGroupId != blockingGroupId) {
            maxBlockingKeys = (maxBlockingKeys == -1 || currentBlockingKeysCount > maxBlockingKeys) ?
                    currentBlockingKeysCount : maxBlockingKeys;
            minBlockingKeys = (minBlockingKeys == -1 || currentBlockingKeysCount < minBlockingKeys) ?
                    currentBlockingKeysCount : minBlockingKeys;
            currentBlockingKeysCount = 0;
            currentBlockingGroupId = blockingGroupId;
        }
    }

    /**
     * Finilize results by comparing the remaining count if the can out-max/min the previous
     * counters.
     */
    public void refresh() {
        if(currentBlockingKeysCount > 0) {
            maxBlockingKeys = (maxBlockingKeys == -1 || currentBlockingKeysCount > maxBlockingKeys) ?
                    currentBlockingKeysCount : maxBlockingKeys;
            minBlockingKeys = (minBlockingKeys == -1 || currentBlockingKeysCount < minBlockingKeys) ?
                    currentBlockingKeysCount : minBlockingKeys;
        }
    }

    public long getMaxBlockingKeys() {
        return maxBlockingKeys;
    }

    public long getMinBlockingKeys() {
        return minBlockingKeys;
    }

    public long getTotalBlockingKeysCount() {
        return totalBlockingKeysCount;
    }
}
