package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.BitSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common utility methods class.
 */
public class CommonUtil {

    /**
     * Shortens the given URL string.
     *
     * @param url URL string
     * @return shorten URL string.
     */
    static String shortenUrl(final String url) {
        Pattern p = Pattern.compile(".*://.*?(/.*)");
        Matcher m = p.matcher(url);
        if(m.matches()) {
            return m.group(1);
        } else {
            p = Pattern.compile(".*?(/.*)");
            m = p.matcher(url);
            if(m.matches()) return m.group(1);
            else return url;
        }
    }

    /**
     * Save counters to stats files.
     *
     * @param groups counter groups.
     */
    static void saveCountersToStats(final FileSystem fs, final Path statsPath, final CounterGroup... groups)
            throws IOException {
        final FSDataOutputStream fsdos = fs.create(statsPath, true);
        for(CounterGroup group : groups) {
            for(Counter c : group) {
                fsdos.writeBytes(String.format("%s=%d\n",c.getDisplayName(),c.getValue()));
            }
        }
        fsdos.close();
    }

    /**
     * Returns the min/max/avg blocking counts from the counters
     * of the blocking job.
     *
     * @param counters counters.
     * @param L number of blocking groups.
     * @param R number of partitions (reducer count of job).
     * @return the min/max/avg blocking counts from the counters
     * of the blocking job.
     */
    static int[] getMinMaxAvgBlockingKeyCounts(final Counters counters, int L, int R) {
        final int avgKeysInBlockingGroupCount = (int) counters.findCounter(
                CommonKeys.COUNTER_GROUP_NAME,CommonKeys.TOTAL_BLOCKING_KEYS_COUNTER).getValue() / L;

        int minKeysInBlockingGroupCount = (int) counters.findCounter(
                CommonKeys.COUNTER_GROUP_NAME,
                "0."+CommonKeys.MIN_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER).getValue();
        int maxKeysInBlockingGroupCount = (int) counters.findCounter(
                CommonKeys.COUNTER_GROUP_NAME,
                "0."+CommonKeys.MIN_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER).getValue();
        for (int i = 1; i < R; i++) {
            int min = (int)counters.findCounter(
                    CommonKeys.COUNTER_GROUP_NAME,
                    i+"."+CommonKeys.MIN_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER).getValue();
            if(min < minKeysInBlockingGroupCount)  minKeysInBlockingGroupCount = min;
            int max = (int)counters.findCounter(
                    CommonKeys.COUNTER_GROUP_NAME,
                    i+"."+CommonKeys.MAX_BLOCKING_KEYS_IN_ANY_GROUP_COUNTER).getValue();
            if(max > minKeysInBlockingGroupCount)  maxKeysInBlockingGroupCount = max;
        }

        return new int[]{minKeysInBlockingGroupCount,
                maxKeysInBlockingGroupCount,
                avgKeysInBlockingGroupCount};
    }


    /**
     * Increase the total pair counter.
     *
     * @param context <code>Context</code> instance.
     * @param value value to increase.
     */
    public static void increaseTotalPairCounter(final Reducer.Context context, final long value) {
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME, CommonKeys.TOTAL_PAIR_COUNTER).increment(value);
    }

    /**
     * Increase the total blocking counter.
     *
     * @param context <code>Context</code> instance.
     * @param value value to increase.
     */
    public static void increaseTotalBlockingKeyCount(Reducer.Context context, long value) {
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME,
                CommonKeys.TOTAL_BLOCKING_KEYS_COUNTER).increment(value);
    }

    /**
     * Increase the frequent pair counter.
     *
     * @param context <code>Context</code> instance.
     * @param value value to increase.
     */
    public static void increaseFrequentPairCounter(final Reducer.Context context, final long value) {
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME, CommonKeys.FREQUENT_PAIR_COUNTER).increment(value);
    }

    /**
     * Increase the frequent pair counter.
     *
     * @param context <code>Context</code> instance.
     * @param value value to increase.
     */
    public static void increaseFrequentPairCounter(final Mapper.Context context, final long value) {
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME, CommonKeys.FREQUENT_PAIR_COUNTER).increment(value);
    }

    /**
     * Increase the matched pair counter.
     *
     * @param context <code>Context</code> instance.
     * @param value value to increase.
     */
    public static void increaseMatchedPairsCounter(Reducer.Context context, long value) {
        context.getCounter(CommonKeys.COUNTER_GROUP_NAME,
                        CommonKeys.MATCHED_PAIR_COUNTER).increment(value);
    }

    /**
     * Increase the dataset total record counter.
     *
     * @param context <code>Context</code> instance.
     * @param dataset a char representating dataset (A for alice, B for Bob).
     * @param value value to increase.
     */
    public static void increaseRecordCounter(final Mapper.Context context, final char dataset, final long value) {
        context.getCounter(
                CommonKeys.COUNTER_GROUP_NAME,
                String.format("%s.%c",CommonKeys.RECORD_COUNT_COUNTER,dataset)).increment(value);
    }

    /**
     * Add all frequent pair files to cache.
     * @param job job.
     * @param fs file system.
     * @param frequentPairsPath frequent pairs path (parent).
     * @throws IOException
     */
    public static void addContainingPathsToCache(final Job job, final FileSystem fs, final Path frequentPairsPath)
            throws IOException {
        if(fs.isFile(frequentPairsPath)) {
            job.addCacheFile(fs.makeQualified(frequentPairsPath).toUri());
            return;
        }
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(frequentPairsPath, false);
        while(iterator.hasNext()) {
            LocatedFileStatus lfs = iterator.next();
            if (lfs.isFile()) job.addCacheFile(fs.makeQualified(lfs.getPath()).toUri());
        }
    }
}
