package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.counters.FileSystemCounterGroup;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
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
     * Save stats to stats files.
     *
     * @param stats a stats maps.
     */
    static void saveStats(final FileSystem fs, final Path statsPath, final Map<String,Long> stats)
            throws IOException {
        final FSDataOutputStream fsdos = fs.exists(statsPath) ?
                fs.append(statsPath) :
                fs.create(statsPath,true);
        fsdos.writeBytes("\n");
        for (Map.Entry<String, Long> entry : stats.entrySet())
            fsdos.writeBytes(String.format("%s=%d\n", entry.getKey(), entry.getValue()));
        fsdos.close();
        stats.clear();
    }

    /**
     * Populate stats with counters.
     *
     * @param job a map reduce job
     * @param stats stats map.
     * @param LOG a log instance for logging.
     */
    static void populateStats(final String header,
                              final Job job,
                              final Map<String, Long> stats, final Logger LOG) throws IOException {
        final String key = header.split(". ")[0];
        for(Counter counter : job.getCounters().getGroup(CommonKeys.COUNTER_GROUP_NAME)) {
            final String name = counter.getDisplayName();
            if(name.contains("blockingkeys.at")) continue;;
            final long value = counter.getValue();
            stats.put(key +"_" + name, value);
        }
        final Counter totalWrittenBytesCounter =
                job.getCounters().findCounter("HDFS",FileSystemCounter.BYTES_WRITTEN);
        if(totalWrittenBytesCounter != null)
            stats.put(key + "_total.hdfs.written.bytes", totalWrittenBytesCounter.getValue());

        try {
            final long duration = job.getFinishTime() - job.getStartTime();
            stats.put(key + "_job.duration",duration);
        } catch (InterruptedException e) { throw new IOException(e.getMessage());}
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
     * Increase the matched pair counter.
     *
     * @param context <code>Context</code> instance.
     * @param value value to increase.
     */
    public static void increaseMatchedPairsCounter(Mapper.Context context, long value) {
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
     * Increase total byte counters
     *
     * @param context <code>Context</code> instance.
     * @param value value to increase.
     */
    public static void increaseTotalByteCounter(final Mapper.Context context, final long value) {
        context.getCounter(
                CommonKeys.COUNTER_GROUP_NAME,
                CommonKeys.TOTAL_BYTES_ON_PPRL).increment(value);
    }

    /**
     * Increase total byte counters.
     *
     * @param context <code>Context</code> instance.
     * @param value value to increase.
     */
    public static void increaseTotalByteCounter(final Reducer.Context context, final long value) {
        context.getCounter(
                CommonKeys.COUNTER_GROUP_NAME,
                CommonKeys.TOTAL_BYTES_ON_PPRL).increment(value);
    }

    /**
     * Add all containing files files to cache.
     * @param job job.
     * @param fs file system.
     * @param parentPath a paraint path.
     * @throws IOException
     */
    public static void addContainingPathsToCache(final Job job, final FileSystem fs, final Path parentPath)
            throws IOException {
        if(fs.isFile(parentPath)) {
            job.addCacheFile(fs.makeQualified(parentPath).toUri());
            return;
        }
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(parentPath, false);
        while(iterator.hasNext()) {
            LocatedFileStatus lfs = iterator.next();
            if (lfs.isFile()) job.addCacheFile(fs.makeQualified(lfs.getPath()).toUri());
        }
    }

    /**
     * Remove _SUCCESS file from path.
     *
     * @param path a path.
     * @throws IOException
     */
    public static void removeSuccessFile(final FileSystem fs,
                                         final Path path) throws IOException {
        final Path p = new Path(path,"_SUCCESS");
        if (fs.exists(p)) fs.delete(p, false);
    }
}
