package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * A Hamming LSHH/FPS Staticstics
 */
public class HammingLSHFPSStatistics {

    private final Map<String,Long> stats = new TreeMap<>();


    /**
     * Save stats to stats files.
     *
     */
    public void saveAndClearStats(final FileSystem fs, final Path statsPath)
            throws IOException {
        final FSDataOutputStream fsdos = fs.exists(statsPath) ?
                fs.append(statsPath) :
                fs.create(statsPath,true);
        for (Map.Entry<String, Long> entry : stats.entrySet())
            fsdos.writeBytes(String.format("%s=%d\n", entry.getKey(), entry.getValue()));
        fsdos.close();
        stats.clear();
    }

    /**
     * Populate stats with counters.
     *
     * @param key a key.
     * @param job a map reduce job to get coutners .
     */
    public void populateStats(final String key,
                              final Job job) throws IOException {
        for(Counter counter : job.getCounters().getGroup(CommonKeys.COUNTER_GROUP_NAME)) {
            final String name = counter.getDisplayName();
            if(name.contains("blockingkeys.at")) continue;
            final long value = counter.getValue();
            stats.put(key +"_" + name, value);
        }
        final Counter totalWrittenBytesCounter =
                job.getCounters().findCounter("HDFS", FileSystemCounter.BYTES_WRITTEN);
        if(totalWrittenBytesCounter != null)
            stats.put(key + "_total.hdfs.written.bytes", totalWrittenBytesCounter.getValue());

        try {
            final long duration = job.getFinishTime() - job.getStartTime();
            stats.put(key + "_job.duration",duration);
        } catch (InterruptedException e) { throw new IOException(e.getMessage());}
    }

    /**
     * Load stats from HDFS stats files.
     *
     */
    public static String loadAsBenchmarkAsStringCSV(final FileSystem fs, final Path statsPath)
            throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(statsPath)));
        final long[] jobTime = new long[3];
        Arrays.fill(jobTime, 0);
        final long[] jobDiskFootprint = new long[3];
        Arrays.fill(jobDiskFootprint,0);
        final long[] jobMemFootprint = new long[3];
        Arrays.fill(jobMemFootprint,0);
        long totalPairCount = 0;
        long frequentPairCount = 0;
        long matchedPairCount = 0;
        try {
            String line;
            int job;
            line=br.readLine();
            while (line != null){
                if(line.startsWith("V")) {
                    final String[] parts = line.split("_");
                    job = Integer.valueOf(parts[0].substring(3));
                    final String key = parts[1].split("=")[0];
                    final int value = Integer.valueOf(parts[1].split("=")[1]);
                    switch (key) {
                        case "job.duration":
                            jobTime[job-1] = value;
                            break;
                        case "total.hdfs.written.bytes":
                            jobDiskFootprint[job-1] = value;
                            break;
                        case "mem.total.bytes":
                            jobMemFootprint[job-1] = value;
                            break;
                        case "total.pairs.count":
                            totalPairCount = value;
                            break;
                        case "frequent.pairs.count":
                            frequentPairCount = value;
                            break;
                        case "matched.pairs.count":
                            matchedPairCount = value;
                            break;
                        default:
                            break;
                    }
                }
                line = br.readLine();
            }
        } finally {
            br.close();
        }
        return String.format("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
                jobTime[0],jobTime[1],jobTime[2],
                jobDiskFootprint[0],jobDiskFootprint[1],jobDiskFootprint[2],
                jobMemFootprint[0],jobMemFootprint[1],jobMemFootprint[2],
                totalPairCount,frequentPairCount,matchedPairCount
                );
    }
}
