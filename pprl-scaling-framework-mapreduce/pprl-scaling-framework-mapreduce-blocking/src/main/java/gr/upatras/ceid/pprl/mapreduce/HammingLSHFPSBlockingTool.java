package gr.upatras.ceid.pprl.mapreduce;

import avro.shaded.com.google.common.collect.Lists;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hamming LSH-FPS Blocking tool class
 */
public class HammingLSHFPSBlockingTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHFPSBlockingTool.class);

    private static final String JOB_1_DESCRIPTION = "Generate Blocking buckets";
    private static final String JOB_2_DESCRIPTION = "Find Frequent Pairs.";
    private static final String JOB_3_DESCRIPTION = "Find Matched Pairs.";

    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 18) {
            LOG.error("args.length= {}",args.length);
            for (int i = 0; i < args.length; i++) {
                LOG.error("args[{}] = {}",i,args[i]);
            }
            LOG.error("Usage: HammingLSHFPSBlockingTool " +
                    "<alice-avro-path> <alice-schema-path> <alice-uid-field-name> " +
                    "<bob-avro-path> <bob-schema-path> <bob-uid-field-name> " +
                    "<bob-buckets-path> <frequent-pair-path> <matched-pairs-path> <stats-path>" +
                    "<number-of-blocking-groups-L> <number-of-hashes-K> <frequent-pair-collision-limit-C> " +
                    "<number-of-reducers-job1> <number-of-reducers-job2> <number-of-reducers-job3> " +
                    "<similarity-method-name> <similarity-threshold>");
            throw new IllegalArgumentException("Invalid number of arguments.");
        }

        final Path alicePath = new Path(args[0]);
        final Path aliceSchemaPath = new Path(args[1]);
        final String aliceUidFieldName = args[2];
        final Path bobPath = new Path(args[3]);
        final Path bobSchemaPath = new Path(args[4]);
        final String bobUidFieldName = args[5];
        final Path bobBucketsPath = new Path(args[6]);
        final Path frequentPairsPath = new Path(args[7]);
        final Path matchedPairsPath = new Path(args[8]);
        final Path statsPath = new Path(args[9]);
        final int L = Integer.valueOf(args[10]);
        final int K = Integer.valueOf(args[11]);
        final short C = Short.valueOf(args[12]);
        final int R1 = Integer.valueOf(args[13]);
        final int R2 = Integer.valueOf(args[14]);
        final int R3 = Integer.valueOf(args[15]);
        final String similarityMethodName = args[16];
        final double similarityThreshold = Double.valueOf(args[17]);

        if(K < 1)
            throw new IllegalArgumentException("Number of hashes K cannot be smaller than 1.");
        if(C > L)
            throw new IllegalArgumentException("Frequent pair collision limit C cannot be greater " +
                    "than then number of blocking groups L.");
        if(L < R1)
            throw new IllegalArgumentException("Number of reducers R cannot be greater" +
                    " than number of blocking groups L.");

        final FileSystem fs = FileSystem.get(conf);
        final Schema aliceEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(fs, aliceSchemaPath);
        final BloomFilterEncoding aliceEncoding = BloomFilterEncodingUtil.setupNewInstance(aliceEncodingSchema);
        final Schema bobEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(fs, bobSchemaPath);
        final Schema unionSchema = Schema.createUnion(Lists.newArrayList(aliceEncodingSchema, bobEncodingSchema));
        final BloomFilterEncoding bobEncoding = BloomFilterEncodingUtil.setupNewInstance(bobEncodingSchema);
        final HammingLSHBlocking blocking = new HammingLSHBlocking(L,K,aliceEncoding,bobEncoding);

        conf.set(CommonKeys.ALICE_SCHEMA_KEY,aliceEncodingSchema.toString());
        conf.set(CommonKeys.ALICE_UID_KEY,aliceUidFieldName);
        conf.set(CommonKeys.BOB_SCHEMA_KEY,bobEncodingSchema.toString());
        conf.set(CommonKeys.BOB_UID_KEY,bobUidFieldName);
        conf.setStrings(CommonKeys.BLOCKING_KEYS_KEY,blocking.groupsAsStrings());
        conf.set(CommonKeys.SIMILARITY_METHOD_NAME_KEY,similarityMethodName);
        conf.setDouble(CommonKeys.SIMILARITY_THRESHOLD_KEY,similarityThreshold);
        conf.setInt(CommonKeys.FREQUENT_PAIR_LIMIT, C);

        // setup job1
        final String description1 = String.format("%s(" +
                        "bob-path : %s, bob-schema-path : %s," +
                        "bob-buckets-path : %s," +
                        "L : %d, K : %d, R : %d)",
                JOB_1_DESCRIPTION,
                shortenUrl(bobPath.toString()), shortenUrl(bobSchemaPath.toString()),
                shortenUrl(bobBucketsPath.toString()),
                L, K, R1);
        LOG.info("Running.1 : {}",description1);
        final Job job1 = Job.getInstance(conf);
        job1.setJarByClass(HammingLSHBlockingTool.class);
        job1.setJobName(description1);
        job1.setNumReduceTasks(R1);

        // setup input & Mappers
        AvroKeyInputFormat.setInputPaths(job1, alicePath, bobPath);
        AvroJob.setInputKeySchema(job1, bobEncodingSchema);
        job1.setInputFormatClass(AvroKeyInputFormat.class);
        job1.setMapperClass(HammingLSHBlockingMapperV2.class);
        job1.setMapOutputKeyClass(BlockingKeyWritable.class);
        job1.setMapOutputValueClass(Text.class);

        // partitioner
        job1.setPartitionerClass(BlockingKeyWritablePartitioner.class);

        // sort class
        job1.setSortComparatorClass(BlockingKeyWritableComparator.class);

        // reducers & setup output
        job1.setReducerClass(GenerateBucketsReducer.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputKeyClass(BlockingKeyWritable.class);
        job1.setOutputValueClass(TextArrayWritable.class);
        SequenceFileOutputFormat.setCompressOutput(job1,true);
        SequenceFileOutputFormat.setOutputCompressionType(job1,
                SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputPath(job1,bobBucketsPath);

        // run job 1
        final boolean job1Success = job1.waitForCompletion(true);
        if(!job1Success) {
            LOG.error("Job \"{}\"not successful",JOB_1_DESCRIPTION);
            return 1;
        }
        LOG.info("Counters : ");
        for(Counter c : job1.getCounters().getGroup(CommonKeys.COUNTER_GROUP_NAME))
            LOG.info("\t{} : {}",c.getDisplayName(),c.getValue());

        // get important stats for the next job
        final int[] minMaxAvg = getMinMaxAvgBlockingKeyCounts(job1.getCounters(),L,R1);
        final int minKeyCount = minMaxAvg[0];
        final int maxKeyCount = minMaxAvg[1];
        final int avgKeyCount = minMaxAvg[2];
        final int bobRecordCount  = (int)job1.getCounters().findCounter(
                CommonKeys.COUNTER_GROUP_NAME,CommonKeys.BOB_RECORD_COUNT_COUNTER).getValue();






        conf.setInt(CommonKeys.BOB_RECORD_COUNT_COUNTER, bobRecordCount);


//        conf.setInt(CommonKeys.BUCKET_INITIAL_CAPACITY);


// TODO after job2 set the alice recordCount :
//        final int aliceRecordCount = (int)job1.getCounters().findCounter(
//                CommonKeys.COUNTER_GROUP_NAME,CommonKeys.ALICE_RECORD_COUNT_COUNTER).getValue();
//        conf.setInt(CommonKeys.ALICE_RECORD_COUNT_COUNTER, aliceRecordCount);


//        saveCountersToStats(fs,statsPath,
//                job1.getCounters().getGroup(CommonKeys.COUNTER_GROUP_NAME),
//                job2.getCounters().getGroup(CommonKeys.COUNTER_GROUP_NAME),
//                job3.getCounters().getGroup(CommonKeys.COUNTER_GROUP_NAME));
//        LOG.info("See \"{}\" for collected stats.", statsPath);

        return 0;
    }

    /**
     * Main
     *
     * @param args input args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HammingLSHBlockingTool(), args);
        System.exit(res);
    }

    /**
     * Shortens the given URL string.
     *
     * @param url URL string
     * @return shorten URL string.
     */
    private static String shortenUrl(final String url) {
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
    private static void saveCountersToStats(final FileSystem fs, final Path statsPath,final CounterGroup... groups)
            throws IOException {
        final FSDataOutputStream fsdos = fs.create(statsPath, true);
        for(CounterGroup group : groups) {
            for(Counter c : group) {
                fsdos.writeBytes(String.format("%s : %d\n",c.getDisplayName(),c.getValue()));
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
    private static int[] getMinMaxAvgBlockingKeyCounts(final Counters counters,int L,int R) {
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
}
