package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.*;

/**
 * Hamming LSH-FPS v3 tool class.
 */
public class HammingLSHFPSToolV3 extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHFPSToolV3.class);
    private static final String VERSION = "V3";
    private static final String JOB_1 = VERSION + "J1";
    private static final String JOB_2 = VERSION + "J2";
    private static final String JOB_1_DESCRIPTION = JOB_1 + ". Generate Bob Blocking buckets";
    private static final String JOB_2_DESCRIPTION = JOB_2 + ". Find Matched Pairs.";

    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 17) {
            LOG.error("args.length= {}", args.length);
            for (int i = 0; i < args.length; i++) {
                LOG.error("args[{}] = {}", i, args[i]);
            }
            LOG.error("Usage: HammingLSHFPSToolV3 " +
                    "<alice-avro-path> <alice-schema-path> <alice-uid-field-name> " +
                    "<bob-avro-path> <bob-schema-path> <bob-uid-field-name> " +
                    "<bob-buckets-path> <matched-pairs-path> <stats-path>" +
                    "<number-of-blocking-groups-L> <number-of-hashes-K> <frequent-pair-collision-limit-C> " +
                    "<number-of-reducers-job1>" +
                    "<mem-profile-1> <mem-profile-2>" +
                    "<hamming-threshold> <seed>");
            throw new IllegalArgumentException("Invalid number of arguments.");
        }

        final Path alicePath = new Path(args[0]);
        final Path aliceSchemaPath = new Path(args[1]);
        final String aliceUidFieldName = args[2];
        final Path bobPath = new Path(args[3]);
        final Path bobSchemaPath = new Path(args[4]);
        final String bobUidFieldName = args[5];
        final Path bobBucketsPath = new Path(args[6]);
        final Path matchedPairsPath = new Path(args[7]);
        final Path statsPath = new Path(args[8]);
        final int L = Integer.valueOf(args[9]);
        final int K = Integer.valueOf(args[10]);
        final short C = Short.valueOf(args[11]);
        final int R1 = Integer.valueOf(args[12]);
        final String memProfile1 = args[13];
        final String memProfile2 = args[14];
        final int hammingThrehold = Integer.valueOf(args[15]);
        final int seed = Integer.valueOf(args[16]);

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
        final BloomFilterEncoding bobEncoding = BloomFilterEncodingUtil.setupNewInstance(bobEncodingSchema);
        final HammingLSHBlocking blocking = (seed >= 0 ) ?
                new HammingLSHBlocking(L,K,seed,aliceEncoding,bobEncoding) :
                new HammingLSHBlocking(L,K,aliceEncoding,bobEncoding);
        final HammingLSHFPSStatistics stats = new HammingLSHFPSStatistics();


        conf.set(CommonKeys.ALICE_SCHEMA,aliceEncodingSchema.toString());
        conf.set(CommonKeys.ALICE_UID,aliceUidFieldName);
        conf.set(CommonKeys.BOB_SCHEMA,bobEncodingSchema.toString());
        conf.set(CommonKeys.BOB_UID,bobUidFieldName);
        conf.setInt(CommonKeys.BLOCKING_GROUP_COUNT, L);
        conf.setStrings(CommonKeys.BLOCKING_KEYS,blocking.groupsAsStrings());
        conf.setInt(CommonKeys.HAMMING_THRESHOLD,hammingThrehold);
        conf.setInt(CommonKeys.FREQUENT_PAIR_LIMIT, C);

        // setup job1
        MemProfileUtil.setMemProfile(memProfile1,conf);
        final String description1 = String.format("%s(" +
                        "bob-path : %s, bob-schema-path : %s, " +
                        "bob-buckets-path : %s, " +
                        "L : %d, K : %d, R : %d)",
                JOB_1_DESCRIPTION,
                shortenUrl(bobPath.toString()), shortenUrl(bobSchemaPath.toString()),
                shortenUrl(bobBucketsPath.toString()),
                L, K, R1);
        LOG.info("Running.1 : {} : {}",memProfile1,description1);
        final Job job1 = Job.getInstance(conf);
        job1.setJarByClass(HammingLSHFPSToolV3.class);
        job1.setJobName(description1);
        job1.setNumReduceTasks(R1);
        job1.setSpeculativeExecution(false);

        // setup input & Mappers
        AvroKeyInputFormat.setInputPaths(job1, bobPath);
        AvroJob.setInputKeySchema(job1, bobEncodingSchema);
        job1.setInputFormatClass(AvroKeyInputFormat.class);
        job1.setMapperClass(HammingLSHBlockingMapper.class);
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
                SequenceFile.CompressionType.NONE);
        SequenceFileOutputFormat.setOutputPath(job1,bobBucketsPath);

        // run job 1
        final boolean job1Success = job1.waitForCompletion(true);
        if(!job1Success) {
            LOG.error("Job \"{}\" not successful",JOB_1_DESCRIPTION);
            final Path parentPath = statsPath.getParent();
            final Path renamedPath = new Path(parentPath.getParent(),"FAILED_" + parentPath.getName());
            fs.rename(parentPath, renamedPath);
            return 1;
        }
        // cleanup and stats
        removeSuccessFile(fs,bobBucketsPath);
        stats.populateStats(JOB_1, job1);
        stats.saveAndClearStats(fs, statsPath);

        // get important stats for the next job
        final int[] minMaxAvg = getMinMaxAvgBlockingKeyCounts(job1.getCounters(), L, R1);
        final int minKeyCount = minMaxAvg[0];
        final int maxKeyCount = minMaxAvg[1];
        final int avgKeyCount = minMaxAvg[2];
        final int bobRecordCount  = (int)job1.getCounters().findCounter(
                CommonKeys.COUNTER_GROUP_NAME,CommonKeys.BOB_RECORD_COUNT_COUNTER).getValue();
        conf.set(CommonKeys.BOB_DATA_PATH, bobPath.toUri().toString());
        conf.setInt(CommonKeys.BOB_RECORD_COUNT_COUNTER, bobRecordCount);
        conf.setInt(CommonKeys.BUCKET_INITIAL_CAPACITY,maxKeyCount);


        // setup job2
        MemProfileUtil.setMemProfile(memProfile2,conf);
        final String description2 = String.format("%s(" +
                        "alice-path : %s, alice-schema-path : %s, " +
                        "bob-path : %s, bob-schema-path : %s," +
                        "bob-buckets-path : %s, matched-pairs-path : %s, " +
                        "L : %d, K : %d, C : %d)",
                JOB_2_DESCRIPTION,
                shortenUrl(alicePath.toString()), shortenUrl(aliceSchemaPath.toString()),
                shortenUrl(bobPath.toString()), shortenUrl(bobSchemaPath.toString()),
                shortenUrl(bobBucketsPath.toString()), shortenUrl(matchedPairsPath.toString()),
                L, K, C);
        LOG.info("Running.2 : {} : {}",memProfile2,description2);
        final Job job2 = Job.getInstance(conf);
        job2.setJarByClass(HammingLSHFPSToolV3.class);
        job2.setJobName(description2);
        job2.setSpeculativeExecution(false);
        job2.setNumReduceTasks(0);

        // setup  cache
        addContainingPathsToCache(job2, fs, bobBucketsPath);

        AvroKeyInputFormat.setInputPaths(job2, alicePath);
        AvroJob.setInputKeySchema(job2, aliceEncodingSchema);
        job2.setInputFormatClass(AvroKeyInputFormat.class);
        job2.setMapperClass(FPSMapperV3.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        // setup output
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        SequenceFileOutputFormat.setCompressOutput(job2,true);
        SequenceFileOutputFormat.setOutputCompressionType(job2,
                SequenceFile.CompressionType.NONE);
        SequenceFileOutputFormat.setOutputPath(job2,matchedPairsPath);

        // run job 2
        final boolean job2success = job2.waitForCompletion(true);
        if(!job2success) {
            LOG.error("Job \"{}\" not successful",JOB_2_DESCRIPTION);
            final Path parentPath = statsPath.getParent();
            final Path renamedPath = new Path(parentPath.getParent(),"FAILED_" + parentPath.getName());
            fs.rename(parentPath, renamedPath);
            return 1;
        }

        // cleanup and stats
        removeSuccessFile(fs,matchedPairsPath);
        stats.populateStats(JOB_2, job2);
        stats.saveAndClearStats(fs, statsPath);

        // all jobs are succesfull save counters to stats path
        LOG.info("All jobs are succesfull. See \"{}\" for the matched pairs list.", matchedPairsPath);
        LOG.info("See \"{}\" for collected stats.", statsPath);

        return 0;
    }

    /**
     * Main
     *
     * @param args input args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HammingLSHFPSToolV0(), args);
        System.exit(res);
    }
}
