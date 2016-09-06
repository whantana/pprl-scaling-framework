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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.*;

/**
 * Hamming LSH-FPS v2 tool class.
 */
public class HammingLSHFPSToolV2 extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHFPSToolV2.class);

    private static final String JOB_1_DESCRIPTION = "V2J1. Generate Bob Blocking buckets";
    private static final String JOB_2_DESCRIPTION = "V2J2. Find Matched Pairs (FPS).";

    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 16) {
            LOG.error("args.length= {}", args.length);
            for (int i = 0; i < args.length; i++) {
                LOG.error("args[{}] = {}", i, args[i]);
            }
            LOG.error("Usage: HammingLSHFPSToolV2 " +
                    "<alice-avro-path> <alice-schema-path> <alice-uid-field-name> " +
                    "<bob-avro-path> <bob-schema-path> <bob-uid-field-name> " +
                    "<bob-buckets-path> <matched-pairs-path> <stats-path>" +
                    "<number-of-blocking-groups-L> <number-of-hashes-K> <frequent-pair-collision-limit-C> " +
                    "<number-of-reducers-job1> <number-of-reducers-job2> " +
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
        final int R2 = Integer.valueOf(args[13]);
        final int hammingThrehold = Integer.valueOf(args[14]);
        final int seed = Integer.valueOf(args[15]);

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
        final Map<String,Long> stats = new TreeMap<>();


        conf.set(CommonKeys.ALICE_SCHEMA,aliceEncodingSchema.toString());
        conf.set(CommonKeys.ALICE_UID,aliceUidFieldName);
        conf.set(CommonKeys.BOB_SCHEMA,bobEncodingSchema.toString());
        conf.set(CommonKeys.BOB_UID,bobUidFieldName);
        conf.setInt(CommonKeys.BLOCKING_GROUP_COUNT, L);
        conf.setStrings(CommonKeys.BLOCKING_KEYS,blocking.groupsAsStrings());
        conf.setInt(CommonKeys.HAMMING_THRESHOLD,hammingThrehold);
        conf.setInt(CommonKeys.FREQUENT_PAIR_LIMIT, C);


        // setup job1
        final String description1 = String.format("%s(" +
                        "bob-path : %s, bob-schema-path : %s, " +
                        "bob-buckets-path : %s, " +
                        "L : %d, K : %d, R : %d)",
                JOB_1_DESCRIPTION,
                shortenUrl(bobPath.toString()), shortenUrl(bobSchemaPath.toString()),
                shortenUrl(bobBucketsPath.toString()),
                L, K, R1);
        LOG.info("Running.1 : {}",description1);
        final Job job1 = Job.getInstance(conf);
        job1.setJarByClass(HammingLSHFPSToolV2.class);
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
                SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputPath(job1,bobBucketsPath);

        // run job 1
        final boolean job1Success = job1.waitForCompletion(true);
        if(!job1Success) {
            LOG.error("Job \"{}\" not successful",JOB_1_DESCRIPTION);
            return 1;
        }
        // cleanup and stats
        removeSuccessFile(fs,bobBucketsPath);
        populateStats(JOB_2_DESCRIPTION,job1, stats, LOG);

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
        LOG.info("Running.2 : {}",description2);
        final Job job2 = Job.getInstance(conf);
        job2.setJarByClass(HammingLSHFPSToolV2.class);
        job2.setJobName(description2);
        job2.setNumReduceTasks(R2);
        job2.setSpeculativeExecution(false);

        // setup  cache
        addContainingPathsToCache(job2, fs, bobBucketsPath);

        AvroKeyInputFormat.setInputPaths(job2, alicePath);
        AvroJob.setInputKeySchema(job2, aliceEncodingSchema);
        job2.setInputFormatClass(AvroKeyInputFormat.class);
        job2.setMapperClass(FPSMapperV2.class);
        AvroJob.setMapOutputKeySchema(job2, aliceEncodingSchema);
        job2.setMapOutputValueClass(Text.class);

        // setup output
        job2.setReducerClass(PrivateSimilarityReducerV2.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        SequenceFileOutputFormat.setCompressOutput(job2,true);
        SequenceFileOutputFormat.setOutputCompressionType(job2,
                SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputPath(job2,matchedPairsPath);

        // run job 2
        final boolean job2success = job2.waitForCompletion(true);
        if(!job2success) {
            LOG.error("Job \"{}\" not successful",JOB_2_DESCRIPTION);
            return 1;
        }

        // cleanup and stats
        removeSuccessFile(fs,matchedPairsPath);
        populateStats(JOB_2_DESCRIPTION,job2, stats, LOG);

        // all jobs are succesfull save counters to stats path
        LOG.info("All jobs are succesfull. See \"{}\" for the matched pairs list.", matchedPairsPath);
        saveStats(fs, statsPath, stats);
        LOG.info("See \"{}\" for collected stats.", statsPath);

        return 0;
    }
}
