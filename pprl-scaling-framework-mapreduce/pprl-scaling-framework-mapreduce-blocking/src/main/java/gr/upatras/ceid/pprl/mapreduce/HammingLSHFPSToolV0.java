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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

import static gr.upatras.ceid.pprl.mapreduce.CommonUtil.*;

/**
 * Hamming LSH-FPS v0 tool class.
 */
public class HammingLSHFPSToolV0 extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHFPSToolV0.class);

    private static final String JOB_1_DESCRIPTION = "V0J1. Generate Total Pairs.";
    private static final String JOB_2_DESCRIPTION = "V0J2. Find Frequent Pairs.";
    private static final String JOB_3_DESCRIPTION = "V0J3. Find Matched Pairs.";

    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 18) {
            LOG.error("args.length= {}",args.length);
            for (int i = 0; i < args.length; i++) {
                LOG.error("args[{}] = {}",i,args[i]);
            }
            LOG.error("Usage: HammingLSHFPSToolV0 " +
                    "<alice-avro-path> <alice-schema-path> <alice-uid-field-name> " +
                    "<bob-avro-path> <bob-schema-path> <bob-uid-field-name> " +
                    "<all-pairs-path> <frequent-pair-path> <matched-pairs-path> <stats-path>" +
                    "<number-of-blocking-groups-L> <number-of-hashes-K> <frequent-pair-collision-limit-C> " +
                    "<number-of-reducers-job1> <number-of-reducers-job2> <number-of-reducers-job3> " +
                    "<hamming-threshold> <hlsh_seed>");
            throw new IllegalArgumentException("Invalid number of arguments.");
        }

        final Path alicePath = new Path(args[0]);
        final Path aliceSchemaPath = new Path(args[1]);
        final String aliceUidFieldName = args[2];
        final Path bobPath = new Path(args[3]);
        final Path bobSchemaPath = new Path(args[4]);
        final String bobUidFieldName = args[5];
        final Path allPairsPath = new Path(args[6]);
        final Path frequentPairsPath = new Path(args[7]);
        final Path matchedPairsPath = new Path(args[8]);
        final Path statsPath = new Path(args[9]);
        final int L = Integer.valueOf(args[10]);
        final int K = Integer.valueOf(args[11]);
        final short C = Short.valueOf(args[12]);
        final int R1 = Integer.valueOf(args[13]);
        final int R2 = Integer.valueOf(args[14]);
        final int R3 = Integer.valueOf(args[15]);
        final int hammingThreshold = Integer.valueOf(args[16]);
        final int seed = Integer.valueOf(args[17]);

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
        conf.setInt(CommonKeys.HAMMING_THRESHOLD, hammingThreshold);
        conf.setInt(CommonKeys.FREQUENT_PAIR_LIMIT, C);

        // setup job1
        final String description1 = String.format("%s(" +
                        "alice-path : %s, alice-schema-path : %s, " +
                        "bob-path : %s, bob-schema-path : %s, " +
                        "all-pairs-path : %s, " +
                        "L : %d, K : %d, R : %d)",
                JOB_1_DESCRIPTION,
                shortenUrl(alicePath.toString()), shortenUrl(aliceSchemaPath.toString()),
                shortenUrl(bobPath.toString()), shortenUrl(bobSchemaPath.toString()),
                shortenUrl(allPairsPath.toString()),
                L, K, R1);
        LOG.info("Running.1 : {}",description1);
        final Job job1 = Job.getInstance(conf);
        job1.setJarByClass(HammingLSHFPSToolV0.class);
        job1.setJobName(description1);
        job1.setNumReduceTasks(R1);
        job1.setSpeculativeExecution(false);

        // setup input & Mappers
        AvroKeyInputFormat.setInputPaths(job1, alicePath,bobPath);
        AvroJob.setInputKeySchema(job1, unionSchema);
        job1.setInputFormatClass(AvroKeyInputFormat.class);
        job1.setMapperClass(HammingLSHBlockingMapper.class);
        job1.setMapOutputKeyClass(BlockingKeyWritable.class);
        job1.setMapOutputValueClass(Text.class);

        // partitioner
        job1.setPartitionerClass(BlockingKeyWritablePartitioner.class);

        // sort class
        job1.setSortComparatorClass(BlockingKeyWritableComparator.class);

        // reducers & setup output
        job1.setReducerClass(GenerateIdPairsReducer.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        SequenceFileOutputFormat.setCompressOutput(job1,true);
        SequenceFileOutputFormat.setOutputCompressionType(job1,
                SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputPath(job1,allPairsPath);

        // run job 1
        final boolean job1Success = job1.waitForCompletion(true);
        if(!job1Success) {
            LOG.error("Job \"{}\" not successful",JOB_1_DESCRIPTION);
            fs.delete(statsPath.getParent(),true);
            return 1;
        }

        // cleanup and stats
        removeSuccessFile(fs,allPairsPath);
        populateStats(JOB_1_DESCRIPTION, job1, stats, LOG);

        // get important counters and add them to configuration
        final int aliceRecordCount = (int)job1.getCounters().findCounter(
                        CommonKeys.COUNTER_GROUP_NAME,CommonKeys.ALICE_RECORD_COUNT_COUNTER).getValue();
        final int bobRecordCount  = (int)job1.getCounters().findCounter(
                        CommonKeys.COUNTER_GROUP_NAME,CommonKeys.BOB_RECORD_COUNT_COUNTER).getValue();
        conf.setInt(CommonKeys.ALICE_RECORD_COUNT_COUNTER, aliceRecordCount);
        conf.setInt(CommonKeys.BOB_RECORD_COUNT_COUNTER, bobRecordCount);

        // setup job2
        final String description2 = String.format("%s(" +
                        "all-pairs-path : %s, frequent-pairs-path : %s, " +
                        " C: %d, R : %d)",
                JOB_2_DESCRIPTION,
                shortenUrl(allPairsPath.toString()),
                shortenUrl(frequentPairsPath.toString()),
                C, R2);
        LOG.info("Running.2 : {}",description2);
        final Job job2 = Job.getInstance(conf);
        job2.setJarByClass(HammingLSHFPSToolV0.class);
        job2.setJobName(description2);
        job2.setNumReduceTasks(R2);
        job2.setSpeculativeExecution(false);

        // setup input & mappers
        SequenceFileInputFormat.setInputPaths(job2, allPairsPath);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setMapperClass(CountIdPairsMapper.class);
        job2.setMapOutputKeyClass(TextPairWritable.class);
        job2.setMapOutputValueClass(ShortWritable.class);

        // setup combiner
        job2.setCombinerClass(FindFrequentIdPairsCombiner.class);

        // setup sort
        job2.setSortComparatorClass(TextPairWritableComparator.class);

        // reducers & setup output
        job2.setReducerClass(FindFrequentIdPairsReducer.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        SequenceFileOutputFormat.setCompressOutput(job2,true);
        SequenceFileOutputFormat.setOutputCompressionType(job2,
                SequenceFile.CompressionType.NONE);
        SequenceFileOutputFormat.setOutputPath(job2,frequentPairsPath);

        // run job 2
        final boolean job2Success = job2.waitForCompletion(true);
        if(!job2Success) {
            LOG.error("Job \"{}\" not successful",JOB_2_DESCRIPTION);
                        fs.delete(statsPath.getParent(),true);             return 1;
        }

        // cleanup and stats
        removeSuccessFile(fs,frequentPairsPath);
        populateStats(JOB_2_DESCRIPTION, job2, stats, LOG);

        // setup job3
        conf.setInt("mapreduce.map.memory.mb", 2048);
        conf.set("mapreduce.map.java.opts","-javaagent:./classmexer-0.0.3.jar -Xms1000m -Xmx1800m");
        conf.setInt("mapreduce.reduce.memory.mb", 1024);
        conf.set("mapreduce.reduce.java.opts","-javaagent:./classmexer-0.0.3.jar -Xms800m -Xmx800m");

        final String description3 = String.format("%s(" +
                        "alice-path : %s, alice-schema-path : %s, " +
                        "bob-path : %s, bob-schema-path : %s, " +
                        "frequent-pairs-path: %s, matched-pairs-path : %s, " +
                        " R : %d)",
                JOB_3_DESCRIPTION,
                shortenUrl(alicePath.toString()), shortenUrl(aliceSchemaPath.toString()),
                shortenUrl(bobPath.toString()), shortenUrl(bobSchemaPath.toString()),
                shortenUrl(frequentPairsPath.toString()), shortenUrl(matchedPairsPath.toString()),R3);
        LOG.info("Running.3 : {}",description3);
        final Job job3 = Job.getInstance(conf);
        job3.setJarByClass(HammingLSHFPSToolV0.class);
        job3.setJobName(description3);
        job3.setNumReduceTasks(R3);
        job3.setSpeculativeExecution(false);

        // setup  cache
        addContainingPathsToCache(job3, fs, frequentPairsPath);

        // setup input & mappers
        AvroKeyInputFormat.setInputPaths(job3, alicePath,bobPath);
        AvroJob.setInputKeySchema(job3, unionSchema);
        job3.setInputFormatClass(AvroKeyInputFormat.class);
        job3.setMapperClass(MakeRecordPairsMapper.class);
        job3.setMapOutputKeyClass(TextPairWritable.class);
        AvroJob.setMapOutputValueSchema(job3,unionSchema);

        // setup sort
        job3.setSortComparatorClass(TextPairWritableComparator.class);

        // reducers & setup output
        job3.setReducerClass(PrivateSimilarityReducer.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        SequenceFileOutputFormat.setCompressOutput(job3,true);
        SequenceFileOutputFormat.setOutputCompressionType(job3,
                SequenceFile.CompressionType.NONE);
        SequenceFileOutputFormat.setOutputPath(job3,matchedPairsPath);

        // run job 3
        final boolean job3Success = job3.waitForCompletion(true);
        if(!job3Success) {
            LOG.error("Job \"{}\" not successful",JOB_3_DESCRIPTION);
            fs.delete(statsPath.getParent(),true);
            return 1;
        }

        // cleanup and stats
        removeSuccessFile(fs,matchedPairsPath);
        populateStats(JOB_3_DESCRIPTION, job3, stats, LOG);


        // all jobs are succesfull save counters to stats path
        LOG.info("All jobs are succesfull. Frequent pairs: \"{}\" , Matched pairs: \"{}\" .",
                frequentPairsPath, matchedPairsPath);
        saveStats(fs, statsPath, stats);
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
