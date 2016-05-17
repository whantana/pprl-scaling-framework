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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HammingLSHBlockingTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHBlockingTool.class);

    private static final String JOB_1_DESCRIPTION = "Retrieve pairs by Haming LSH Blocking.";
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
            LOG.error("Usage: HammingLSHBlockingTool " +
                    "<alice-avro-path> <alice-schema-path> <alice-uid-fieldname> " +
                    "<bob-avro-path> <bob-schema-path> <bob-uid-fieldname> " +
                    "<all-pairs-path> <frequent-pair-path> <matched-pairs-path>" +
                    "<number-of-blocks> <number-of-hashes> <frequent-pair-collision-limit> " +
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
        final Path allPairsPath = new Path(args[6]);
        final Path frequentPairsPath = new Path(args[7]);
        final Path matchedPairsPath = new Path(args[8]);
        final int L = Integer.valueOf(args[9]);
        final int K = Integer.valueOf(args[10]);
        final short C = Short.valueOf(args[11]);
        final int R1 = Integer.valueOf(args[12]);
        final int R2 = Integer.valueOf(args[13]);
        final int R3 = Integer.valueOf(args[14]);
        final String similarityMethodName = args[15];
        final double similarityThreshold = Double.valueOf(args[16]);

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
                        "alice-path : %s, alice-schema-path : %s," +
                        "bob-path : %s, bob-schema-path : %s," +
                        "all-pairs-path : %s," +
                        "L : %d, K : %d, R : %d)",
                JOB_1_DESCRIPTION,
                shortenUrl(alicePath.toString()), shortenUrl(aliceSchemaPath.toString()),
                shortenUrl(bobPath.toString()), shortenUrl(bobSchemaPath.toString()),
                shortenUrl(allPairsPath.toString()),
                L, K, R1);
        LOG.info("Running.1 : {}",description1);
        final Job job1 = Job.getInstance(conf);
        job1.setJarByClass(HammingLSHBlockingTool.class);
        job1.setJobName(description1);
        job1.setNumReduceTasks(R1);

        // setup input & Mappers
        AvroKeyInputFormat.setInputPaths(job1, alicePath,bobPath);
        AvroJob.setInputKeySchema(job1, unionSchema);
        job1.setInputFormatClass(AvroKeyInputFormat.class);
        job1.setMapperClass(HammingLSHBlockingMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // partitioner
        job1.setPartitionerClass(BlockingGroupPartitioner.class);

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
            LOG.error("Job \"{}\"not successful",JOB_1_DESCRIPTION);
            return 1;
        }
        LOG.info("Counters : ");
        for(Counter c : job1.getCounters().getGroup(CommonKeys.COUNTER_GROUP_NAME))
            LOG.info("\t{} : {}",c.getDisplayName(),c.getValue());

        // setup job2
        final String description2 = String.format("%s(" +
                        "all-pairs-path : %s, frequent-pairs-path : %s," +
                        " C: %d, R : %d)",
                JOB_2_DESCRIPTION,
                shortenUrl(allPairsPath.toString()),
                shortenUrl(frequentPairsPath.toString()),
                C, R2);
        LOG.info("Running.2 : {}",description2);
        final Job job2 = Job.getInstance(conf);
        job2.setJarByClass(HammingLSHBlockingTool.class);
        job2.setJobName(description2);
        job2.setNumReduceTasks(R2);

        // setup input & mappers
        SequenceFileInputFormat.setInputPaths(job2, allPairsPath);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setMapperClass(CountIdPairsMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        // setup combiner
        job2.setCombinerClass(FindFrequentIdPairsCombiner.class);

        // reducers & setup output
        job2.setReducerClass(FindFrequentIdPairsReducer.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        SequenceFileOutputFormat.setCompressOutput(job2,true);
        SequenceFileOutputFormat.setOutputCompressionType(job2,
                SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputPath(job2,frequentPairsPath);

        // run job 2
        final boolean job2Success = job2.waitForCompletion(true);
        if(!job2Success) {
            LOG.error("Job \"{}\"not successful",JOB_2_DESCRIPTION);
            return 1;
        }
        fs.delete(allPairsPath,true);
        LOG.info("Counters : ");
        for(Counter c : job2.getCounters().getGroup(CommonKeys.COUNTER_GROUP_NAME))
            LOG.info("\t{} : {}",c.getDisplayName(),c.getValue());

        // setup job3
        final String description3 = String.format("%s(" +
                        "alice-path : %s, alice-schema-path : %s," +
                        "bob-path : %s, bob-schema-path : %s," +
                        "frequent-pairs-path: %s, matched-pairs-path : %s," +
                        " R : %d)",
                JOB_3_DESCRIPTION,
                shortenUrl(alicePath.toString()), shortenUrl(aliceSchemaPath.toString()),
                shortenUrl(bobPath.toString()), shortenUrl(bobSchemaPath.toString()),
                shortenUrl(frequentPairsPath.toString()), shortenUrl(matchedPairsPath.toString()),R3);
        LOG.info("Running.3 : {}",description3);
        final Job job3 = Job.getInstance(conf);
        job3.setJarByClass(HammingLSHBlockingTool.class);
        job3.setJobName(description3);
        job3.setNumReduceTasks(R3);

        // setup  cache
        FormRecordPairsMapper.addFrequentPairsToCache(job3, fs, frequentPairsPath);

        // setup input & mappers
        AvroKeyInputFormat.setInputPaths(job3, alicePath,bobPath);
        AvroJob.setInputKeySchema(job3, unionSchema);
        job3.setInputFormatClass(AvroKeyInputFormat.class);
        job3.setMapperClass(FormRecordPairsMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job3,unionSchema);

        // reducers & setup output
        job3.setReducerClass(PrivateSimilarityReducer.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        SequenceFileOutputFormat.setCompressOutput(job3,true);
        SequenceFileOutputFormat.setOutputCompressionType(job3,
                SequenceFile.CompressionType.NONE);
        SequenceFileOutputFormat.setOutputPath(job3,allPairsPath);


        final boolean job3Success = job2.waitForCompletion(true);
        if(!job3Success) {
            LOG.error("Job \"{}\"not successful",JOB_2_DESCRIPTION);
            return 1;
        }
        fs.delete(frequentPairsPath,true);
        LOG.info("Counters : ");
        for(Counter c : job3.getCounters().getGroup(CommonKeys.COUNTER_GROUP_NAME))
            LOG.info("\t{} : {}",c.getDisplayName(),c.getValue());

        LOG.info("All jobs are succesfull. See \"{}\" for the matched pairs list.", matchedPairsPath);
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
}
