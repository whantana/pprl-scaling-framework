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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HammingLSHBlockingTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHBlockingTool.class);

    private static final String JOB_1_DESCRIPTION = "Create HammingLSH Buckets.";
    private static final String JOB_2_DESCRIPTION = "Count Record Pair Collisions.";
    private static final String JOB_3_DESCRIPTION = "Find Frequent Pairs.";

    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 13) {
            LOG.error("args.length= {}",args.length);
            for (int i = 0; i < args.length; i++) {
                LOG.error("args[{}] = {}",i,args[i]);
            }
            LOG.error("Usage: HammingLSHBlockingTool " +
                    "<alice-avro-path> <alice-schema-path> <alice-uid-fieldname> " +
                    "<bob-avro-path> <bob-schema-path> <bob-uid-fieldname> " +
                    "<bucket-path> <pair-count-path> <frequent-pair-path>" +
                    "<number-of-blocks> <number-of-hashes> <frequent-pair-colisions> " +
                    "<number-of-reducers>");
            throw new IllegalArgumentException("Invalid number of arguments.");
        }

        final Path alicePath = new Path(args[0]);
        final Path aliceSchemaPath = new Path(args[1]);
        final String aliceUidFieldName = args[2];
        final Path bobPath = new Path(args[3]);
        final Path bobSchemaPath = new Path(args[4]);
        final String bobUidFieldName = args[5];
        final Path bucketPath = new Path(args[6]);
        final Path pairCountPath = new Path(args[7]);
        final Path frequentPairPath = new Path(args[8]);
        final int L = Integer.valueOf(args[9]);
        final int K = Integer.valueOf(args[10]);
        final int C = Integer.valueOf(args[11]);
        final int R = Integer.valueOf(args[12]);

        if(L < R)
            throw new IllegalArgumentException("Number of reducers cannot be greater" +
                    " than number of blocking groups.");

        final String description1 = String.format("%s(" +
                        "alice-path : %s, alice-schema-path : %s," +
                        "bob-path : %s, bob-schema-path : %s," +
                        "L : %d, K : %s)",
                JOB_1_DESCRIPTION,
                shortenUrl(alicePath.toString()), shortenUrl(aliceSchemaPath.toString()),
                shortenUrl(bobPath.toString()), shortenUrl(bobSchemaPath.toString()),
                L, K);
        LOG.info("Running.1 : {}",description1);

        final FileSystem fs = FileSystem.get(conf);
        final Schema aliceEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(fs, aliceSchemaPath);
        final BloomFilterEncoding aliceEncoding = BloomFilterEncodingUtil.setupNewInstance(aliceEncodingSchema);
        final Schema bobEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(fs, bobSchemaPath);
        final Schema unionSchema = Schema.createUnion(Lists.newArrayList(aliceEncodingSchema, bobEncodingSchema));
        final BloomFilterEncoding bobEncoding = BloomFilterEncodingUtil.setupNewInstance(bobEncodingSchema);
        final HammingLSHBlocking blocking = new HammingLSHBlocking(L,K,aliceEncoding,bobEncoding);

        conf.set(HammingLSHBlockingMapper.ALICE_SCHEMA_KEY,aliceEncodingSchema.toString());
        conf.set(HammingLSHBlockingMapper.ALICE_UID_KEY,aliceUidFieldName);
        conf.set(HammingLSHBlockingMapper.BOB_SCHEMA_KEY,bobEncodingSchema.toString());
        conf.set(HammingLSHBlockingMapper.BOB_UID_KEY,bobUidFieldName);
        LOG.info("{}", Arrays.toString(blocking.groupsAsStrings()));
        conf.setStrings(HammingLSHBlockingMapper.BLOCKING_KEYS_KEY,blocking.groupsAsStrings());

        // setup job
        final Job job1 = Job.getInstance(conf);
        job1.setJarByClass(HammingLSHBlockingTool.class);
        job1.setJobName(description1);
        job1.setNumReduceTasks(R);

        // setup input & Mappers
        AvroKeyInputFormat.setInputPaths(job1, alicePath,bobPath);
        AvroJob.setInputKeySchema(job1, unionSchema);
        job1.setInputFormatClass(AvroKeyInputFormat.class);
        job1.setMapperClass(HammingLSHBlockingMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // partitioner
        job1.setPartitionerClass(HammingLSHBlockingPartitioner.class);

        // reducers & setup output
        // todo reducer must concetrate blocks


        final boolean job1Success = job1.waitForCompletion(true);

        // todo job2

        // todo job3


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
