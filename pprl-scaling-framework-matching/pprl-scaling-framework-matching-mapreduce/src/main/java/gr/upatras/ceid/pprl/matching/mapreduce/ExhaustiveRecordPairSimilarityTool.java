package gr.upatras.ceid.pprl.matching.mapreduce;


import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExhaustiveRecordPairSimilarityTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(ExhaustiveRecordPairSimilarityTool.class);

    private static final String JOB_DESCRIPTION = "Exhaustive Record-pair similiarity";

    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 7) {
            LOG.error("Usage: ExhaustiveRecordPairSimilarityTool " +
                    "<input-path> <input-schema-path> " +
                    "<output-path> " +
                    "<uid.field.name> <record-count> " +
                    "<comma-separated-field-names> <reducers-count>");
            throw new IllegalArgumentException("Invalid number of arguments (" + args.length + ").");
        }

        final Path input = new Path(args[0]);
        final Path inputSchemaPath = new Path(args[1]);
        final Path outputPath = new Path(args[2]);
        final String uidFieldName = args[3];
        final int recordCount  = Integer.valueOf(args[4]);
        final String[] fieldNames = args[5].contains(",") ? args[5].split(",") : new String[]{args[5]};
        final int reducersCount = Integer.valueOf(args[6]);

        conf.set(GenerateRecordPairsMapper.UID_FIELD_NAME_KEY,uidFieldName);
        conf.setInt(GenerateRecordPairsMapper.RECORD_COUNT_KEY, recordCount);
        conf.setStrings(RecordPairSimilarityReducer.FIELD_NAMES_KEY,fieldNames);
        final Schema inputSchema = loadAvroSchemaFromHdfs(FileSystem.get(conf), inputSchemaPath);


        // set description and log it
        final String description = String.format("%s(" +
                        "input-path : %s, input-schema-path : %s," +
                        "output-path : %s," +
                        "uid-field-name : %s, record-count : %d , field-names : %s" +
                        "reducers-count : %d)",
                JOB_DESCRIPTION,
                shortenUrl(input.toString()),shortenUrl(inputSchemaPath.toString()),
                shortenUrl(outputPath.toString()),
                uidFieldName,recordCount,Arrays.toString(fieldNames),reducersCount);
        LOG.info("Running :" + description);

        // setup job
        final Job job = Job.getInstance(conf);
        job.setJarByClass(ExhaustiveRecordPairSimilarityTool.class);
        job.setJobName(description);
        job.setNumReduceTasks(reducersCount);

        // setup input
        AvroKeyInputFormat.setInputPaths(job, input);
        AvroJob.setInputKeySchema(job, inputSchema);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // setup mapper
        job.setMapperClass(GenerateRecordPairsMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        AvroJob.setMapOutputValueSchema(job,inputSchema);

        // setup combiners and reducers
        job.setCombinerClass(RecordPairSimilarityCombiner.class);
        job.setReducerClass(RecordPairSimilarityReducer.class);

        // set ouput
        job.setOutputFormatClass(NullOutputFormat.class);

        // run job
        boolean success  = job.waitForCompletion(true);
        if(success) {
            final Counters counters = job.getCounters();
            long pairsDoneInCombine =
                    counters.findCounter(
                            RecordPairSimilarityReducer.PAIRS_DONE_KEY, "combine").getValue();
            long pairsDoneInReduce =
                    counters.findCounter(
                            RecordPairSimilarityReducer.PAIRS_DONE_KEY, "reduce").getValue();
            LOG.info("Pairs Done during COMBINE={}\tPairs Done during REDUCE={}",
                    pairsDoneInCombine,pairsDoneInReduce);

            return 0;
        } else throw new IllegalStateException("Job not successfull.");
    }
    public static void counters2Properties(final FileSystem fs,final Path outputPath,
                                           final Counters counters, final String[] fieldNames)
            throws IOException {
        final Properties properties = counters2Properties(counters, fieldNames);
        final FSDataOutputStream fsdos = fs.create(outputPath, true);
        properties.store(fsdos, "Similarity Matrix");
        fsdos.close();
        LOG.info("Properties stored at {}.",fs.makeQualified(outputPath));
    }

    public static Properties counters2Properties(final Counters counters, String[] fieldNames) {
        final SimilarityMatrix matrix = new SimilarityMatrix(fieldNames.length);
        for (int i = 0; i <  matrix.getVectorCounts().length; i++) {
            final Counter counter = counters.findCounter(
                    RecordPairSimilarityReducer.SIMILARITY_VECTORS_KEY,
                    String.valueOf(i));
            long value = counter.getValue();
            LOG.info("Counter {} value {}",counter.getDisplayName(),value);
            matrix.getVectorCounts()[i] = value;
        }
        return matrix.toProperties();
    }

    private static Schema loadAvroSchemaFromHdfs(final FileSystem fs,final Path schemaPath)
            throws IOException {
        FSDataInputStream fsdis = fs.open(schemaPath);
        Schema schema = (new Schema.Parser()).parse(fsdis);
        fsdis.close();
        return schema;
    }

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
