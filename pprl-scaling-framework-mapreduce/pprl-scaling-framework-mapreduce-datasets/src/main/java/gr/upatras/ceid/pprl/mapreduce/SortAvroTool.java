package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sort Avro tool class.
 */
public class SortAvroTool extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "Sort AVRO Records";


    private static final Logger LOG = LoggerFactory.getLogger(SortAvroTool.class);

    public int run(String[] args) throws Exception {

        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 6) {
            LOG.error("args.length= {}",args.length);
            for (int i = 0; i < args.length; i++) {
                LOG.error("args[{}] = {}",i,args[i]);
            }
            LOG.error("Usage: SortAvroTool <input-avro-path> <input-schema-path> <output-path> <output-schema-path> <reducers> <order-by-field-names>");
            throw new IllegalArgumentException("Invalid number of arguments.");
        }

        LOG.info("Input-path : {}", args[0]);
        final Path inputPath = new Path(args[0]);
        final Path inputSchemaPath = new Path(args[1]);
        final Path outputPath = new Path(args[2]);
        final Path outputSchemaPath = new Path(args[3]);
        final int reducersCount = Integer.parseInt(args[4]);
        final String[] fieldNames = args[5].split(",");

        final String description = String.format("%s(" +
                "input-path : %s, input-schema-path : %s," +
                "output-path : %s, output-schema-path : %s," +
                "reducers-count : %d, field-names : %s)",
                JOB_DESCRIPTION,
                shortenUrl(inputPath.toString()),shortenUrl(inputSchemaPath.toString()),
                shortenUrl(outputPath.toString()),shortenUrl(outputSchemaPath.toString()),
                reducersCount, Arrays.toString(fieldNames));
        LOG.info("Running : " + description);

        final FileSystem fs = FileSystem.get(conf);

        final Schema loadedSchema =
                DatasetsUtil.loadSchemaFromFSPath(fs, inputSchemaPath);
        final Schema sortedSchema =
                DatasetsUtil.updateSchemaWithOrderByFields(loadedSchema,fieldNames);

        conf.set(SortMapper.SORTED_SCHEMA_KEY,sortedSchema.toString());

        // setup job
        final Job job = Job.getInstance(conf);
        job.setJarByClass(SortAvroTool.class);
        job.setJobName(description);
        job.setNumReduceTasks(reducersCount);


        // setup input
        AvroKeyInputFormat.setInputPaths(job, inputPath);
        AvroJob.setInputKeySchema(job, loadedSchema);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // setup mapper
        job.setMapperClass(SortMapper.class);
        AvroJob.setMapOutputKeySchema(job,sortedSchema);
        job.setMapOutputValueClass(NullWritable.class);

        // setup reducer
        job.setReducerClass(Reducer.class);
        AvroJob.setOutputKeySchema(job, sortedSchema);
        job.setOutputValueClass(NullWritable.class);

        // setup output
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroKeyOutputFormat.setOutputPath(job, outputPath);

        // sort
        Path partitionPath = null;
        if(reducersCount > 1) {
            partitionPath = new Path(outputPath.getParent(), SortAvroTotalOrderPartitioner.DEFAULT_PATH);
            if (fs.exists(partitionPath)) fs.delete(partitionPath);
            SortAvroTotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionPath);
            SortAvroInputSampler.writePartitionFile(job, new SortAvroInputSampler.RandomSampler(0.1, 40));
            LOG.info("Partition file created at \"{}\"",partitionPath);
            job.setPartitionerClass(SortAvroTotalOrderPartitioner.class);
            LOG.info("Sort Avro Total Order Partitioner set.");
        }

        // run job
        final boolean success = job.waitForCompletion(true);

        // delete partition file
        if(reducersCount > 1 || partitionPath != null) {
            if (fs.exists(partitionPath)) fs.delete(partitionPath);
            LOG.info("Partition file deleted at \"{}\"",partitionPath);
        }

        // if run was successful save schema and remove sucess file
        if(success) {
            removeSuccessFile(fs,outputPath);
            DatasetsUtil.saveSchemaToFSPath(fs, sortedSchema, outputSchemaPath);
        }

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SortAvroTool(), args);
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
