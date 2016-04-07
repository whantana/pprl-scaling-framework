package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Encoding Tool class.
 */
public class EncodingTool extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "Encode HDFS Data";

    private static final Logger LOG = LoggerFactory.getLogger(EncodingTool.class);

    /**
     * Run tool.
     *
     * @param args input args
     * @return 0 if job is successfully run, 1 otherwise.
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws BloomFilterEncodingException
     */
    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException,
            BloomFilterEncodingException {

        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 4) {
            LOG.error("Usage without encoding creation: EncodeDatasetTool " +
                    "<input-path> <input-schema> <encoding-path> <encoding-schema>\n");
            return -1;
        }
        final Path inputDataPath = new Path(args[0]);
        final Path inputSchemaPath = new Path(args[1]);
        final Path outputDataPath = new Path(args[2]);
        final Path outputSchemaPath = new Path(args[3]);
        if(FileSystem.get(conf).exists(outputDataPath)) {
            FileSystem.get(conf).delete(outputDataPath,true);
            LOG.info("Deleting path {}",outputDataPath);
        }
        final Schema inputSchema = loadAvroSchemaFromHdfs(FileSystem.get(conf), inputSchemaPath);
        conf.set(BloomFilterEncodingMapper.INPUT_SCHEMA_KEY,inputSchema.toString());
        final Schema outputSchema = loadAvroSchemaFromHdfs(FileSystem.get(conf), outputSchemaPath);
        conf.set(BloomFilterEncodingMapper.OUTPUT_SCHEMA_KEY,outputSchema.toString());

        // set description
        final String description = String.format("%s(" +
                        "input-path : %s, input-schema-path : %s," +
                        " output-path : %s, output-schema-path : %s)",
                JOB_DESCRIPTION,
                shortenUrl(inputDataPath.toString()),shortenUrl(inputSchemaPath.toString()),
                shortenUrl(outputDataPath.toString()),shortenUrl(outputDataPath.toString())
        );
        LOG.info("Running :" + description);

        // setup map only job
        final Job job = Job.getInstance(conf);
        job.setJarByClass(EncodingTool.class);
        job.setJobName(description);
        job.setNumReduceTasks(0);

        // setup input
        AvroKeyInputFormat.setInputPaths(job, inputDataPath);
        AvroJob.setInputKeySchema(job, inputSchema);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // setup mapper
        job.setMapperClass(BloomFilterEncodingMapper.class);
        AvroJob.setMapOutputKeySchema(job, outputSchema);

        // setup output
        AvroKeyOutputFormat.setOutputPath(job, outputDataPath);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // run job
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Main
     *
     * @param args input arguments.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new EncodingTool(), args);
        System.exit(res);
    }

    /**
     * Returns shorten URL.
     *
     * @param url URL to be shorten.
     * @return shorten URL.
     */
    private static String shortenUrl(final String url) {
        Pattern p = Pattern.compile(".*://.*?(/.*)");
        Matcher m = p.matcher(url);
        if (m.matches()) {
            return m.group(1);
        } else {
            p = Pattern.compile(".*?(/.*)");
            m = p.matcher(url);
            if (m.matches()) return m.group(1);
            else return url;
        }
    }

    /**
     * Load avro schema from the filesystem.
     *
     * @param fs a <code>FileSystem</code> reference.
     * @param schemaPath a path to schema file.
     * @return a <code>Schema</code> instance.
     * @throws IOException
     */
    private static Schema loadAvroSchemaFromHdfs(final FileSystem fs,final Path schemaPath)
            throws IOException {
        FSDataInputStream fsdis = fs.open(schemaPath);
        Schema schema = (new Schema.Parser()).parse(fsdis);
        fsdis.close();
        return schema;
    }
}
