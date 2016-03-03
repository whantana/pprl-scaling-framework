package gr.upatras.ceid.pprl.datasets.mapreduce;

import gr.upatras.ceid.pprl.datasets.DatasetFieldStatistics;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetDatasetsStatsTool extends Configured implements Tool {

    // TODO probably remove this code for Spark code.

    private static final Logger LOG = LoggerFactory.getLogger(GetDatasetsStatsTool.class);

    private static final String JOB_DESCRIPTION = "Calculate column related stats from dataset";

    public int run(String[] args) throws Exception {
        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 3) {
            LOG.error("Usage: GetStatsTool  <input-path> <input-schema> <output-path>");
            return -1;
        }

        final Path inputDataPath = new Path(args[0]);
        final Path inputSchemaPath = new Path(args[1]);
        final Schema inputSchema =
                loadAvroSchemaFromHdfs(FileSystem.get(conf), inputSchemaPath);

        final Path outputDataPath = new Path(args[2]);
        final int Q = Integer.valueOf(args[3]);

        conf.set(GetDatasetsStatsMapper.INPUT_SCHEMA_KEY, inputSchema.toString());

        String description = JOB_DESCRIPTION + " ("
                + "input-path=" + shortenUrl(inputDataPath.toString()) + ", "
                + "input-schema-path=" + shortenUrl(inputSchemaPath.toString()) + ", "
                + "output-path=" + shortenUrl(outputDataPath.toString()) + ", "
                + "Q=" + Q + "-grams)";

        // setup map only job
        Job job = Job.getInstance(conf);
        job.setJarByClass(GetDatasetsStatsTool.class);
        job.setJobName(description);

        // setup input
        AvroKeyInputFormat.setInputPaths(job, inputDataPath);
        AvroJob.setInputKeySchema(job, inputSchema);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // setup mapper
        job.setMapperClass(GetDatasetsStatsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DatasetFieldStatistics.class);

        // setup reducer
        job.setNumReduceTasks(1);
        job.setReducerClass(GetDatasetsStatsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DatasetFieldStatistics.class);

        // setup output
        SequenceFileOutputFormat.setOutputPath(job,outputDataPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // run job
        return job.waitForCompletion(true) ? 0 : 1;
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


    private static Schema loadAvroSchemaFromHdfs(final FileSystem fs,final Path schemaPath) throws IOException {
        FSDataInputStream fsdis = fs.open(schemaPath);
        Schema schema = (new Schema.Parser()).parse(fsdis);
        fsdis.close();
        return schema;
    }
}
