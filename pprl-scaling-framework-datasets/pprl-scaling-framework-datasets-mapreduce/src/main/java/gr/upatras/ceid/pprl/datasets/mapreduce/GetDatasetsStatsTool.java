package gr.upatras.ceid.pprl.datasets.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetDatasetsStatsTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(GetDatasetsStatsTool.class);

    private static final String JOB_DESCRIPTION = "Calculate column related stats from dataset";

    private static final int[] AVAILABLE_Q_GRAMS = {1,2,3};

    public int run(String[] args) throws Exception {
        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 4) {
            LOG.error("Usage: GetStatsTool  <input-path> <input-schema> <output-path>" +
                    "<Q=1|2|3>");
            return -1;
        }

        final Path inputDataPath = new Path(args[0]);
        final Path inputSchemaPath = new Path(args[1]);
        final Schema inputSchema =
                loadAvroSchemaFromHdfs(FileSystem.get(conf), inputSchemaPath);

        final Path outputDataPath = new Path(args[2]);
        final int Q = Integer.valueOf(args[6]);
        if(!Arrays.asList(AVAILABLE_Q_GRAMS).contains(Q))
            throw new IllegalArgumentException("Error : " + Q + " Availble q-grams are : " +
                    Arrays.toString(AVAILABLE_Q_GRAMS));
        conf.set(GetDatasetsStatsMapper.INPUT_SCHEMA_KEY, inputSchema.toString());
        conf.setInt(GetDatasetsStatsMapper.Q_KEY, Q);
        conf.set("mapreduce.output.basename",String.format("stats_%d",Q));

        String description = JOB_DESCRIPTION + " ("
                + "input-path=" + shortenUrl(inputDataPath.toString()) + ", "
                + "input-schema-path=" + shortenUrl(inputSchemaPath.toString()) + ", "
                + "output-path=" + shortenUrl(outputDataPath.toString()) + ", "
                + "Q=" + Q + "-grams)";

        // setup map only job
        Job job = Job.getInstance(conf);
        job.setJarByClass(GetDatasetsStatsTool.class);
        job.setJobName(description);
        job.setNumReduceTasks(1);

        // setup input
        AvroKeyInputFormat.setInputPaths(job, inputDataPath);
        AvroJob.setInputKeySchema(job, inputSchema);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // setup mapper
        job.setMapperClass(GetDatasetsStatsMapper.class);

        // setup reducer
        job.setReducerClass(GetDatasetsStatsReducer.class);

        // setup output
        TextOutputFormat.setOutputPath(job, outputDataPath);
        job.setOutputFormatClass(TextOutputFormat.class);

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
