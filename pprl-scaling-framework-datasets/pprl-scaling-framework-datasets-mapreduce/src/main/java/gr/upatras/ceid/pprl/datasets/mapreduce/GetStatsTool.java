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

public class GetStatsTool extends Configured implements Tool { // TODO test me!

    private static final Logger LOG = LoggerFactory.getLogger(GetStatsTool.class);

    private static final String JOB_DESCRIPTION = "Calculate column related stats from dataset";

    private static final String[] AVAILABLE_Q_GRAMS = {"UNIGRAMS","BIGRAMS","TRIGRAMS"};


    public int run(String[] args) throws Exception {
        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 4) {
            LOG.error("Usage: GetStatsTool  <input-path> <input-schema> <output-path>" +
                    "<UNIGRAMS|BIGRAMS|TRIGRAMS>");
            return -1;
        }

        final Path inputDataPath = new Path(args[0]);
        final Path inputSchemaPath = new Path(args[1]);
        final Schema inputSchema =
                loadAvroSchemaFromHdfs(FileSystem.get(conf), inputSchemaPath);

        final Path outputDataPath = new Path(args[2]);
        final String gram = args[6];
        if(!Arrays.asList(AVAILABLE_Q_GRAMS).contains(gram))
            throw new IllegalArgumentException("Error : " + gram + " Availble q-grams are : " +
                    Arrays.toString(AVAILABLE_Q_GRAMS));
        final int selectedQ = Arrays.asList(AVAILABLE_Q_GRAMS).indexOf(gram) + 1;
        conf.set(GetStatsMapper.INPUT_SCHEMA_KEY, inputSchema.toString());
        conf.setInt(GetStatsMapper.Q_KEY, selectedQ);
        conf.set(TextOutputFormat.SEPERATOR,":");
        conf.set("mapreduce.textoutputformat.separator",":");

        String description = JOB_DESCRIPTION + " ("
                + "input-path=" + shortenUrl(inputDataPath.toString()) + ", "
                + "input-schema-path=" + shortenUrl(inputSchemaPath.toString()) + ", "
                + "output-path=" + shortenUrl(outputDataPath.toString()) + ", "
                + "selected-Q=" + selectedQ + ")";

        // setup map only job
        Job job = Job.getInstance(conf);
        job.setJarByClass(GetStatsTool.class);
        job.setJobName(description);
        job.setNumReduceTasks(1);

        // setup input
        AvroKeyInputFormat.setInputPaths(job, inputDataPath);
        AvroJob.setInputKeySchema(job, inputSchema);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // setup mapper
        job.setMapperClass(GetStatsMapper.class);

        // setup reducer
        job.setReducerClass(GetStatsReducer.class);

        // setup output
        TextOutputFormat.setOutputPath(job, outputDataPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        // run job
        return (job.waitForCompletion(true) ? 0 : 1);
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
