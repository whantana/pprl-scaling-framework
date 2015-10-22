package gr.upatras.ceid.pprl.encoding.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static gr.upatras.ceid.pprl.encoding.EncodingAvroSchemaUtil.getSchemaJsonFromHdfs;
import static gr.upatras.ceid.pprl.encoding.mapreduce.EncodeDatasetMapper.*;

public class EncodeDatasetTool extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "Encode Dataset";

    private static final Logger LOG = LoggerFactory.getLogger(EncodeDatasetTool.class);


    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 8) {
            LOG.error("Usage: EncodeDatasetTool <input-path> <input-schema> <col0,col1,...,colN> <FBF|RBF> <512|1024|2048> <k> <output-path> <output-schema>");
            return -1;
        }

        // input output and params
        final Path input = new Path(args[0]);
        final Schema inputSchema = getSchemaJsonFromHdfs(new Path(args[1]), conf);
        conf.set(INPUT_SCHEMA_KEY, inputSchema.toString());
        final List<String> columns = Arrays.asList(args[2].split(","));
        conf.setStrings(ENCODING_COLUMNS, args[2].split(","));
        final String encodingMethod = args[3];
        conf.set(ENCODING_METHOD,encodingMethod);
        final int bfSize = Integer.parseInt(args[4]);
        conf.setInt(BF_SIZE, bfSize);
        final int bfK = Integer.parseInt(args[5]);
        conf.setInt(BF_K,bfK);
        final Path output = new Path(args[6]);
        final Schema outputSchema = getSchemaJsonFromHdfs(new Path(args[7]), conf);
        conf.set(OUTPUT_SCHEMA_KEY,outputSchema.toString());

        final String description = JOB_DESCRIPTION + " (input-path=" + shortenUrl(input.toString()) + ", columns=" +
                columns + ", encoding-method=" + encodingMethod + "("+ bfSize + "," + bfK + "), output-path=" + shortenUrl(output.toString()) + ")";

        try {

            // setup map only job
            Job job = Job.getInstance(conf);
            job.setJarByClass(EncodeDatasetTool.class);
            job.setJobName(description);
            job.setNumReduceTasks(0);

            // setup input
            AvroKeyInputFormat.setInputPaths(job,input);
            AvroJob.setInputKeySchema(job,inputSchema);
            job.setInputFormatClass(AvroKeyInputFormat.class);

            // setup mapper
            job.setMapperClass(EncodeDatasetMapper.class);
            AvroJob.setMapOutputKeySchema(job, outputSchema);

            // setup output
            AvroKeyOutputFormat.setOutputPath(job, output);
            job.setOutputFormatClass(AvroKeyOutputFormat.class);

            // run job
            return (job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new EncodeDatasetTool(), args);
        System.exit(res);
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
