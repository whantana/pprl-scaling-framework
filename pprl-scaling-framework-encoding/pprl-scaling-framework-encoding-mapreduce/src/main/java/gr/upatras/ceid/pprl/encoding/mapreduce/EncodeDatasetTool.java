package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
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
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EncodeDatasetTool extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "Encode Dataset";

    private static final Logger LOG = LoggerFactory.getLogger(EncodeDatasetTool.class);


    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException,
            BloomFilterEncodingException {

        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 10) {
            LOG.error(" Usage: EncodeDatasetTool <input-path> <input-schema> <encoding-path> <encoding-schema>\n" +
                    "\t<sel_field_1,sel_field_2,...,sel_field_M>\n" +
                    "\t<rest_field_1,rest_field_2,...,rest_field_R>\n" +
                    "\t<FBF|RBF> <N_1,N_2,...,N_M> <K> <Q>");
            return -1;
        }

        // set confuguration and params
        final Path inputDataPath = new Path(args[0]);
        final Path inputSchemaPath = new Path(args[1]);
        final Schema inputSchema =
                loadAvroSchemaFromHdfs(FileSystem.get(conf), inputSchemaPath);

        final Path outputDataPath = new Path(args[2]);
        final Path outputSchemaPath = new Path(args[3]);
        final Schema outputSchema =
                loadAvroSchemaFromHdfs(FileSystem.get(conf), outputSchemaPath);

        final String[] selectedFielNames = args[4].split(",");
        final String[] restFieldNames = args[5].split(",");
        final String methodName = args[6];
        if(!BloomFilterEncoding.AVAILABLE_METHODS.contains(methodName))
            throw new IllegalArgumentException("Error : " + methodName +
                    " Availble methods are : " + BloomFilterEncoding.AVAILABLE_METHODS);
        final int[] N = new int[args[7].split(",").length];
        if(N.length != selectedFielNames.length)
            throw new IllegalArgumentException("Error : N count must agree with selected field names count.");
        final String[] Nstr = new String[N.length];
        for (int i = 0; i < N.length; i++) {
            N[i] = Integer.parseInt(args[7].split(",")[i]);
            Nstr[i] = args[7].split(",")[i];
        }
        final int K = Integer.parseInt(args[8]);
        final int Q = Integer.parseInt(args[9]);

        conf.set(BloomFilterEncodingMapper.METHOD_NAME_KEY, methodName);
        conf.set(BloomFilterEncodingMapper.INPUT_SCHEMA_KEY, inputSchema.toString());
        conf.set(BloomFilterEncodingMapper.OUTPUT_SCHEMA_KEY, outputSchema.toString());
        conf.setStrings(BloomFilterEncodingMapper.SELECTED_FIELDS_KEY, selectedFielNames);
        conf.setStrings(BloomFilterEncodingMapper.REST_FIELDS_KEY, restFieldNames);
        conf.setStrings(BloomFilterEncodingMapper.N_KEY, Nstr);
        conf.setInt(BloomFilterEncodingMapper.K_KEY,K);
        conf.setInt(BloomFilterEncodingMapper.Q_KEY,Q);

        // set description
        String description = JOB_DESCRIPTION + " ("
                + String.format(" N=%s, K=%d, Q=%d",Arrays.toString(N),K,Q) + ", method=" + methodName + ","
                + "input-path=" + shortenUrl(inputDataPath.toString()) + ", "
                + "input-schema-path=" + shortenUrl(inputSchemaPath.toString()) + ", "
                + "output-path=" + shortenUrl(outputDataPath.toString()) + ", "
                + "output-schema-path=" + shortenUrl(outputDataPath.toString()) + ")";

        // setup map only job
        Job job = Job.getInstance(conf);
        job.setJarByClass(EncodeDatasetTool.class);
        job.setJobName(description);
        job.setNumReduceTasks(0);

        // setup input
        AvroKeyInputFormat.setInputPaths(job,inputDataPath);
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

    private static Schema loadAvroSchemaFromHdfs(final FileSystem fs,final Path schemaPath) throws IOException {
        FSDataInputStream fsdis = fs.open(schemaPath);
        Schema schema = (new Schema.Parser()).parse(fsdis);
        fsdis.close();
        return schema;
    }
}
