package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.EncodingAvroSchemaException;
import gr.upatras.ceid.pprl.encoding.FieldBFEncoding;
import gr.upatras.ceid.pprl.encoding.RowBFEncoding;
import gr.upatras.ceid.pprl.encoding.SimpleBFEncoding;
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
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static gr.upatras.ceid.pprl.encoding.EncodingAvroSchemaUtil.loadAvroSchemaFromHdfs;
import static gr.upatras.ceid.pprl.encoding.mapreduce.EncodeDatasetMapper.*;

public class EncodeDatasetTool extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "Encode Dataset";

    private static final String[] AVAILABLE_METHODS = {"SFB","FBF","RBF"};

    private static final Logger LOG = LoggerFactory.getLogger(EncodeDatasetTool.class);


    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException,
            EncodingAvroSchemaException {

        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 9) {
            LOG.error("Usage: EncodeDatasetTool <input-path> <input-schema> <encoding-path> <encoding-schema>" +
                    "\n\t <col0,col1,...,colN> <SBF|FBF|RBF> <N> <Q> <K>");
            return -1;
        }

        // set confuguration and params
        final Path input = new Path(args[0]);
        final Schema inputSchema = loadAvroSchemaFromHdfs(new Path(args[1]), conf);
        final Path encodingOutput = new Path(args[2]);
        final Schema encodingSchema = loadAvroSchemaFromHdfs(new Path(args[3]), conf);
        final String[] columns = args[4].split(",");
        final String encodingMethod = args[5];
        if(!Arrays.asList(AVAILABLE_METHODS).contains(encodingMethod))
            throw new IllegalArgumentException("Availble methods are : " + Arrays.toString(AVAILABLE_METHODS));
        final int N = Integer.parseInt(args[6]);
        final int K = Integer.parseInt(args[7]);
        final int Q = Integer.parseInt(args[8]);

        conf.set(INPUT_SCHEMA_KEY, inputSchema.toString());
        conf.set(OUTPUT_SCHEMA_KEY, encodingSchema.toString());
        conf.setStrings(ENCODING_COLUMNS_KEY, columns);
        conf.set(ENCODING_METHOD_KEY,encodingMethod);
        conf.setInt(N_KEY, N);
        conf.setInt(K_KEY,K);
        conf.setInt(Q_KEY,Q);

        // validate schema encoding
        boolean valid = false;
        int selectedMethod = Arrays.asList(AVAILABLE_METHODS).indexOf(encodingMethod);
        switch(selectedMethod) {
            case 0 :
                valid = (new SimpleBFEncoding(inputSchema,encodingSchema,columns,N,K,Q)).validateEncodingSchema();
                break;
            case 1 :
                valid = (new FieldBFEncoding(inputSchema,encodingSchema,columns,N,K,Q)).validateEncodingSchema();
                break;
            case 2 :
                valid = (new RowBFEncoding(inputSchema,encodingSchema,columns,N,K,Q)).validateEncodingSchema();
                break;
        }
        if(!valid) throw new EncodingAvroSchemaException("Encoding schema is not appropriate for input schema (.");

        // set description
        final String description = JOB_DESCRIPTION + " (input-path=" + shortenUrl(input.toString()) + ", columns=" +
                Arrays.toString(columns) + ", encoding-method=" + encodingMethod + "(N="+ N + ",K=" + K + ",Q=" + Q +
                "), output-path=" + shortenUrl(encodingOutput.toString()) + ")";

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
        AvroJob.setMapOutputKeySchema(job, encodingSchema);

        // setup output
        AvroKeyOutputFormat.setOutputPath(job, encodingOutput);
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
}
