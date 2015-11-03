package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BFEncodingException;
import gr.upatras.ceid.pprl.encoding.BaseBFEncoding;
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
import static gr.upatras.ceid.pprl.encoding.mapreduce.BaseBFEncodingMapper.*;

public class EncodeDatasetTool extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "Encode Dataset";

    private static final String[] AVAILABLE_METHODS = {"SBF","FBF","RBF"};

    private static final Logger LOG = LoggerFactory.getLogger(EncodeDatasetTool.class);


    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException,
            BFEncodingException {

        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 10) {
            LOG.error(" Usage: EncodeDatasetTool <input-path> <input-schema> <encoding-path> <encoding-schema>" +
                    "\n\t <col0,col1,...,colN> <uid_col> <SBF|FBF|RBF> <N> <K> <Q>");
            return -1;
        }

        // set confuguration and params
        final Path input = new Path(args[0]);
        final Schema inputSchema = loadAvroSchemaFromHdfs(new Path(args[1]), conf);
        final Path encodingOutput = new Path(args[2]);
        final Schema encodingSchema = loadAvroSchemaFromHdfs(new Path(args[3]), conf);
        final String[] columns = args[4].split(",");
        final String uidColumn = args[5];
        final String encodingMethod = args[6];
        if(!Arrays.asList(AVAILABLE_METHODS).contains(encodingMethod))
            throw new IllegalArgumentException("Error : " + encodingMethod + " Availble methods are : " + Arrays.toString(AVAILABLE_METHODS));
        final int selectedMethod = Arrays.asList(AVAILABLE_METHODS).indexOf(encodingMethod);
        final int N = Integer.parseInt(args[7]);
        final int K = Integer.parseInt(args[8]);
        final int Q = Integer.parseInt(args[9]);

        conf.set(INPUT_SCHEMA_KEY, inputSchema.toString());
        conf.set(INPUT_UID_COLUMN_KEY, uidColumn);
        conf.set(ENCODING_SCHEMA_KEY, encodingSchema.toString());
        conf.setStrings(ENCODING_COLUMNS_KEY, columns);
        conf.setInt(N_KEY, N);
        conf.setInt(K_KEY,K);
        conf.setInt(Q_KEY,Q);

        // validate schema encoding
        String description = JOB_DESCRIPTION + " (input-path=" + shortenUrl(input.toString()) +
                ",  output-path=" + shortenUrl(encodingOutput.toString())  +")";
        BaseBFEncoding encoding;
        switch(selectedMethod) {
            case 0 :
                encoding = new SimpleBFEncoding(inputSchema,encodingSchema,uidColumn,Arrays.asList(columns),N,K,Q);
                description += " " + encoding.toString();
                break;
            case 1 :
                encoding = new FieldBFEncoding(inputSchema,encodingSchema,uidColumn,Arrays.asList(columns),N,K,Q);
                description += " " + encoding.toString();
                break;
            case 2 :
                encoding = new RowBFEncoding(inputSchema,encodingSchema,uidColumn,Arrays.asList(columns),N,K,Q);
                description += " " + encoding.toString();
                break;
            default:
                throw new BFEncodingException("Invalid method selection : \"" + selectedMethod +"\".");
        }
        boolean valid = encoding.validateEncodingSchema();
        if(!valid) throw new BFEncodingException("Encoding schema is not appropriate for input schema.");

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
        switch(selectedMethod) {
            case 0 :
                job.setMapperClass(SimpleBFEncodingMapper.class);
                break;
            case 1 :
                job.setMapperClass(FieldBFEncodingMapper.class);
                break;
            case 2 :
                job.setMapperClass(RowBFEncodingMapper.class);
                break;
            default:
                throw new BFEncodingException("Invalid method selection : \"" + selectedMethod +"\".");
        }

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