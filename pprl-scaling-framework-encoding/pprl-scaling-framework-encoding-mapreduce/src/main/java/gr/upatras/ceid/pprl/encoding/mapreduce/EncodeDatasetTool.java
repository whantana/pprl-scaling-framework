package gr.upatras.ceid.pprl.encoding.mapreduce;

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

import static gr.upatras.ceid.pprl.encoding.mapreduce.BaseBloomFilterEncodingMapper.*;

public class EncodeDatasetTool extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "Encode Dataset";

    private static final String[] AVAILABLE_METHODS = {"SIMPLE","MULTI","ROW"};

    private static final Logger LOG = LoggerFactory.getLogger(EncodeDatasetTool.class);


    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException,
            BloomFilterEncodingException {

        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 10) {
            LOG.error(" Usage: EncodeDatasetTool <input-path> <input-schema> <encoding-path> <encoding-schema>" +
                    "\n\t <col0,col1,...,colN> <uid_col> <SIMPLE|MULTI|ROW> <N> <K> <Q>");
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
        final String[] encodingColumns = args[4].split(",");
        final String uidColumn = args[5];
        final String encodingMethod = args[6];
        if(!Arrays.asList(AVAILABLE_METHODS).contains(encodingMethod))
            throw new IllegalArgumentException("Error : " + encodingMethod + " Availble methods are : " + Arrays.toString(AVAILABLE_METHODS));
        final int selectedMethod = Arrays.asList(AVAILABLE_METHODS).indexOf(encodingMethod);
        final int N = Integer.parseInt(args[7]);
        final int K = Integer.parseInt(args[8]);
        final int Q = Integer.parseInt(args[9]);

        conf.set(INPUT_SCHEMA_KEY, inputSchema.toString());
        conf.set(OUTPUT_SCHEMA_KEY, outputSchema.toString());
        conf.set(UID_COLUMN_KEY, uidColumn);
        conf.setStrings(SELECTED_COLUMNS_KEY, encodingColumns);
        conf.setInt(N_KEY, N);
        conf.setInt(K_KEY,K);
        conf.setInt(Q_KEY,Q);

        // validate schema encoding
        String description = JOB_DESCRIPTION + " ("
                + String.format(" N=%d, K=%d, Q=%d",N,K,Q) + ", method=" + encodingMethod + ","
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
        switch(selectedMethod) {
            case 0 :
                job.setMapperClass(SimpleBloomFilterEncodingMapper.class);
                break;
            case 1 :
                job.setMapperClass(MultiBloomFilterEncodingMapper.class);
                break;
            case 2 :
                job.setMapperClass(RowBloomFilterEncodingMapper.class);
                break;
        }

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

    public static Schema loadAvroSchemaFromHdfs(final FileSystem fs,final Path schemaPath) throws IOException {
        FSDataInputStream fsdis = fs.open(schemaPath);
        Schema schema = (new Schema.Parser()).parse(fsdis);
        fsdis.close();
        return schema;
    }
}
