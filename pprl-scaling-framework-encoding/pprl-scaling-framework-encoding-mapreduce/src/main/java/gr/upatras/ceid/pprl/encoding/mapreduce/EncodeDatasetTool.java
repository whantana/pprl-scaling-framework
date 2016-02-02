package gr.upatras.ceid.pprl.encoding.mapreduce;

import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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

public class EncodeDatasetTool extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "Encode Dataset";

    private static final Logger LOG = LoggerFactory.getLogger(EncodeDatasetTool.class);

    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException,
            BloomFilterEncodingException {

        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length > 4 || args.length < 8) {
            LOG.error("Usage without encoding creation: EncodeDatasetTool " +
                    "<input-path> <input-schema> <encoding-path> <encoding-schema>\n");
            LOG.error("Usage with encoding creation: EncodeDatasetTool " +
                    "<input-path> <input-schema> <encoding-path> <encoding-schema>\n" +
                    "\t<sel_field_1,sel_field_2,...,sel_field_M>\n" +
                    "\t[<rest_field_1,rest_field_2,...,rest_field_R>]\n" +
                    "\t<FBF|RBF> [<N>] <K> <Q>");
            return -1;
        }
        boolean createEncoding = args.length >= 8;
        final Path inputDataPath = new Path(args[0]);
        final Path inputSchemaPath = new Path(args[1]);
        final Path outputDataPath = new Path(args[2]);
        final Path outputSchemaPath = new Path(args[3]);
        final Schema inputSchema = loadAvroSchemaFromHdfs(FileSystem.get(conf), inputSchemaPath);
        conf.set(BloomFilterEncodingMapper.INPUT_SCHEMA_KEY,inputSchema.toString());
        final Schema outputSchema;
        if(createEncoding) {
            outputSchema = createBloomFilterEncoding(args, inputSchema).getEncodingSchema();
            LOG.info("Created Encoding Schema :" + outputSchema.toString(true));
            saveAvroSchemaToHdfs(outputSchema,FileSystem.get(conf),outputSchemaPath);
        } else outputSchema = loadAvroSchemaFromHdfs(FileSystem.get(conf), outputSchemaPath);
        conf.set(BloomFilterEncodingMapper.OUTPUT_SCHEMA_KEY,outputSchema.toString());

        // set description
        String description = JOB_DESCRIPTION + (createEncoding ? "Create Encoding" :" (" ) +
                "input-path=" + shortenUrl(inputDataPath.toString()) + ", " +
                "input-schema-path=" + shortenUrl(inputSchemaPath.toString()) + ", " +
                "output-path=" + shortenUrl(outputDataPath.toString()) + ", " +
                "output-schema-path=" + shortenUrl(outputDataPath.toString()) + ")";

        // setup map only job
        final Job job = Job.getInstance(conf);
        job.setJarByClass(EncodeDatasetTool.class);
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


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new EncodeDatasetTool(), args);
        System.exit(res);
    }

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

    private static BloomFilterEncoding createBloomFilterEncoding(final String[] args, final Schema inputSchema) throws
            BloomFilterEncodingException {
        final String methodName;
        final String[] restFieldNames;
        final int N;
        final int K;
        final int Q;
        final String[] selectedFieldNames = args[4].split(",");
        switch (args.length) {
            case 8:
                methodName = args[5];
                restFieldNames = new String[0];
                N = -1;
                K = Integer.valueOf(args[6]);
                Q = Integer.valueOf(args[7]);
                break;
            case 9:
                if (args[5].equals("FBF") || args[5].equals("RBF")) {
                    methodName = args[5];
                    restFieldNames = new String[0];
                    N = Integer.valueOf(args[6]);
                } else {
                    restFieldNames = args[5].contains(",") ?
                            args[5].split(",") : new String[]{args[5]};
                    methodName = args[6];
                    N = -1;
                }
                K = Integer.valueOf(args[7]);
                Q = Integer.valueOf(args[8]);
                break;
            case 10:
                restFieldNames = args[5].contains(",") ?
                        args[5].split(",") : new String[]{args[5]};
                methodName = args[6];
                N = Integer.valueOf(args[7]);
                K = Integer.valueOf(args[8]);
                Q = Integer.valueOf(args[9]);
                break;
            default:
                LOG.error("Error. Shouldnt be here.");
                throw new IllegalArgumentException("Illegal args");
        }
        if (!BloomFilterEncoding.AVAILABLE_METHODS.contains(methodName))
            throw new IllegalArgumentException("Error : " + methodName +
                    " Availble methods are : " + BloomFilterEncoding.AVAILABLE_METHODS);
        final BloomFilterEncoding encoding;
        if (methodName.equals("FBF") && N > 0) {
            encoding = new FieldBloomFilterEncoding(N, selectedFieldNames.length, K, Q);
        } else if (methodName.equals("FBF") && N < 0) {
            double[] avgQCount = new double[selectedFieldNames.length]; // need stats here
            encoding = new FieldBloomFilterEncoding(avgQCount, K, Q);
        } else if (methodName.equals("RBF") && N > 0) {
            double[] avgQCount = new double[selectedFieldNames.length]; // need stats here
            encoding = new RowBloomFilterEncoding(avgQCount, N, K, Q);
        } else {
            double[] avgQCount = new double[selectedFieldNames.length]; // need stats here
            double[] weights = new double[selectedFieldNames.length]; // need stats here
            encoding = new RowBloomFilterEncoding(avgQCount, weights, K, Q);
        }

        encoding.makeFromSchema(inputSchema, selectedFieldNames, restFieldNames);
        if (!encoding.isEncodingOfSchema(inputSchema))
            throw new BloomFilterEncodingException("Encoding schema is not derived from input schema.");
        return encoding;
    }


    private static Schema loadAvroSchemaFromHdfs(final FileSystem fs,final Path schemaPath)
            throws IOException {
        FSDataInputStream fsdis = fs.open(schemaPath);
        Schema schema = (new Schema.Parser()).parse(fsdis);
        fsdis.close();
        return schema;
    }

    private static void saveAvroSchemaToHdfs(final Schema schema,final FileSystem fs,final Path schemaPath)
            throws IOException {
        final FSDataOutputStream fsdos = fs.create(schemaPath, true);
        fsdos.write(schema.toString(true).getBytes());
        fsdos.close();
    }
}
