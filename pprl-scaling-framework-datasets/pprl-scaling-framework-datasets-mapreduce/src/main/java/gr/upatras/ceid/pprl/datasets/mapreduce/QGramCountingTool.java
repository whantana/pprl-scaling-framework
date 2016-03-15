package gr.upatras.ceid.pprl.datasets.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QGramCountingTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(DblpXmlToAvroTool.class);

    private static final String JOB_DESCRIPTION = "Count QGrams of AVRO Records";

    private static final FsPermission ONLY_OWNER_PERMISSION
            = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE, false);

    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 4) {
            LOG.error("Usage: QGramCountingTool <input-path> <input-schema-path> <output-path> <comma-separated-field-names>");
            throw new IllegalArgumentException("Invalid number of arguments (" + args.length + ").");
        }

        final Path input = new Path(args[0]);
        final Path inputSchemaPath = new Path(args[1]);
        final Path outputPath = new Path(args[2]);
        final Schema inputSchema = loadAvroSchemaFromHdfs(FileSystem.get(conf), inputSchemaPath);
        conf.set(QGramCountingMapper.SCHEMA_KEY,inputSchema.toString());
        final String[] fieldNames = args[3].contains(",") ? args[3].split(",") : new String[]{args[3]};
        conf.setStrings(QGramCountingMapper.FIELD_NAMES_KEY,fieldNames);

        // set description and log it
        final String description = JOB_DESCRIPTION + "(input : " + shortenUrl(input.toString()) +
                ", field-names : " + Arrays.toString(fieldNames) + ")";
        LOG.info("Running :" + description);

        // setup map only job
        Job job = Job.getInstance(conf);
        job.setJarByClass(QGramCountingTool.class);
        job.setJobName(description);
        job.setNumReduceTasks(0);

        // setup input
        AvroKeyInputFormat.setInputPaths(job, input);
        AvroJob.setInputKeySchema(job, inputSchema);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // setup mapper
        job.setMapperClass(QGramCountingMapper.class);

        // run job
        boolean success  = job.waitForCompletion(true);
        if(success) {
            counters2HdfsFile(FileSystem.get(conf), outputPath, job.getCounters());
            return 0;
        } else throw new IllegalStateException("Job not successfull.");
    }

    public static void counters2HdfsFile(final FileSystem fs,final Path outputPath, final Counters counters) throws IOException {
        Properties properties = new Properties();
        long recordCount = counters.findCounter("", QGramCountingMapper.RECORD_COUNT_KEY).getValue();
        properties.setProperty("record.count",String.valueOf((double)recordCount));
        for (String counterGroupName : counters.getGroupNames()) {
            if(counterGroupName.equals("")) continue;
            for (String counterName : QGramCountingMapper.STATISTICS) {
                long val = counters.findCounter(counterGroupName, counterName).getValue();
                final String key = counterGroupName + ".avg." + counterName;
                double avg = (double) val / (double) recordCount;
                LOG.info("Key = {} , value = {}", key, avg);
                properties.setProperty(key,String.valueOf(avg));
            }
        }

        final FSDataOutputStream fsdos = fs.create(outputPath, true);
        properties.store(fsdos,"");
    }

    private static Schema loadAvroSchemaFromHdfs(final FileSystem fs,final Path schemaPath)
            throws IOException {
        FSDataInputStream fsdis = fs.open(schemaPath);
        Schema schema = (new Schema.Parser()).parse(fsdis);
        fsdis.close();
        return schema;
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
