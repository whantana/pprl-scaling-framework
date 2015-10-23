package gr.upatras.ceid.pprl.datasets.mapreduce;

import gr.upatras.ceid.pprl.datasets.avro.dblp.DblpPublication;
import gr.upatras.ceid.pprl.datasets.input.MultiTagXmlInputFormat;
import org.apache.avro.mapreduce.AvroJob;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DblpXmlToAvroTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(DblpXmlToAvroTool.class);

    private static final String JOB_DESCRIPTION = "DBLP XML file to Avro";

    private static final String[] TAGS = new String[]{"article","phdthesis","mastersthesis"};

    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 2) {
            LOG.error("Usage: DblpXmlToAvroTool <input-path> <output-path>");
            return -1;
        }

        // set confuguration and params
        final Path input = new Path(args[0]);
        final Path output = new Path(args[1]);
        conf.setStrings(MultiTagXmlInputFormat.TAGS_KEY, TAGS);

        // set description
        final String description = JOB_DESCRIPTION + "(input : " + shortenUrl(input.toString()) +
                ", output : " + shortenUrl(output.toString()) + ")";

        // setup map only job
        Job job = Job.getInstance(conf);
        job.setJarByClass(DblpXmlToAvroTool.class);
        job.setJobName(description);
        job.setNumReduceTasks(0);

        // setup input
        MultiTagXmlInputFormat.setInputPaths(job, input);
        job.setInputFormatClass(MultiTagXmlInputFormat.class);

        // setup mapper
        job.setMapperClass(DblpXmlToAvroMapper.class);
        AvroJob.setMapOutputKeySchema(job, DblpPublication.getClassSchema());

        // setup output
        AvroKeyOutputFormat.setOutputPath(job,output);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // run job
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new DblpXmlToAvroTool(), args);
        System.exit(res);
    }

    public static String shortenUrl(final String url) {
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
