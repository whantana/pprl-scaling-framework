package gr.upatras.ceid.pprl.mapreduce;

import gr.upatras.ceid.pprl.avro.dblp.DblpPublication;
import gr.upatras.ceid.pprl.mapreduce.input.DblpXmlInputFormat;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DBLP XML to Avro Tool class.
 */
public class DblpToAvroTool  extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(DblpXmlToAvroTool.class);

    private static final String JOB_DESCRIPTION = "DBLP XML file to Avro";

    private static final String[] TAGS = new String[]{
            "article","inproceedings","proceedings",
            "book","incollection","www",
            "phdthesis","mastersthesis"};

    private static final String[] SECONDARY_TAGS = {"author","title","year"};

    /**
     * Run the tool.
     *
     * @param args input args
     * @return 0 for successful job execution, 1 for failed job execution.
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        // get args
        final Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 2) {
            LOG.error("Usage: DblpXmlToAvroTool <input-path> <output-path>");
            throw new IllegalArgumentException("Invalid number of arguments.");
        }

        // set confuguration and params
        final Path input = new Path(args[0]);
        final Path output = new Path(args[1]);
        conf.setStrings(DblpXmlInputFormat.TAGS_KEY, TAGS);
        conf.setStrings(DblpXmlInputFormat.SECONDARY_TAGS_KEY, SECONDARY_TAGS );

        // set description and log it
        final String description = String.format("%s(" +
                        "input-path : %s, output-path : %s)",
                JOB_DESCRIPTION,
                shortenUrl(input.toString()), shortenUrl(output.toString())
        );
        LOG.info("Running : " + description);

        // setup map only job
        Job job = Job.getInstance(conf);
        job.setJarByClass(DblpXmlToAvroTool.class);
        job.setJobName(description);
        job.setNumReduceTasks(0);

        // setup input
        DblpXmlInputFormat.setInputPaths(job, input);
        job.setInputFormatClass(DblpXmlInputFormat.class);

        // setup mapper
        job.setMapperClass(DblpToAvroMapper.class);
        AvroJob.setMapOutputKeySchema(job, DblpPublication.getClassSchema());
        job.setMapOutputValueClass(NullWritable.class);

        // setup output
        AvroKeyOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // run job
        boolean success = job.waitForCompletion(true);
        if(success) removeSuccessFile(FileSystem.get(conf),output);

        return (success ? 0 : 1);
    }

    /**
     * Main
     *
     * @param args input args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new DblpXmlToAvroTool(), args);
        System.exit(res);
    }

    /**
     * Shortens the given URL string.
     *
     * @param url URL string
     * @return shorten URL string.
     */
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

    /**
     * Remove _SUCCESS file from path.
     *
     * @param path a path.
     * @throws IOException
     */
    public static void removeSuccessFile(final FileSystem fs,
                                         final Path path) throws IOException {
        final Path p = new Path(path,"_SUCCESS");
        if (fs.exists(p)) fs.delete(p, false);
    }
}