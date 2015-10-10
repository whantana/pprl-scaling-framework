package gr.upatras.ceid.pprl.datasets.mapreduce;

import gr.upatras.ceid.pprl.datasets.avro.dblp.DblpPublication;
import gr.upatras.ceid.pprl.datasets.input.MultiTagXmlInputFormat;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DblpXmlToAvroTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(DblpXmlToAvroTool.class);

    private static final String JOB_DESCRIPTION = "DBLP XML file to Avro";

    private static final String[] TAGS = new String[]{"article","phdthesis","mastersthesis"};

    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        if (args.length != 2) {
            LOG.error("Usage: DblpXmlToAvroTool <input-path> <output-path>");
            return -1;
        }

        // set tags
        final Configuration configuration = getConf();
        final Path input = new Path(args[0]);
        final Path output = new Path(args[1]);
		// TODO shorten input/output names 
        final String description = JOB_DESCRIPTION + "(input : " + input + ", output : " + output + ")";
        configuration.setStrings(MultiTagXmlInputFormat.TAGS_KEY,TAGS);

        try{
            // setup job
            Job job = Job.getInstance(configuration);
            job.setJarByClass(DblpXmlToAvroTool.class);
            job.setJobName(description);
            job.setInputFormatClass(MultiTagXmlInputFormat.class);
            job.setOutputFormatClass(AvroKeyOutputFormat.class);
            MultiTagXmlInputFormat.setInputPaths(job, input);
            AvroKeyOutputFormat.setOutputPath(job,output);
            job.setNumReduceTasks(0);
            job.setMapperClass(DblpXmlToAvroMapper.class);
            AvroJob.setMapOutputKeySchema(job, DblpPublication.getClassSchema());
            AvroJob.setOutputKeySchema(job, DblpPublication.getClassSchema());

            // run job
            return (job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage());
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new DblpXmlToAvroTool(), args);
        System.exit(res);
    }
}
