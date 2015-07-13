package gr.upatras.ceid.pprl.datasets.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/*
  TODO use custom input format for the dblp.
  Need to take account multiple start/ending tags (article,phdthesis and so)
  Also Need to ensure that no records are broken because of new lines
*/


public class XmlToAvroJob extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "XML dataset to Avro";

    public int run(String[] strings) throws Exception {

//        // get configuration
//        final Configuration configuration = getConf();
//        final Path inputPath = new Path(configuration.get(Names.IMPORT_DATA_HDFS_INPUT_PATH));
//        final Path outputPath = new Path(configuration.get(Names.IMPORT_DATA_HDFS_OUTPUT_PATH));
//        final boolean overwriteOutput = configuration.getBoolean(Names.IMPORT_DATA_OVERWRITE_OUTPUT, false);
//
//        // set start/end tags
//        //configuration.set(XmlInputFormat.START_TAG_KEY, "<article");
//        //configuration.set(XmlInputFormat.END_TAG_KEY, "</article>");
//
//        // get instance of job and configuration
//        final Job job = Job.getInstance(configuration, JOB_DESCRIPTION +
//                "(inputPath=" + inputPath +
//                ",outputPath=" + outputPath +
//                ",overwriteOutput=" + overwriteOutput);
//
//        // set main class in job's jar
//        job.setJarByClass(getClass());
//
//        // set job's input format classes
//        //job.setInputFormatClass(XmlInputFormat.class);
//        FileInputFormat.addInputPath(job, inputPath);
//
//        // set job's output format and path
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Text.class);
//        FileOutputFormat.setOutputPath(job, outputPath);
//        if (overwriteOutput) {
//            FileSystem fs = DistributedFileSystem.get(configuration);
//            if (fs.exists(outputPath)) fs.delete(outputPath, true);
//            fs.close();
//        } else {
//            FileSystem fs = DistributedFileSystem.get(configuration);
//            if (fs.exists(outputPath)) {
//                fs.close();
//                throw new Exception("outputPath=" + outputPath + " already exists and cannot overwrite it");
//            }
//        }
//
//        // TODO set the mapper and reducer class
//
//        // wait for job completion
//        final boolean success = job.waitForCompletion(true);
//
//        // return 0 for success and 1 for failure
//        return success ? 0 : 1;

        return 0;
    }
}
