package gr.upatras.ceid.pprl.datasets;

import gr.upatras.ceid.pprl.datasets.jobs.XmlToAvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

/**
 * Import Datasets
 */
public class ImportDatasetMain {

    /**
     * Main method.
     *
     * @param args arguments
     */
    public static void main(final String[] args) throws Exception {

        // load configuration
        GenericOptionsParser gop = new GenericOptionsParser(args);
        Configuration configuration = gop.getConfiguration();


        // TODO check configuration here
        // TODO after check determine which job to run

        // for now just run xml to avro
        ToolRunner.run(configuration, new XmlToAvroJob(), args);
    }
}
