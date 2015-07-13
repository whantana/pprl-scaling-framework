package gr.upatras.ceid.pprl.datasets.job;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class TextToAvroJob extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "Text dataset to Avro";

    public int run(String[] strings) throws Exception {
        return 0;
    }
}
