package gr.upatras.ceid.pprl.datasets.jobs;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class TextToHBaseJob extends Configured implements Tool {

    public final static String JOB_DESCRIPTION = "Text to HBase";

    public int run(String[] strings) throws Exception {
        return 0;
    }
}
