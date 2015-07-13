package gr.upatras.ceid.pprl.datasets.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;


public class XmlToHBaseJob extends Configured implements Tool {

    private static final String JOB_DESCRIPTION = "XML dataset to HBase";

    public int run(String[] strings) throws Exception {
        return 0;
    }
}
