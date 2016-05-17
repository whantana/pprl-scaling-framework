package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hamming LSH-FPS Blocking tool class
 */
public class HammingLSHFPSBlockingTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHFPSBlockingTool.class);

    public int run(String[] args) throws Exception {
        return 0;
    }

    /**
      * Main
      *
      * @param args input args
      * @throws Exception
      */
     public static void main(String[] args) throws Exception {
         int res = ToolRunner.run(new HammingLSHBlockingTool(), args);
         System.exit(res);
     }

     /**
      * Shortens the given URL string.
      *
      * @param url URL string
      * @return shorten URL string.
      */
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
