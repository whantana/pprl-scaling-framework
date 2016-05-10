package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Count Id pairs mapper.
 */
public class CountIdPairsMapper extends Mapper <Text,Text,Text,IntWritable> {

    public static IntWritable ONE = new IntWritable(1);
    public static String KEYS_DELIMITER = "_#_";

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text(key + KEYS_DELIMITER + value),ONE);
    }
}
