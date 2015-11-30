package gr.upatras.ceid.pprl.datasets.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GetStatsReducer  extends Reducer<Text,StatsWritable,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
        int size = 0;
        int sumLen = 0;
        int sumQgramCount = 0;
        for(StatsWritable sw : values) {
            sumLen += sw.getFieldLength();
            sumQgramCount += sw.getFieldQgramCount();
            size++;
        }
        double avgLen = (sumLen / (double) size);
        double avgeQgramCount = (sumQgramCount / (double) size);
        context.write(key,new Text(String.format("%.2f,%.2f",avgLen,avgeQgramCount)));
    }
}
