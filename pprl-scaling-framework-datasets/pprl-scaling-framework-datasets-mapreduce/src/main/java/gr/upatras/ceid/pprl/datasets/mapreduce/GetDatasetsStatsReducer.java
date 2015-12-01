package gr.upatras.ceid.pprl.datasets.mapreduce;

import gr.upatras.ceid.pprl.datasets.DatasetStatsWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GetDatasetsStatsReducer extends Reducer<Text,DatasetStatsWritable,Text,DatasetStatsWritable> {
    @Override
    protected void reduce(Text key, Iterable<DatasetStatsWritable> values, Context context) throws IOException, InterruptedException {
        long size = 0;
        double sumLen = 0;
        double sumQgramCount = 0;
        for(DatasetStatsWritable sw : values) {
            sumLen += sw.getFieldLength();
            sumQgramCount += sw.getFieldQgramCount();
            size++;
        }
        double avgLen = (sumLen / (double) size);
        double avgeQgramCount = (sumQgramCount / (double) size);
        DatasetStatsWritable dsw = new DatasetStatsWritable();
        dsw.setFieldLength(avgLen);
        dsw.setFieldQgramCount(avgeQgramCount);
        context.write(key,dsw);
    }
}
