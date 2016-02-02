package gr.upatras.ceid.pprl.datasets.mapreduce;

import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GetDatasetsStatsReducer extends Reducer<Text,DatasetStatistics,Text,DatasetStatistics> {
    @Override
    protected void reduce(Text key, Iterable<DatasetStatistics> values, Context context) throws IOException, InterruptedException {
        long size = 0;
        double sumLen = 0;
        double[] sumQgramCount = new double[DatasetStatistics.Q_GRAMS.length];
        for(DatasetStatistics sw : values) {
            sumLen += sw.getFieldLength();
            for (int i = 0; i < DatasetStatistics.Q_GRAMS.length ; i++) {
                sumQgramCount[i] += sw.getFieldQgramCount()[i];
            }
            size++;
        }
        double avgLen = (sumLen / (double) size);
        double[] avgQgramCount = new double[sumQgramCount.length];
        for (int i = 0; i < sumQgramCount.length; i++)
            avgQgramCount[i] = (sumQgramCount[i] / (double) size);

        DatasetStatistics dsw = new DatasetStatistics();
        dsw.setFieldLength(avgLen);
        dsw.setFieldQgramCount(avgQgramCount);
        context.write(key,dsw);
    }
}
