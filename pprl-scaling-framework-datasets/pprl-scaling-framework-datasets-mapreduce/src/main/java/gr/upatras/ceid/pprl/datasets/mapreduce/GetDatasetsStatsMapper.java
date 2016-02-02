package gr.upatras.ceid.pprl.datasets.mapreduce;

import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GetDatasetsStatsMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, DatasetStatistics>{
    public static String INPUT_SCHEMA_KEY = "pprl.datasets.schema";
    private Schema schema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        schema = (new Schema.Parser()).parse(context.getConfiguration().get(INPUT_SCHEMA_KEY));
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        for (Schema.Field field : schema.getFields()) {
            record.get(field.name());
            Object obj = record.get(field.name());
            Schema.Type type = field.schema().getType();

            DatasetStatistics dsw = new DatasetStatistics();
            dsw.setFieldLength((double)String.valueOf(obj).length());
            dsw.setFieldQgramCount(new double[DatasetStatistics.Q_GRAMS.length]);
            for (int q : DatasetStatistics.Q_GRAMS)
                dsw.setFieldQGramCount(q,(double) QGramUtil.calcQgramsCount(obj,type,q));
            context.write(new Text(field.name()),dsw);
        }
    }
}
