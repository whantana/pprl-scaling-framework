package gr.upatras.ceid.pprl.datasets.mapreduce;

import gr.upatras.ceid.pprl.datasets.DatasetStatsWritable;
import gr.upatras.ceid.pprl.datasets.QGramUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GetDatasetsStatsMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, DatasetStatsWritable>{
    public static String Q_KEY = "pprl.datasets.q";
    public static String INPUT_SCHEMA_KEY = "pprl.datasets.schema";

    private int Q;
    private Schema schema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Q = context.getConfiguration().getInt(Q_KEY, 2);
        schema = (new Schema.Parser()).parse(context.getConfiguration().get(INPUT_SCHEMA_KEY));
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();
        for (Schema.Field field : schema.getFields()) {
            record.get(field.name());
            Object obj = record.get(field.name());
            Schema.Type type = field.schema().getType();
            DatasetStatsWritable dsw = new DatasetStatsWritable();
            dsw.setFieldLength((double)String.valueOf(obj).length());
            dsw.setFieldQgramCount((double)QGramUtil.calcQgramsCount(obj,type,Q));
            context.write(new Text(field.name()), dsw);
        }
    }
}
