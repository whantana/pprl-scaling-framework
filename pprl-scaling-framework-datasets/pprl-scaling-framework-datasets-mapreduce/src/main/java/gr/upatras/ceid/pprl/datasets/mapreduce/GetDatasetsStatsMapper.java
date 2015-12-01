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
            int fieldLength = -1;
            int qGramsCount = -1;
            switch(type) {
                case BOOLEAN:
                    fieldLength = 1;
                    qGramsCount = QGramUtil.calcQgramsCount((Boolean)obj,Q);
                    break;
                case STRING:
                    fieldLength = obj.toString().length();
                    qGramsCount = QGramUtil.calcQgramsCount(obj.toString(),Q);
                    break;
                case INT:
                case LONG:
                case DOUBLE:
                case FLOAT:
                    fieldLength = String.valueOf(obj).length();
                    qGramsCount = QGramUtil.calcQgramsCount((Number) obj,Q);
                    break;
                default:
                    break;
            }
            if(fieldLength <= 0 || qGramsCount <= 0) continue;

            DatasetStatsWritable dsw = new DatasetStatsWritable();
            dsw.setFieldLength((double)fieldLength);
            dsw.setFieldQgramCount((double)qGramsCount);
            context.write(new Text(field.name()), dsw);
        }
    }
}
