package gr.upatras.ceid.pprl.datasets.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class GetStatsMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, StatsWritable>{
    public static String Q_KEY = "pprl.datasets.q";
    public static String INPUT_SCHEMA_KEY = "pprl.datasets.schema";

    private int Q;
    private Schema schema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Q = context.getConfiguration().getInt(Q_KEY, 1);
        schema = (new Schema.Parser()).parse(context.getConfiguration().get(INPUT_SCHEMA_KEY));
    }

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        final GenericRecord record = key.datum();

        for (Schema.Field field : schema.getFields()) {
            record.get(field.name());
            Object obj = record.get(field.name());
            Schema.Type type = field.schema().getType();
            int fieldLength = 0;
            int qGramsCount = 0;
            switch(type) {
                case BOOLEAN:
                    fieldLength = 1;
                    qGramsCount = (2+fieldLength) - Q + 1;
                    break;
                case STRING:
                    String string = obj.toString();
                    String onlyAlnum = string.replaceAll("[^\\pL\\pN\\s]+", " ");
                    String onlyCapitals = onlyAlnum.toUpperCase();
                    String replaceSpaces = onlyCapitals.replaceAll("\\s+","_");
                    replaceSpaces = (replaceSpaces.startsWith("_")) ?
                            replaceSpaces : "_" + replaceSpaces;
                    replaceSpaces = (replaceSpaces.endsWith("_")) ?
                            replaceSpaces : replaceSpaces + "_";
                    String finalString = replaceSpaces;
                    fieldLength = string.length();
                    qGramsCount = finalString.length() - Q + 1;
                    break;
                case INT:
                case DOUBLE:
                case FLOAT:
                    String numString = String.valueOf(obj);
                    String finalNumString = numString.replaceAll("[^\\pN\\s]+", "_");
                    fieldLength = numString.length();
                    qGramsCount =  finalNumString.length() - Q + 1;
                    break;
                default:
                break;
            }
            if(fieldLength == 0 && qGramsCount == 0) break;
            StatsWritable sw = new StatsWritable();
            sw.setFieldLength(fieldLength);
            sw.setFieldQgramCount(qGramsCount);
            context.write(new Text(field.name()),sw);
        }
    }
}
