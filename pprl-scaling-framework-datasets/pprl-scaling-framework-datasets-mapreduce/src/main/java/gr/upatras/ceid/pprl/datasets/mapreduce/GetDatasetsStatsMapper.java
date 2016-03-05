package gr.upatras.ceid.pprl.datasets.mapreduce;

public class GetDatasetsStatsMapper { //extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, DatasetFieldStatistics>{
//    // TODO probably remove this code for Spark code.
//
//    public static String INPUT_SCHEMA_KEY = "pprl.datasets.schema";
//    private Schema schema;
//
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);
//        schema = (new Schema.Parser()).parse(context.getConfiguration().get(INPUT_SCHEMA_KEY));
//    }
//
//    @Override
//    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
//        final GenericRecord record = key.datum();
//        for (Schema.Field field : schema.getFields()) {
//            record.get(field.name());
//            Object obj = record.get(field.name());
//            Schema.Type type = field.schema().getType();
//
//            DatasetFieldStatistics dsw = new DatasetFieldStatistics();
//            dsw.setLength((double) String.valueOf(obj).length());
//            dsw.setQgramCount(new double[DatasetFieldStatistics.Q_GRAMS.length]);
//            for (int q : DatasetFieldStatistics.Q_GRAMS)
//                dsw.setQgramCount(q, (double) QGramUtil.calcQgramsCount(obj, type, q));
//            context.write(new Text(field.name()),dsw);
//        }
//    }
}
