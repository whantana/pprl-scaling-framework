package gr.upatras.ceid.pprl.matching.test;

import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
import gr.upatras.ceid.pprl.matching.mapreduce.GeneratePairsMapper;
import gr.upatras.ceid.pprl.matching.mapreduce.PairSimilarityCombiner;
import gr.upatras.ceid.pprl.matching.mapreduce.PairSimilarityReducer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExhaustivePairSimilarityMRTest {

    private MapDriver<AvroKey<GenericRecord>, NullWritable, LongWritable, AvroKey<GenericRecord>> mapDriver;
    private MapReduceDriver<
            AvroKey<GenericRecord>, NullWritable,
            LongWritable, AvroKey<GenericRecord>,
            NullWritable,NullWritable> mapReduceDriver;

    private static final Logger LOG = LoggerFactory.getLogger(ExhaustivePairSimilarityMRTest.class);


    private Schema schema;
    private GenericRecord[] records = new GenericRecord[10];
    private final String[] fieldNames = {"name","surname","location"};

    @Before
    public void setup() throws IOException {
        schema = loadAvroSchemaFromFile(new File("person_small/schema/person_small.avsc"));
        for (int i = 0; i < 10; i++) {
            records[i] = new GenericData.Record(schema);
            records[i].put("id",String.valueOf(i));
            records[i].put("name",String.format("Name #%d",i));
            records[i].put("surname",String.format("Surname #%d",i));
            records[i].put("location",String.format("Location #%d",i));
        }
        LOG.info(10 + " records ready.");


        mapDriver = MapDriver.newMapDriver(new GeneratePairsMapper());
        mapDriver.getContext().getConfiguration().setInt("record.count", 10);
        mapDriver.getContext().getConfiguration().set("uid.field.name", "id");
        AvroSerialization.setKeyWriterSchema(mapDriver.getConfiguration(), schema);
        AvroSerialization.setKeyReaderSchema(mapDriver.getConfiguration(), schema);
        mapDriver.setOutputSerializationConfiguration(mapDriver.getConfiguration());
        AvroSerialization.addToConfiguration(mapDriver.getOutputSerializationConfiguration());
        AvroSerialization.setValueWriterSchema(mapDriver.getOutputSerializationConfiguration(), schema);
        AvroSerialization.setValueReaderSchema(mapDriver.getOutputSerializationConfiguration(), schema);
        LOG.info("MapDriver ready.");

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(
                new GeneratePairsMapper(),
                new PairSimilarityReducer(),
                new PairSimilarityCombiner()
        );
        mapReduceDriver.getConfiguration().setInt("record.count", 10);
        mapReduceDriver.getConfiguration().set("uid.field.name", "id");
        mapReduceDriver.getConfiguration().setStrings("field.names",fieldNames);
        AvroSerialization.setKeyWriterSchema(mapReduceDriver.getConfiguration(), schema);
        AvroSerialization.setKeyReaderSchema(mapReduceDriver.getConfiguration(), schema);
        mapReduceDriver.setOutputSerializationConfiguration(mapReduceDriver.getConfiguration());
        AvroSerialization.addToConfiguration(mapReduceDriver.getOutputSerializationConfiguration());
        AvroSerialization.setValueWriterSchema(mapReduceDriver.getOutputSerializationConfiguration(), schema);
        AvroSerialization.setValueReaderSchema(mapReduceDriver.getOutputSerializationConfiguration(), schema);
        LOG.info("MapReduceDriver ready.");
    }

    @Test
    public void test0() throws IOException {
        mapDriver.withInput(new AvroKey<GenericRecord>(records[0]), NullWritable.get());
        List<Pair<LongWritable, AvroKey<GenericRecord>>> expectedOutputs =
                new ArrayList<Pair<LongWritable,  AvroKey<GenericRecord>>>();
        long[] ranksWith0 = CombinatoricsUtil.ranksContaining(0,10);
        LOG.info("Ranks of element 0 in 10 : {}", Arrays.toString(ranksWith0));
        for (int i = 0; i <ranksWith0.length; i++) {
            expectedOutputs.add(new Pair<LongWritable, AvroKey<GenericRecord>>(
                    new LongWritable(ranksWith0[i]),
                    new AvroKey<GenericRecord>(records[0])));
        }

        mapDriver.withAllOutput(expectedOutputs);
        mapDriver.runTest();
    }


    @Test
    public void test1() throws IOException {

        List<Pair<AvroKey<GenericRecord>,NullWritable>> inputs
                = new ArrayList<Pair<AvroKey<GenericRecord>, NullWritable>>();
        inputs.add(new Pair<AvroKey<GenericRecord>, NullWritable>(
                new AvroKey<GenericRecord>(records[0]), NullWritable.get()));
        inputs.add(new Pair<AvroKey<GenericRecord>, NullWritable>(
                new AvroKey<GenericRecord>(records[1]), NullWritable.get()));

        mapDriver.withAll(inputs);
        List<Pair<LongWritable, AvroKey<GenericRecord>>> expectedOutputs =
                new ArrayList<Pair<LongWritable,  AvroKey<GenericRecord>>>();
        long[] ranksWith0 = CombinatoricsUtil.ranksContaining(0,10);
        long[] ranksWith1 = CombinatoricsUtil.ranksContaining(1,10);
        LOG.info("Ranks of element 0 in 10 : {}", Arrays.toString(ranksWith0));
        LOG.info("Ranks of element 1 in 10 : {}", Arrays.toString(ranksWith1));
        assert ranksWith0.length == ranksWith1.length;
        for (int i = 0; i <ranksWith0.length; i++) {
            expectedOutputs.add(new Pair<LongWritable, AvroKey<GenericRecord>>(
                    new LongWritable(ranksWith0[i]),
                    new AvroKey<GenericRecord>(records[0])));
            expectedOutputs.add(new Pair<LongWritable, AvroKey<GenericRecord>>(
                    new LongWritable(ranksWith1[i]),
                    new AvroKey<GenericRecord>(records[1])));
        }

        final List<Pair<LongWritable, AvroKey<GenericRecord>>> result = mapDriver.run();
        for (Pair p : result) {
            LOG.info(String.format("Rank %s -> contains %s records",p.getFirst(),p.getSecond()));
        }
    }

    @Test
    public void test2() throws IOException {
        List<Pair<AvroKey<GenericRecord>,NullWritable>> inputs
                = new ArrayList<Pair<AvroKey<GenericRecord>, NullWritable>>();
        for (int i = 0; i < 10; i++) {
            inputs.add(new Pair<AvroKey<GenericRecord>, NullWritable>(
                    new AvroKey<GenericRecord>(records[i]), NullWritable.get()));
        }
        mapReduceDriver.run();
        for (int i = 0 ; i < (1 << fieldNames.length);i++) {
            String counterName = mapReduceDriver.getCounters()
                    .findCounter("similarity.vectors", String.valueOf(i)).getDisplayName();
            long value =
                    mapReduceDriver.getCounters()
                            .findCounter("similarity.vectors", String.valueOf(i)).getValue();
            LOG.info("Counter {} value {}",counterName,value);
        }
    }


    private static Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
        FileInputStream fis = new FileInputStream(schemaFile);
        Schema schema = (new Schema.Parser()).parse(fis);
        fis.close();
        return schema;
    }
}

