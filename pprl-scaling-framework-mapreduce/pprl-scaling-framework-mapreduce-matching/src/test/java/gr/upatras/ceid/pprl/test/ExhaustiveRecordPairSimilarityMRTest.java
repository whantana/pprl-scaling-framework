package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.combinatorics.CombinatoricsUtil;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.mapreduce.CommonKeys;
import gr.upatras.ceid.pprl.mapreduce.ExhaustiveRecordPairSimilarityTool;
import gr.upatras.ceid.pprl.mapreduce.GenerateRecordPairsMapper;
import gr.upatras.ceid.pprl.mapreduce.RecordPairSimilarityCombiner;
import gr.upatras.ceid.pprl.mapreduce.RecordPairSimilarityReducer;
import gr.upatras.ceid.pprl.mapreduce.TextArrayWritable;
import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ExhaustiveRecordPairSimilarityMRTest {

    private MapDriver<AvroKey<GenericRecord>, NullWritable, LongWritable, TextArrayWritable> mapDriver;
    private MapReduceDriver<
            AvroKey<GenericRecord>, NullWritable,
            LongWritable, TextArrayWritable,
            NullWritable,NullWritable> mapReduceDriver;

    private static final Logger LOG = LoggerFactory.getLogger(ExhaustiveRecordPairSimilarityMRTest.class);


    private GenericRecord[] records;
    private final String[] fieldNames = {"name","surname","location"};

    @Before
    public void setup() throws IOException, DatasetException {
        final Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Schema schema = DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/person_small/schema/person_small.avsc"));

        records = DatasetsUtil.loadAvroRecordsFromFSPaths(
                fs, schema, new Path("data/person_small/avro/person_small.avro"));

        conf.setInt(CommonKeys.RECORD_COUNT_KEY, records.length);
        conf.set(CommonKeys.UID_FIELD_NAME_KEY,"uiid");
        conf.setStrings(CommonKeys.FIELD_NAMES_KEY, fieldNames);
        conf.set(CommonKeys.SCHEMA_KEY, schema.toString());

        mapDriver = MapDriver.newMapDriver(new GenerateRecordPairsMapper());
        mapDriver.getConfiguration().addResource(conf);
        AvroSerialization.setKeyWriterSchema(mapDriver.getConfiguration(), schema);
        AvroSerialization.setKeyReaderSchema(mapDriver.getConfiguration(), schema);
        mapDriver.setOutputSerializationConfiguration(mapDriver.getConfiguration());
        AvroSerialization.addToConfiguration(mapDriver.getOutputSerializationConfiguration());
        LOG.info("MapDriver ready.");

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(
                new GenerateRecordPairsMapper(),
                new RecordPairSimilarityReducer(),
                new RecordPairSimilarityCombiner()
        );
        mapReduceDriver.getConfiguration().addResource(conf);
        AvroSerialization.setKeyWriterSchema(mapReduceDriver.getConfiguration(), schema);
        AvroSerialization.setKeyReaderSchema(mapReduceDriver.getConfiguration(), schema);
        mapReduceDriver.setOutputSerializationConfiguration(mapReduceDriver.getConfiguration());
        AvroSerialization.addToConfiguration(mapReduceDriver.getOutputSerializationConfiguration());
        LOG.info("MapReduceDriver ready.");
    }

    @Test
    public void test0() throws IOException {
        mapDriver.withInput(new AvroKey<GenericRecord>(records[0]), NullWritable.get());
        long[] ranksWith0 = CombinatoricsUtil.ranksContaining(0,records.length);
        LOG.info("Ranks of element 0 in {} : {}",records.length, Arrays.toString(ranksWith0));

        List<Pair<LongWritable, TextArrayWritable>> expectedOutputs =
                new ArrayList<Pair<LongWritable, TextArrayWritable>>();
        TextArrayWritable values =
                GenerateRecordPairsMapper.toWritableFieldValues(records[0],fieldNames);
        for (long rank : ranksWith0) {
            expectedOutputs.add(
                    new Pair<LongWritable, TextArrayWritable>(
                            new LongWritable(rank), values));
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

        TextArrayWritable values0 =
                GenerateRecordPairsMapper.toWritableFieldValues(records[0],fieldNames);
        TextArrayWritable values1 =
                GenerateRecordPairsMapper.toWritableFieldValues(records[1],fieldNames);

        List<Pair<LongWritable, TextArrayWritable>> expectedOutputs =
                new ArrayList<Pair<LongWritable,  TextArrayWritable>>();
        long[] ranksWith0 = CombinatoricsUtil.ranksContaining(0,records.length);
        long[] ranksWith1 = CombinatoricsUtil.ranksContaining(1,records.length);
        LOG.info("Ranks of element 0 in {} : {}",records.length, Arrays.toString(ranksWith0));
        LOG.info("Ranks of element 1 in {} : {}",records.length, Arrays.toString(ranksWith1));
        assert ranksWith0.length == ranksWith1.length;
        for (int i = 0; i <ranksWith0.length; i++) {
            expectedOutputs.add(new Pair<LongWritable, TextArrayWritable>(
                    new LongWritable(ranksWith0[i]), values0));
            expectedOutputs.add(new Pair<LongWritable, TextArrayWritable>(
                    new LongWritable(ranksWith1[i]), values1));
        }

        final List<Pair<LongWritable, TextArrayWritable>> result = mapDriver.run();
        for (Pair p : result) {
            LOG.info(String.format("Rank %s -> contains %s records",p.getFirst(),p.getSecond()));
        }
    }

    @Test
    public void test2() throws IOException {
        List<Pair<AvroKey<GenericRecord>,NullWritable>> inputs
                = new ArrayList<Pair<AvroKey<GenericRecord>, NullWritable>>();
        for (GenericRecord record : records) {
            inputs.add(new Pair<AvroKey<GenericRecord>, NullWritable>(
                    new AvroKey<GenericRecord>(record), NullWritable.get()));
        }
        mapReduceDriver.withAll(inputs);
        mapReduceDriver.run();

        long pairsDoneInCombine =
                mapReduceDriver.getCounters().findCounter("pairs.done","combine").getValue();

        long pairsDoneInReduce =
                mapReduceDriver.getCounters().findCounter("pairs.done","reduce").getValue();

        LOG.info("pairsDoneInCombine={} pairsDoneInReduce={}",pairsDoneInCombine,pairsDoneInReduce);

        final Properties properties =
            ExhaustiveRecordPairSimilarityTool.counters2Properties(mapReduceDriver.getCounters(), fieldNames);
        final Properties expectedProperties = SimilarityUtil.vectorFrequencies(records,fieldNames).toProperties();
        LOG.info("matrix = " + properties);
        LOG.info("expectedMatrix = " + expectedProperties);
        assertEquals(properties,expectedProperties);
    }
}

