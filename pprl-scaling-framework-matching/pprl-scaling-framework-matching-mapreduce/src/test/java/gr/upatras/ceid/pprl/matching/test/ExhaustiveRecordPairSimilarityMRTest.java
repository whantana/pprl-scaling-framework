package gr.upatras.ceid.pprl.matching.test;

import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import gr.upatras.ceid.pprl.matching.mapreduce.ExhaustiveRecordPairSimilarityTool;
import gr.upatras.ceid.pprl.matching.mapreduce.GenerateRecordPairsMapper;
import gr.upatras.ceid.pprl.matching.mapreduce.RecordPairSimilarityCombiner;
import gr.upatras.ceid.pprl.matching.mapreduce.RecordPairSimilarityReducer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ExhaustiveRecordPairSimilarityMRTest {

    private MapDriver<AvroKey<GenericRecord>, NullWritable, LongWritable, AvroValue<GenericRecord>> mapDriver;
    private MapReduceDriver<
            AvroKey<GenericRecord>, NullWritable,
            LongWritable, AvroValue<GenericRecord>,
            NullWritable,NullWritable> mapReduceDriver;

    private static final Logger LOG = LoggerFactory.getLogger(ExhaustiveRecordPairSimilarityMRTest.class);


    private Schema schema;
    private GenericRecord[] records;
    private final String[] fieldNames = {"name","surname","location"};

    @Before
    public void setup() throws IOException {
        schema = loadAvroSchemaFromFile(
                new File("data/person_small/schema/person_small.avsc"));
        records = loadAvroRecordsFromFiles(schema, new File[]{
                new File("data/person_small/avro/person_small.avro")});

        mapDriver = MapDriver.newMapDriver(new GenerateRecordPairsMapper());
        mapDriver.getContext().getConfiguration().setInt("record.count", records.length);
        mapDriver.getContext().getConfiguration().set("uid.field.name", "id");
        AvroSerialization.setKeyWriterSchema(mapDriver.getConfiguration(), schema);
        AvroSerialization.setKeyReaderSchema(mapDriver.getConfiguration(), schema);
        mapDriver.setOutputSerializationConfiguration(mapDriver.getConfiguration());
        AvroSerialization.addToConfiguration(mapDriver.getOutputSerializationConfiguration());
        AvroSerialization.setValueWriterSchema(mapDriver.getOutputSerializationConfiguration(), schema);
        AvroSerialization.setValueReaderSchema(mapDriver.getOutputSerializationConfiguration(), schema);
        LOG.info("MapDriver ready.");

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(
                new GenerateRecordPairsMapper(),
                new RecordPairSimilarityReducer(),
                new RecordPairSimilarityCombiner()
        );
        mapReduceDriver.getConfiguration().setInt("record.count", records.length);
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
        List<Pair<LongWritable, AvroValue<GenericRecord>>> expectedOutputs =
                new ArrayList<Pair<LongWritable,  AvroValue<GenericRecord>>>();
        long[] ranksWith0 = CombinatoricsUtil.ranksContaining(0,records.length);
        LOG.info("Ranks of element 0 in {} : {}",records.length, Arrays.toString(ranksWith0));
        for (int i = 0; i <ranksWith0.length; i++) {
            expectedOutputs.add(new Pair<LongWritable, AvroValue<GenericRecord>>(
                    new LongWritable(ranksWith0[i]),
                    new AvroValue<GenericRecord>(records[0])));
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
        List<Pair<LongWritable, AvroValue<GenericRecord>>> expectedOutputs =
                new ArrayList<Pair<LongWritable,  AvroValue<GenericRecord>>>();
        long[] ranksWith0 = CombinatoricsUtil.ranksContaining(0,records.length);
        long[] ranksWith1 = CombinatoricsUtil.ranksContaining(1,records.length);
        LOG.info("Ranks of element 0 in {} : {}",records.length, Arrays.toString(ranksWith0));
        LOG.info("Ranks of element 1 in {} : {}",records.length, Arrays.toString(ranksWith1));
        assert ranksWith0.length == ranksWith1.length;
        for (int i = 0; i <ranksWith0.length; i++) {
            expectedOutputs.add(new Pair<LongWritable, AvroValue<GenericRecord>>(
                    new LongWritable(ranksWith0[i]),
                    new AvroValue<GenericRecord>(records[0])));
            expectedOutputs.add(new Pair<LongWritable, AvroValue<GenericRecord>>(
                    new LongWritable(ranksWith1[i]),
                    new AvroValue<GenericRecord>(records[1])));
        }

        final List<Pair<LongWritable, AvroValue<GenericRecord>>> result = mapDriver.run();
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
        final Properties expectedProperties = similarityMatrix(records,fieldNames).toProperties();
        LOG.info("matrix = " + properties);
        LOG.info("expectedMatrix = " + expectedProperties);
        assertEquals(properties,expectedProperties);
    }


    private static Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
        FileInputStream fis = new FileInputStream(schemaFile);
        Schema schema = (new Schema.Parser()).parse(fis);
        fis.close();
        return schema;
    }

    private static GenericRecord[] loadAvroRecordsFromFiles(final Schema schema,final File[] avroFiles) throws IOException {
        final List<GenericRecord> recordList =  new ArrayList<GenericRecord>();
        int i = 0;
        for (File avroFile : avroFiles) {
            final DataFileReader<GenericRecord> reader =
                    new DataFileReader<GenericRecord>(avroFile,
                            new GenericDatumReader<GenericRecord>(schema));
            for (GenericRecord record : reader) recordList.add(i++,record);
            reader.close();
        }
        return recordList.toArray(new GenericRecord[recordList.size()]);
    }

    public static SimilarityMatrix similarityMatrix(final GenericRecord[] records,
                                                    final String[] fieldNames,
                                                    final String similarityMethodName) {
        final long pairCount = CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = fieldNames.length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final SimilarityMatrix matrix = new SimilarityMatrix(fieldCount);
        final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
        do {
            int pair[] = pairIter.next();
            boolean[] row = new boolean[fieldCount];
            for(int j = 0 ; j < fieldCount ; j++) {
                String s1 = String.valueOf(records[pair[0]].get(fieldNames[j]));
                String s2 = String.valueOf(records[pair[1]].get(fieldNames[j]));
                if(SimilarityMatrix.similarity(similarityMethodName, s1, s2)) row[j] = true;
            }
            matrix.set(row);

        }while(pairIter.hasNext());
        return matrix;
    }

    public static SimilarityMatrix similarityMatrix(final GenericRecord[] records,
                                                    final String[] fieldNames) {
        return similarityMatrix(records, fieldNames, SimilarityMatrix.DEFAULT_SIMILARITY_METHOD_NAME);
    }
}

