package gr.upatras.ceid.pprl.test;

import avro.shaded.com.google.common.collect.Lists;
import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.mapreduce.BlockingGroupPartitioner;
import gr.upatras.ceid.pprl.mapreduce.CountIdPairsMapper;
import gr.upatras.ceid.pprl.mapreduce.FindFrequentIdPairsCombiner;
import gr.upatras.ceid.pprl.mapreduce.FindFrequentIdPairsReducer;
import gr.upatras.ceid.pprl.mapreduce.GenerateIdPairsReducer;
import gr.upatras.ceid.pprl.mapreduce.HammingLSHBlockingMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class HammingLSHBlockingMRTest {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHBlockingMRTest.class);


    private MapDriver<AvroKey<GenericRecord>, NullWritable, Text, Text> mapDriver;
    private BlockingGroupPartitioner partitioner;
    private Map<Text,List<Text>>[] partitionedMapperResults;
    private ReduceDriver<Text, Text, Text, Text>[] reduceDrivers;

    private BloomFilterEncoding aliceEncoding;
    private GenericRecord[] aliceEncodedRecords;

    private BloomFilterEncoding bobEncoding;
    private GenericRecord[] bobEncodedRecords;

    int L = 10;
    int K = 5;
    int C = 2;
    int R = 4;

    @Before
    public void setup()
            throws IOException, DatasetException,
            BloomFilterEncodingException, BlockingException {
        FileSystem fs = FileSystem.get(new Configuration());

        Schema aliceEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(
                fs, new Path("data/clk_voters_a/schema/clk_voters_a.avsc"));
        aliceEncoding = BloomFilterEncodingUtil.setupNewInstance(aliceEncodingSchema);
        aliceEncodedRecords = DatasetsUtil.loadAvroRecordsFromFSPaths(
                fs,20,aliceEncodingSchema, new Path("data/clk_voters_a/avro/clk_voters_a.avro"));


        Schema bobEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(
                fs, new Path("data/clk_voters_b/schema/clk_voters_b.avsc"));
        bobEncoding = BloomFilterEncodingUtil.setupNewInstance(bobEncodingSchema);
        bobEncodedRecords = DatasetsUtil.loadAvroRecordsFromFSPaths(
                fs,2,bobEncodingSchema , new Path("data/clk_voters_b/avro/clk_voters_b.avro"));

        final HammingLSHBlocking blocking = new HammingLSHBlocking(L,K,aliceEncoding,bobEncoding);
        Schema unionSchema = Schema.createUnion(
                Lists.newArrayList(aliceEncodingSchema, bobEncodingSchema));


        mapDriver = MapDriver.newMapDriver(new HammingLSHBlockingMapper());
        final Configuration configuration = mapDriver.getContext().getConfiguration();
        configuration.set(HammingLSHBlockingMapper.ALICE_SCHEMA_KEY, aliceEncodingSchema.toString());
        configuration.set(HammingLSHBlockingMapper.ALICE_UID_KEY, "id");
        configuration.set(HammingLSHBlockingMapper.BOB_SCHEMA_KEY, bobEncodingSchema.toString());
        configuration.set(HammingLSHBlockingMapper.BOB_UID_KEY, "id");
        configuration.setStrings(HammingLSHBlockingMapper.BLOCKING_KEYS_KEY,blocking.groupsAsStrings());
        AvroSerialization.setKeyWriterSchema(configuration, unionSchema);
        AvroSerialization.setKeyReaderSchema(configuration, unionSchema);
        mapDriver.setOutputSerializationConfiguration(configuration);
        AvroSerialization.addToConfiguration(configuration);
        LOG.info("MapDriver ready.");

        configuration.setInt(BlockingGroupPartitioner.BLOCKING_GROUP_COUNT_KEY,L);
        configuration.setInt("mapreduce.job.reduces",R);
        partitioner = new BlockingGroupPartitioner();
        partitioner.setConf(configuration);
        partitionedMapperResults = new Map[R];
        reduceDrivers = new ReduceDriver[R];
        for(int i = 0; i < R ; i++ ) {
            reduceDrivers[i] = ReduceDriver.newReduceDriver(new GenerateIdPairsReducer());
            partitionedMapperResults[i] = new TreeMap<Text,List<Text>>(
                    WritableComparator.get(Text.class));
        }
    }

    @Test
    public void test1() throws IOException {
        mapDriver.withInput(new AvroKey<GenericRecord>(bobEncodedRecords[0]), NullWritable.get());
        mapDriver.addInput(new AvroKey<GenericRecord>(bobEncodedRecords[1]), NullWritable.get());
        mapDriver.addInput(new AvroKey<GenericRecord>(aliceEncodedRecords[0]), NullWritable.get());
        mapDriver.addInput(new AvroKey<GenericRecord>(aliceEncodedRecords[1]), NullWritable.get());
        mapDriver.addInput(new AvroKey<GenericRecord>(aliceEncodedRecords[2]), NullWritable.get());
        mapDriver.addInput(new AvroKey<GenericRecord>(aliceEncodedRecords[3]), NullWritable.get());
        for(Pair<Text,Text> pair : mapDriver.run()) {
            final Text key = pair.getFirst();
            final Text value = pair.getSecond();
            final int partition = partitioner.getPartition(key,value,R);
            if(!partitionedMapperResults[partition].containsKey(key))
                partitionedMapperResults[partition].put(key, new ArrayList<Text>());
            partitionedMapperResults[partition].get(key).add(value);
        }

        final List<Pair<Text,Text>>[] reducersResults = new List[R];
        for (int i = 0; i < R; i++) {
            LOG.info("Partition : " + i);
            Map<Text,List<Text>> map = partitionedMapperResults[i];
            for(Map.Entry<Text,List<Text>> entry : map.entrySet()) {
                LOG.info("\t {} : {}",entry.getKey(),entry.getValue().toString());
                reduceDrivers[i].addInput(entry.getKey(),entry.getValue());
            }
            reducersResults[i] = reduceDrivers[i].run();
        }

        List<Pair<Text,Text>> allPairs = new ArrayList<Pair<Text, Text>>();
        LOG.info("All pairs : ");
        for (int i = 0; i < R; i++) {
            for (Pair<Text, Text> keyValue : reducersResults[i]) {
                LOG.info("pair = {}", keyValue.toString());
                allPairs.add(keyValue);
            }
        }
        LOG.info("All pairs size : " + allPairs.size());


        final MapReduceDriver<Text,Text,Text,IntWritable,Text,Text> mapReduceDriver =
                MapReduceDriver.newMapReduceDriver(
                        new CountIdPairsMapper(),
                        new FindFrequentIdPairsReducer(),
                        new FindFrequentIdPairsCombiner());
        mapReduceDriver.getConfiguration().setInt(FindFrequentIdPairsReducer.FREQUENT_PAIR_LIMIT_KEY,C);
        for (Pair<Text,Text> in : allPairs)
            mapReduceDriver.addInput(in);
        List<Pair<Text,Text>> frequentPairs = mapReduceDriver.run();
        LOG.info("Frequent pairs : ");
        for(Pair<Text, Text> keyValue : frequentPairs) {
            LOG.info("pair = {}", keyValue.toString());
        }
        LOG.info("Frequent pairs size : " + frequentPairs.size());
    }
}
