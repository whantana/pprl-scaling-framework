package gr.upatras.ceid.pprl.test;


import avro.shaded.com.google.common.collect.Lists;
import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.mapreduce.BlockingKeyWritable;
import gr.upatras.ceid.pprl.mapreduce.BlockingKeyWritableComparator;
import gr.upatras.ceid.pprl.mapreduce.BlockingKeyWritablePartitioner;
import gr.upatras.ceid.pprl.mapreduce.CommonKeys;
import gr.upatras.ceid.pprl.mapreduce.GenerateBucketsReducer;
import gr.upatras.ceid.pprl.mapreduce.HammingLSHBlockingMapperV2;
import gr.upatras.ceid.pprl.mapreduce.TextArrayWritable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class HammingLSHFPSBlockingMRTest {
    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHFPSBlockingMRTest.class);

    private MapDriver<AvroKey<GenericRecord>, NullWritable, BlockingKeyWritable, Text> blockingMapperDriverB;

    private BlockingKeyWritablePartitioner partitioner;
    private Map<BlockingKeyWritable,List<Text>>[] partitionedMapperResults;

    private ReduceDriver<BlockingKeyWritable, Text, BlockingKeyWritable, TextArrayWritable>[] generateBuckets;


    private GenericRecord[] aliceEncodedRecords;
    private GenericRecord[] bobEncodedRecords;

    final Configuration conf = new Configuration();

    int L = 10;
    int K = 5;
    int C = 2;
    int R = 4;

    @Before
    public void setup() throws IOException, DatasetException, BloomFilterEncodingException, BlockingException {
        final FileSystem fs = FileSystem.get(conf);

        // alice encoding
        final Schema aliceEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(fs,
                new Path("data/clk_voters_a/schema/clk_voters_a.avsc"));
        final BloomFilterEncoding aliceEncoding =
                BloomFilterEncodingUtil.setupNewInstance(aliceEncodingSchema);
        aliceEncodedRecords = DatasetsUtil.loadAvroRecordsFromFSPaths(fs,20,aliceEncodingSchema,
                new Path("data/clk_voters_a/avro/clk_voters_a.avro"));

        // bob encoding
        Schema bobEncodingSchema = DatasetsUtil.loadSchemaFromFSPath(
                fs, new Path("data/clk_voters_b/schema/clk_voters_b.avsc"));
        final BloomFilterEncoding bobEncoding =
                BloomFilterEncodingUtil.setupNewInstance(bobEncodingSchema);
        bobEncodedRecords = DatasetsUtil.loadAvroRecordsFromFSPaths(fs,20,bobEncodingSchema,
                new Path("data/clk_voters_b/avro/clk_voters_b.avro"));

        // blocking instance
        final HammingLSHBlocking blocking =
                new HammingLSHBlocking(L,K,aliceEncoding,bobEncoding);

        // union schema setup
        final Schema unionSchema = Schema.createUnion(
                Lists.newArrayList(aliceEncodingSchema, bobEncodingSchema));

        // common conf setup
        conf.set(CommonKeys.ALICE_SCHEMA, aliceEncodingSchema.toString());
        conf.set(CommonKeys.ALICE_UID, "id");
        conf.set(CommonKeys.BOB_SCHEMA, bobEncodingSchema.toString());
        conf.set(CommonKeys.BOB_UID, "id");
        conf.setStrings(CommonKeys.BLOCKING_KEYS, blocking.groupsAsStrings());
        conf.setInt(CommonKeys.BLOCKING_GROUP_COUNT,L);
        conf.setInt("mapreduce.job.reduces",R);
        conf.setInt(CommonKeys.FREQUENT_PAIR_LIMIT, C);
        conf.set(CommonKeys.SIMILARITY_METHOD_NAME,"hamming");
        conf.setDouble(CommonKeys.SIMILARITY_THRESHOLD,100);

        // avro conf setup
        AvroSerialization.setKeyWriterSchema(conf, bobEncodingSchema);
        AvroSerialization.setKeyReaderSchema(conf, bobEncodingSchema);
        AvroSerialization.setValueReaderSchema(conf, unionSchema);
        AvroSerialization.setValueWriterSchema(conf, unionSchema);
        AvroSerialization.addToConfiguration(conf);


        // setup bob mapper
        blockingMapperDriverB = MapDriver.newMapDriver(new HammingLSHBlockingMapperV2());
        blockingMapperDriverB.getConfiguration().addResource(conf);
        blockingMapperDriverB.setOutputSerializationConfiguration(conf);
        LOG.info("blockingMapperDriverB is ready.");

        // partitioner setup
        partitioner = new BlockingKeyWritablePartitioner();
        partitioner.setConf(conf);
        LOG.info("partitioner ready.");

        // reducers setup
        partitionedMapperResults = new Map[R];
        generateBuckets = new ReduceDriver[R];
        for(int i = 0; i < R ; i++ ) {
            partitionedMapperResults[i] = new TreeMap<BlockingKeyWritable,List<Text>>();
            generateBuckets[i] = ReduceDriver.newReduceDriver(new GenerateBucketsReducer());
            LOG.info("generateBuckets " + i +" ready.");
        }

    }

    @Test
    public void test0() throws IOException {
        final SortedSet<BlockingKeyWritable> sortedKeys = new
                TreeSet<BlockingKeyWritable>();
        BitSet b1 = new BitSet(K);
        b1.set(0);
        b1.set(1);

        BitSet b2 = new BitSet(K);
        b2.set(0);
        b2.set(1);
        b2.set(2);
        BitSet b3 = new BitSet(K);
        b3.set(3);

        BlockingKeyWritable key1 = new BlockingKeyWritable(0,b1,'A');
        BlockingKeyWritable key2 = new BlockingKeyWritable(0,b1,'B');
        BlockingKeyWritable key3 = new BlockingKeyWritable(1,b1,'A');
        BlockingKeyWritable key4 = new BlockingKeyWritable(2,b1,'B');
        BlockingKeyWritable key5 = new BlockingKeyWritable(5,b2,'A');
        BlockingKeyWritable key6 = new BlockingKeyWritable(5,b3,'A');
        BlockingKeyWritable key7 = new BlockingKeyWritable(5,b2,'B');
        sortedKeys.addAll(Arrays.asList(key1,key3,key2,key5,key6,key7,key4));
        for (BlockingKeyWritable bkw : sortedKeys) {
            LOG.info(bkw.toString());
        }

        DataOutputBuffer dob1 = new DataOutputBuffer();
        key6.write(dob1);
        DataOutputBuffer dob2 = new DataOutputBuffer();
        key7.write(dob2);

        int result = (new BlockingKeyWritableComparator()).compare(
                dob1.getData(),0,dob1.getLength(),
                dob2.getData(),0,dob2.getLength()
        );
        LOG.info("Compare dob1,dob2 : {}",result);
    }

    @Test
    public void test1() throws IOException {
        final List<Pair<BlockingKeyWritable,Text>> blockingResults = new ArrayList<Pair<BlockingKeyWritable,Text>>();

        blockingMapperDriverB.addInput(new AvroKey<GenericRecord>(bobEncodedRecords[0]), NullWritable.get());
        blockingMapperDriverB.addInput(new AvroKey<GenericRecord>(bobEncodedRecords[1]), NullWritable.get());
        blockingMapperDriverB.addInput(new AvroKey<GenericRecord>(bobEncodedRecords[2]), NullWritable.get());
        blockingMapperDriverB.addInput(new AvroKey<GenericRecord>(bobEncodedRecords[3]), NullWritable.get());
        blockingResults.addAll(blockingMapperDriverB.run());

        for(Pair<BlockingKeyWritable,Text> pair : blockingResults) {
            final BlockingKeyWritable key = pair.getFirst();
            final Text value = pair.getSecond();
            final int partition = partitioner.getPartition(key,value,R);
            if(!partitionedMapperResults[partition].containsKey(key))
                partitionedMapperResults[partition].put(key, new ArrayList<Text>());
            partitionedMapperResults[partition].get(key).add(value);
        }

        final List<Pair<BlockingKeyWritable,TextArrayWritable>>[] reducersResults = new List[R];
        for (int i = 0; i < R; i++) {
            LOG.info("Partition : " + i);
            Map<BlockingKeyWritable,List<Text>> map = partitionedMapperResults[i];
            for(Map.Entry<BlockingKeyWritable,List<Text>> entry : map.entrySet()) {
                LOG.info("\t {} : {}",entry.getKey(),entry.getValue().toString());
                generateBuckets[i].addInput(entry.getKey(), entry.getValue());
            }
            reducersResults[i] = generateBuckets[i].run();
        }


        LOG.info("Buckets : ");
        for (int i = 0; i < R; i++) {
            for (Pair<BlockingKeyWritable, TextArrayWritable> keyValue : reducersResults[i]) {
                LOG.info("bucket= {} , ids = {}", keyValue.getFirst().toString(K),
                        Arrays.toString(keyValue.getSecond().toStrings()));
            }
        }

        List<Counters> allCounters = new ArrayList<Counters>();
        allCounters.add(blockingMapperDriverB.getCounters());
        for (int i = 0 ; i < generateBuckets.length; i++)
            allCounters.add( generateBuckets[i].getCounters());
// TODO TESTS HERE FOR THE FPS SOLUTION
//        allCounters.add(findFrequentPairsMapReduceDriver.getCounters());
//        allCounters.add(formRecordPairsMapperDriverA.getCounters());
//        allCounters.add(formRecordPairsMapperDriverB.getCounters());
//        allCounters.add(privateSimilarityReducerDriver.getCounters());

        LOG.info("Counters : ");
        for(Counters c : allCounters) {
            Iterator<Counter> iterator =
                    c.getGroup(CommonKeys.COUNTER_GROUP_NAME).iterator();
            while(iterator.hasNext()) {
                final Counter cc = iterator.next();
                LOG.info("{} : {}",cc.getDisplayName(), cc.getValue());
            }
        }
    }
}
