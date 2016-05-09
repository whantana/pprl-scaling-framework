package gr.upatras.ceid.pprl.test;

import avro.shaded.com.google.common.collect.Lists;
import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.mapreduce.HammingLSHBlockingMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class HammingLSHBlockingMRTest {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHBlockingMRTest.class);


    private MapDriver<AvroKey<GenericRecord>, NullWritable, Text, Text> mapDriver;

    private BloomFilterEncoding aliceEncoding;
    private GenericRecord[] aliceEncodedRecords;

    private BloomFilterEncoding bobEncoding;
    private GenericRecord[] bobEncodedRecords;

    int L = 10;
    int K = 5;

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
        LOG.info("{}", Arrays.toString(blocking.groupsAsStrings()));
        Schema unionSchema = Schema.createUnion(
                Lists.newArrayList(aliceEncodingSchema, bobEncodingSchema));


        mapDriver = MapDriver.newMapDriver(new HammingLSHBlockingMapper());
        mapDriver.getContext().getConfiguration().set(HammingLSHBlockingMapper.ALICE_SCHEMA_KEY, aliceEncodingSchema.toString());
        mapDriver.getContext().getConfiguration().set(HammingLSHBlockingMapper.ALICE_UID_KEY, "id");
        mapDriver.getContext().getConfiguration().set(HammingLSHBlockingMapper.BOB_SCHEMA_KEY, bobEncodingSchema.toString());
        mapDriver.getContext().getConfiguration().set(HammingLSHBlockingMapper.BOB_UID_KEY, "id");
        mapDriver.getContext().getConfiguration().setStrings(HammingLSHBlockingMapper.BLOCKING_KEYS_KEY,blocking.groupsAsStrings());
        AvroSerialization.setKeyWriterSchema(mapDriver.getConfiguration(), unionSchema);
        AvroSerialization.setKeyReaderSchema(mapDriver.getConfiguration(), unionSchema);
        mapDriver.setOutputSerializationConfiguration(mapDriver.getConfiguration());
        AvroSerialization.addToConfiguration(mapDriver.getOutputSerializationConfiguration());


        LOG.info("MapDriver ready.");

    }

    @Test
    public void test1() throws IOException {
        mapDriver.withInput(new AvroKey<GenericRecord>(bobEncodedRecords[0]), NullWritable.get());
        mapDriver.addInput(new AvroKey<GenericRecord>(aliceEncodedRecords[0]), NullWritable.get());
        for(Pair<Text,Text> pair : mapDriver.run()) {
            LOG.info("{} {}",pair.getFirst(),pair.getSecond());
        }
    }
}
