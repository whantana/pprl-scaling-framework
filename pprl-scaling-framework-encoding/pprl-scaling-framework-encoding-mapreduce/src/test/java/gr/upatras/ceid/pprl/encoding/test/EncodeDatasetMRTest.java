package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.encoding.EncodingAvroSchemaUtil;
import gr.upatras.ceid.pprl.encoding.mapreduce.BaseBloomFilterEncodingMapper;
import gr.upatras.ceid.pprl.encoding.mapreduce.MultiBloomFilterEncodingMapper;
import gr.upatras.ceid.pprl.encoding.mapreduce.RowBloomFilterEncodingMapper;
import gr.upatras.ceid.pprl.encoding.mapreduce.SimpleBloomFilterEncodingMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static gr.upatras.ceid.pprl.encoding.mapreduce.BaseBloomFilterEncodingMapper.*;

// TODO MRUnit messes up the two schemas seriliazation
@Ignore
public class EncodeDatasetMRTest {

    private static final String EXPECTED_KEY = "journals/acta/Saxena96";
    private static final String EXPECTED_AUTHOR = "Sanjeev Saxena";
    private static final String EXPECTED_TITLE = "Parallel Integer Sorting and Simulation Amongst CRCW Models.";
    private static final String EXPECTED_YEAR = "1996";
    private static final String[] COLUMNS = {"author","title"};
    private static final String UID_COLUMN = "key";


    private static final int N = 1024;
    private static final int K = 30;
    private static final int Q = 2;

    private static final byte[] ONE = new byte[(int) Math.ceil(N / 8)];
    static { ONE[0] = (byte) 1; }

    private GenericRecord inputRecord;

    private GenericRecord outRecordSBF;
    private MapDriver<AvroKey<GenericRecord>,NullWritable,AvroKey<GenericRecord>,NullWritable> mapDriverSBE;

    private GenericRecord outRecordRBF;
    private MapDriver<AvroKey<GenericRecord>,NullWritable,AvroKey<GenericRecord>,NullWritable> mapDriverRBE;

    private GenericRecord outRecordFBF;
    private MapDriver<AvroKey<GenericRecord>,NullWritable,AvroKey<GenericRecord>,NullWritable> mapDriverFBE;

    @Before
    public void setUp() throws IOException, URISyntaxException {

        // input record
        final Schema s = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(
                new File(getClass().getResource("/dblp.avsc").toURI()));
        inputRecord = new GenericData.Record(s);
        inputRecord.put("key", EXPECTED_KEY);
        inputRecord.put("author", EXPECTED_AUTHOR);
        inputRecord.put("title", EXPECTED_TITLE);
        inputRecord.put("year", EXPECTED_YEAR);

        Schema sbes = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(new File(getClass().getResource("/enc_sbf_1024_30_2_dblp.avsc").toURI()));
        outRecordSBF = new GenericData.Record(sbes);
        outRecordSBF.put("key",EXPECTED_KEY);
        outRecordSBF.put("year", EXPECTED_YEAR);
        outRecordSBF.put("enc_SBF_1024_30_2_author_title",
                new GenericData.Fixed(sbes.getField("enc_SBF_1024_30_2_author_title").schema(),ONE));
        mapDriverSBE = setupMapDriver(new SimpleBloomFilterEncodingMapper(),s, sbes);

        Schema rbes = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(
                new File(getClass().getResource("/enc_rbf_1024_30_2_dblp.avsc").toURI()));
        outRecordRBF = new GenericData.Record(rbes);
        outRecordRBF.put("key",EXPECTED_KEY);
        outRecordRBF.put("year", EXPECTED_YEAR);
        outRecordRBF.put("enc_RBF_1024_30_2_author_title",
                new GenericData.Fixed(rbes.getField("enc_RBF_1024_30_2_author_title").schema(),ONE));
        mapDriverRBE = setupMapDriver(new RowBloomFilterEncodingMapper(),s, rbes);


        Schema fbes = EncodingAvroSchemaUtil.loadAvroSchemaFromFile(
                new File(getClass().getResource("/enc_fbf_1024_30_2_dblp.avsc").toURI()));
        outRecordFBF = new GenericData.Record(fbes);
        outRecordFBF.put("key",EXPECTED_KEY);
        outRecordFBF.put("year", EXPECTED_YEAR);
        outRecordFBF.put("enc_FBF_1024_30_2_author",
                new GenericData.Fixed(fbes.getField("enc_FBF_1024_30_2_author").schema(),ONE));
        outRecordFBF.put("enc_FBF_1024_30_2_title",
                new GenericData.Fixed(fbes.getField("enc_FBF_1024_30_2_title").schema(),ONE));
        mapDriverFBE = setupMapDriver(new MultiBloomFilterEncodingMapper(),s, fbes);
    }

    public MapDriver<AvroKey<GenericRecord>,NullWritable,AvroKey<GenericRecord>,NullWritable> setupMapDriver(
                            BaseBloomFilterEncodingMapper mapper,Schema input, Schema output) throws IOException {


        MapDriver<AvroKey<GenericRecord>,NullWritable,AvroKey<GenericRecord>,NullWritable> mapDriver =
                MapDriver.newMapDriver(mapper);

        AvroSerialization.addToConfiguration(mapDriver.getConfiguration());
        AvroSerialization.setKeyWriterSchema(mapDriver.getConfiguration(), input);
        AvroSerialization.setKeyReaderSchema(mapDriver.getConfiguration(), input);
        mapDriver.setOutputSerializationConfiguration(mapDriver.getConfiguration());
        AvroSerialization.addToConfiguration(mapDriver.getOutputSerializationConfiguration());
        AvroSerialization.setKeyWriterSchema(mapDriver.getOutputSerializationConfiguration(), output);
        AvroSerialization.setKeyReaderSchema(mapDriver.getOutputSerializationConfiguration(), output);


        mapDriver.getConfiguration().set(INPUT_SCHEMA_KEY, input.toString());
        mapDriver.getConfiguration().set(ENCODING_SCHEMA_KEY, output.toString());
        mapDriver.getConfiguration().set(INPUT_UID_COLUMN_KEY, UID_COLUMN);
        mapDriver.getConfiguration().setStrings(ENCODING_COLUMNS_KEY, COLUMNS);
        mapDriver.getConfiguration().setInt(N_KEY, N);
        mapDriver.getConfiguration().setInt(K_KEY, K);
        mapDriver.getConfiguration().setInt(Q_KEY,Q);

        return mapDriver;
    }

    @Test
    public void test1() throws IOException {
        mapDriverSBE.withInput(new AvroKey<GenericRecord>(inputRecord), NullWritable.get());
        mapDriverSBE.withOutput(new AvroKey<GenericRecord>(outRecordSBF), NullWritable.get());
        mapDriverSBE.runTest();

        mapDriverRBE.withInput(new AvroKey<GenericRecord>(inputRecord), NullWritable.get());
        mapDriverRBE.withOutput(new AvroKey<GenericRecord>(outRecordRBF), NullWritable.get());
        mapDriverRBE.runTest();

        mapDriverFBE.withInput(new AvroKey<GenericRecord>(inputRecord), NullWritable.get());
        mapDriverFBE.withOutput(new AvroKey<GenericRecord>(outRecordFBF), NullWritable.get());
        mapDriverFBE.runTest();
    }
}
