package gr.upatras.ceid.pprl.datasets.test;


import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.QGramUtil;
import gr.upatras.ceid.pprl.datasets.mapreduce.GetDatasetsStatsMapper;
import gr.upatras.ceid.pprl.datasets.mapreduce.GetDatasetsStatsReducer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DatasetStatMRTest {
    private static final String EXPECTED_KEY = "journals/acta/Saxena96";
    private static final String EXPECTED_AUTHOR = "Sanjeev Saxena";
    private static final String EXPECTED_TITLE = "Parallel Integer Sorting and Simulation Amongst CRCW Models.";
    private static final String EXPECTED_YEAR = "1996";

    private GenericRecord inputRecord;
    private MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetStatistics> mapDriver;
    private ReduceDriver<Text,DatasetStatistics,Text,DatasetStatistics> reduceDriver1;
    private ReduceDriver<Text,DatasetStatistics,Text,DatasetStatistics> reduceDriver2;

    @Before
    public void setUp() throws IOException, URISyntaxException {

        // input record
        final Schema s = loadAvroSchemaFromFile(new File("dblp.avsc"));
        //new File(getClass().getResource("/dblp.avsc").toURI()));
        inputRecord = new GenericData.Record(s);
        inputRecord.put("key", EXPECTED_KEY);
        inputRecord.put("author", EXPECTED_AUTHOR);
        inputRecord.put("title", EXPECTED_TITLE);
        inputRecord.put("year", EXPECTED_YEAR);
        mapDriver = setupMapDriver(new GetDatasetsStatsMapper(),s);
        reduceDriver1 = ReduceDriver.newReduceDriver(new GetDatasetsStatsReducer());
        reduceDriver2 = ReduceDriver.newReduceDriver(new GetDatasetsStatsReducer());
    }


    public MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetStatistics> setupMapDriver(
            GetDatasetsStatsMapper mapper,Schema input) throws IOException {


        MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetStatistics> mapDriver =
                MapDriver.newMapDriver(mapper);

        AvroSerialization.addToConfiguration(mapDriver.getConfiguration());
        AvroSerialization.setKeyWriterSchema(mapDriver.getConfiguration(), input);
        AvroSerialization.setKeyReaderSchema(mapDriver.getConfiguration(), input);


        mapDriver.getConfiguration().set(GetDatasetsStatsMapper.INPUT_SCHEMA_KEY, input.toString());

        return mapDriver;
    }

    @Test
    public void test1() throws IOException {
        List<Pair<Text, DatasetStatistics>> pairs = new ArrayList<Pair<Text, DatasetStatistics>>();

        pairs.clear();
        pairs.add(new Pair<Text, DatasetStatistics>(new Text("key"), new DatasetStatistics(
                EXPECTED_KEY.length(), new double[]{
                QGramUtil.calcQgramsCount(EXPECTED_KEY, 2),
                QGramUtil.calcQgramsCount(EXPECTED_KEY, 3),
                QGramUtil.calcQgramsCount(EXPECTED_KEY, 4),
        }
        )));

        pairs.add(new Pair<Text, DatasetStatistics>(new Text("author"), new DatasetStatistics(
                EXPECTED_AUTHOR.length(), new double[]{
                QGramUtil.calcQgramsCount(EXPECTED_AUTHOR, 2),
                QGramUtil.calcQgramsCount(EXPECTED_AUTHOR, 3),
                QGramUtil.calcQgramsCount(EXPECTED_AUTHOR, 4),
        })));


        pairs.add(new Pair<Text, DatasetStatistics>(new Text("title"), new DatasetStatistics(
                EXPECTED_TITLE.length(), new double[]{
                QGramUtil.calcQgramsCount(EXPECTED_TITLE, 2),
                QGramUtil.calcQgramsCount(EXPECTED_TITLE, 3),
                QGramUtil.calcQgramsCount(EXPECTED_TITLE, 4),
        })));

        pairs.add(new Pair<Text, DatasetStatistics>(new Text("year"), new DatasetStatistics(
                EXPECTED_YEAR.length(), new double[]{
                QGramUtil.calcQgramsCount(EXPECTED_YEAR, 2),
                QGramUtil.calcQgramsCount(EXPECTED_YEAR, 3),
                QGramUtil.calcQgramsCount(EXPECTED_YEAR, 4),
        })));

        mapDriver.withInput(new AvroKey<GenericRecord>(inputRecord), NullWritable.get());
        mapDriver.withAllOutput(pairs);
        mapDriver.runTest();
    }

    @Test
    public void test2() throws IOException {
        Pair<Text,List<DatasetStatistics>> pair1 = new Pair<Text,List<DatasetStatistics>>(
                new Text("key1"), new ArrayList<DatasetStatistics>(Arrays.asList(
                new DatasetStatistics(1.0,new double[]{1.0,1.0,1.0}),
                new DatasetStatistics(2.0,new double[]{1.0,1.0,1.0}),
                new DatasetStatistics(3.0,new double[]{1.0,1.0,1.0}),
                new DatasetStatistics(4.0,new double[]{1.0,1.0,1.0}))));
        Pair<Text,List<DatasetStatistics>> pair2 = new Pair<Text,List<DatasetStatistics>>(
                new Text("key2"), new ArrayList<DatasetStatistics>(Arrays.asList(
                new DatasetStatistics(1.0,new double[]{2.0,2.0,2.0}),
                new DatasetStatistics(1.0,new double[]{-2.0,-2.0,-2.0}),
                new DatasetStatistics(1.0,new double[]{2.0,2.0,2.0}),
                new DatasetStatistics(1.0,new double[]{-2.0,-2.0,-2.0}))));

        Pair<Text,DatasetStatistics> outPair1 = new Pair<Text,DatasetStatistics>(
                new Text("key1"), new DatasetStatistics(
                ((1.0 + 2.0 + 3.0 + 4.0)/(double)4), new double[] {1.0, 1.0, 1.0}));
        Pair<Text,DatasetStatistics> outPair2 = new Pair<Text,DatasetStatistics>(
                new Text("key2"), new DatasetStatistics(
                ((1.0 + 1.0 + 1.0 + 1.0)/(double)4), new double[] {0, 0, 0}));

        reduceDriver1.withInput(pair1);
        reduceDriver1.withOutput(outPair1);
        reduceDriver1.runTest();

        reduceDriver2.withInput(pair2);
        reduceDriver2.withOutput(outPair2);
        reduceDriver2.runTest();
    }


    private static Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
        FileInputStream fis = new FileInputStream(schemaFile);
        Schema schema = (new Schema.Parser()).parse(fis);
        fis.close();
        return schema;
    }
}
