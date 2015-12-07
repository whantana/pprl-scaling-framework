package gr.upatras.ceid.pprl.datasets.test;


import gr.upatras.ceid.pprl.datasets.DatasetStatsWritable;
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
    private MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetStatsWritable> mapDriverQ1;
    private MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetStatsWritable> mapDriverQ2;
    private MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetStatsWritable> mapDriverQ3;
    private ReduceDriver<Text,DatasetStatsWritable,Text,DatasetStatsWritable> reduceDriver1;
    private ReduceDriver<Text,DatasetStatsWritable,Text,DatasetStatsWritable> reduceDriver2;

    @Before
    public void setUp() throws IOException, URISyntaxException {

        // input record
        final Schema s = loadAvroSchemaFromFile(
                new File(getClass().getResource("/dblp.avsc").toURI()));
        inputRecord = new GenericData.Record(s);
        inputRecord.put("key", EXPECTED_KEY);
        inputRecord.put("author", EXPECTED_AUTHOR);
        inputRecord.put("title", EXPECTED_TITLE);
        inputRecord.put("year", EXPECTED_YEAR);
        mapDriverQ1 = setupMapDriver(new GetDatasetsStatsMapper(),s,1);
        mapDriverQ2 = setupMapDriver(new GetDatasetsStatsMapper(), s, 2);
        mapDriverQ3 = setupMapDriver(new GetDatasetsStatsMapper(),s,3);
        reduceDriver1 = ReduceDriver.newReduceDriver(new GetDatasetsStatsReducer());
        reduceDriver2 = ReduceDriver.newReduceDriver(new GetDatasetsStatsReducer());
    }


    public MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetStatsWritable> setupMapDriver(
            GetDatasetsStatsMapper mapper,Schema input,int Q) throws IOException {


        MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetStatsWritable> mapDriver =
                MapDriver.newMapDriver(mapper);

        AvroSerialization.addToConfiguration(mapDriver.getConfiguration());
        AvroSerialization.setKeyWriterSchema(mapDriver.getConfiguration(), input);
        AvroSerialization.setKeyReaderSchema(mapDriver.getConfiguration(), input);


        mapDriver.getConfiguration().set(GetDatasetsStatsMapper.INPUT_SCHEMA_KEY, input.toString());
        mapDriver.getConfiguration().setInt(GetDatasetsStatsMapper.Q_KEY, Q);

        return mapDriver;
    }

    @Test
    public void test1() throws IOException {
        List<Pair<Text,DatasetStatsWritable>> pairs = new ArrayList<Pair<Text,DatasetStatsWritable>>();

        pairs.clear();
        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("key"), new DatasetStatsWritable(
                EXPECTED_KEY.length(),
                QGramUtil.calcQgramsCount(EXPECTED_KEY, 1))));

        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("author"), new DatasetStatsWritable(
                EXPECTED_AUTHOR.length(),
                QGramUtil.calcQgramsCount(EXPECTED_AUTHOR, 1))));

        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("title"), new DatasetStatsWritable(
                EXPECTED_TITLE.length(),
                QGramUtil.calcQgramsCount(EXPECTED_TITLE, 1))));

        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("year"), new DatasetStatsWritable(
                EXPECTED_YEAR.length(),
                QGramUtil.calcQgramsCount(EXPECTED_YEAR, 1))));
        mapDriverQ1.withInput(new AvroKey<GenericRecord>(inputRecord), NullWritable.get());
        mapDriverQ1.withAllOutput(pairs);
        mapDriverQ1.runTest();

        pairs.clear();
        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("key"), new DatasetStatsWritable(
                EXPECTED_KEY.length(),
                QGramUtil.calcQgramsCount(EXPECTED_KEY, 2))));

        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("author"), new DatasetStatsWritable(
                EXPECTED_AUTHOR.length(),
                QGramUtil.calcQgramsCount(EXPECTED_AUTHOR, 2))));

        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("title"), new DatasetStatsWritable(
                EXPECTED_TITLE.length(),
                QGramUtil.calcQgramsCount(EXPECTED_TITLE, 2))));

        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("year"), new DatasetStatsWritable(
                EXPECTED_YEAR.length(),
                QGramUtil.calcQgramsCount(EXPECTED_YEAR, 2))));
        mapDriverQ2.withInput(new AvroKey<GenericRecord>(inputRecord), NullWritable.get());
        mapDriverQ2.withAllOutput(pairs);
        mapDriverQ2.runTest();

        pairs.clear();
        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("key"), new DatasetStatsWritable(
                EXPECTED_KEY.length(),
                QGramUtil.calcQgramsCount(EXPECTED_KEY, 3))));

        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("author"), new DatasetStatsWritable(
                EXPECTED_AUTHOR.length(),
                QGramUtil.calcQgramsCount(EXPECTED_AUTHOR, 3))));

        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("title"), new DatasetStatsWritable(
                EXPECTED_TITLE.length(),
                QGramUtil.calcQgramsCount(EXPECTED_TITLE, 3))));

        pairs.add(new Pair<Text, DatasetStatsWritable>(new Text("year"), new DatasetStatsWritable(
                EXPECTED_YEAR.length(),
                QGramUtil.calcQgramsCount(EXPECTED_YEAR, 3))));
        mapDriverQ3.withInput(new AvroKey<GenericRecord>(inputRecord), NullWritable.get());
        mapDriverQ3.withAllOutput(pairs);
        mapDriverQ3.runTest();
    }

    @Test
    public void test2() throws IOException {
        Pair<Text,List<DatasetStatsWritable>> pair1 = new Pair<Text,List<DatasetStatsWritable>>(
                new Text("key1"), new ArrayList<DatasetStatsWritable>(Arrays.asList(
                    new DatasetStatsWritable(1.0,1.0),
                    new DatasetStatsWritable(2.0,2.0),
                    new DatasetStatsWritable(3.0,4.0),
                    new DatasetStatsWritable(4.0,8.0))));
        Pair<Text,List<DatasetStatsWritable>> pair2 = new Pair<Text,List<DatasetStatsWritable>>(
                new Text("key2"), new ArrayList<DatasetStatsWritable>(Arrays.asList(
                    new DatasetStatsWritable(1.0,2.0),
                    new DatasetStatsWritable(1.0,2.0),
                    new DatasetStatsWritable(1.0,2.0),
                    new DatasetStatsWritable(1.0,2.0))));

        Pair<Text,DatasetStatsWritable> outPair1 = new Pair<Text,DatasetStatsWritable>(
                new Text("key1"), new DatasetStatsWritable(
                    ((1.0 + 2.0 + 3.0 + 4.0)/(double)4),((1.0 + 2.0 + 4.0 + 8.0)/(double)4)
                ));
        Pair<Text,DatasetStatsWritable> outPair2 = new Pair<Text,DatasetStatsWritable>(
                new Text("key2"), new DatasetStatsWritable(
                    ((1.0 + 1.0 + 1.0 + 1.0)/(double)4),((2.0 + 2.0 + 2.0 + 2.0)/(double)4)
                ));


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
