package gr.upatras.ceid.pprl.datasets.test;


import org.junit.Ignore;
@Ignore
public class DatasetStatMRTest {
//    private static final String EXPECTED_KEY = "journals/acta/Saxena96";
//    private static final String EXPECTED_AUTHOR = "Sanjeev Saxena";
//    private static final String EXPECTED_TITLE = "Parallel Integer Sorting and Simulation Amongst CRCW Models.";
//    private static final String EXPECTED_YEAR = "1996";
//
//    private GenericRecord inputRecord;
//    private MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetFieldStatistics> mapDriver;
//    private ReduceDriver<Text,DatasetFieldStatistics,Text,DatasetFieldStatistics> reduceDriver1;
//    private ReduceDriver<Text,DatasetFieldStatistics,Text,DatasetFieldStatistics> reduceDriver2;
//
//    @Before
//    public void setUp() throws IOException, URISyntaxException {
//
//        // input record
//        final Schema s = loadAvroSchemaFromFile(new File("dblp.avsc"));
//        //new File(getClass().getResource("/dblp.avsc").toURI()));
//        inputRecord = new GenericData.Record(s);
//        inputRecord.put("key", EXPECTED_KEY);
//        inputRecord.put("author", EXPECTED_AUTHOR);
//        inputRecord.put("title", EXPECTED_TITLE);
//        inputRecord.put("year", EXPECTED_YEAR);
//        mapDriver = setupMapDriver(new GetDatasetsStatsMapper(),s);
//        reduceDriver1 = ReduceDriver.newReduceDriver(new GetDatasetsStatsReducer());
//        reduceDriver2 = ReduceDriver.newReduceDriver(new GetDatasetsStatsReducer());
//    }
//
//
//    public MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetFieldStatistics> setupMapDriver(
//            GetDatasetsStatsMapper mapper,Schema input) throws IOException {
//
//
//        MapDriver<AvroKey<GenericRecord>,NullWritable,Text,DatasetFieldStatistics> mapDriver =
//                MapDriver.newMapDriver(mapper);
//
//        AvroSerialization.addToConfiguration(mapDriver.getConfiguration());
//        AvroSerialization.setKeyWriterSchema(mapDriver.getConfiguration(), input);
//        AvroSerialization.setKeyReaderSchema(mapDriver.getConfiguration(), input);
//
//
//        mapDriver.getConfiguration().set(GetDatasetsStatsMapper.INPUT_SCHEMA_KEY, input.toString());
//
//        return mapDriver;
//    }
//
//    @Test
//    public void test1() throws IOException {
//        List<Pair<Text, DatasetFieldStatistics>> pairs = new ArrayList<Pair<Text, DatasetFieldStatistics>>();
//
//        pairs.clear();
//        pairs.add(new Pair<Text, DatasetFieldStatistics>(new Text("key"), new DatasetFieldStatistics(
//                EXPECTED_KEY.length(), new double[]{
//                QGramUtil.calcQgramsCount(EXPECTED_KEY, Schema.Type.STRING, 2),
//                QGramUtil.calcQgramsCount(EXPECTED_KEY, Schema.Type.STRING, 3),
//                QGramUtil.calcQgramsCount(EXPECTED_KEY, Schema.Type.STRING, 4),
//        }
//        )));
//
//        pairs.add(new Pair<Text, DatasetFieldStatistics>(new Text("author"), new DatasetFieldStatistics(
//                EXPECTED_AUTHOR.length(), new double[]{
//                QGramUtil.calcQgramsCount(EXPECTED_AUTHOR, Schema.Type.STRING, 2),
//                QGramUtil.calcQgramsCount(EXPECTED_AUTHOR, Schema.Type.STRING, 3),
//                QGramUtil.calcQgramsCount(EXPECTED_AUTHOR, Schema.Type.STRING, 4),
//        })));
//
//
//        pairs.add(new Pair<Text, DatasetFieldStatistics>(new Text("title"), new DatasetFieldStatistics(
//                EXPECTED_TITLE.length(), new double[]{
//                QGramUtil.calcQgramsCount(EXPECTED_TITLE, Schema.Type.STRING, 2),
//                QGramUtil.calcQgramsCount(EXPECTED_TITLE, Schema.Type.STRING, 3),
//                QGramUtil.calcQgramsCount(EXPECTED_TITLE, Schema.Type.STRING, 4),
//        })));
//
//        pairs.add(new Pair<Text, DatasetFieldStatistics>(new Text("year"), new DatasetFieldStatistics(
//                EXPECTED_YEAR.length(), new double[]{
//                QGramUtil.calcQgramsCount(EXPECTED_YEAR, Schema.Type.STRING, 2),
//                QGramUtil.calcQgramsCount(EXPECTED_YEAR, Schema.Type.STRING, 3),
//                QGramUtil.calcQgramsCount(EXPECTED_YEAR, Schema.Type.STRING, 4),
//        })));
//
//        mapDriver.withInput(new AvroKey<GenericRecord>(inputRecord), NullWritable.get());
//        mapDriver.withAllOutput(pairs);
//        mapDriver.runTest();
//    }
//
//    @Test
//    public void test2() throws IOException {
//        Pair<Text,List<DatasetFieldStatistics>> pair1 = new Pair<Text,List<DatasetFieldStatistics>>(
//                new Text("key1"), new ArrayList<DatasetFieldStatistics>(Arrays.asList(
//                new DatasetFieldStatistics(1.0,new double[]{1.0,1.0,1.0}),
//                new DatasetFieldStatistics(2.0,new double[]{1.0,1.0,1.0}),
//                new DatasetFieldStatistics(3.0,new double[]{1.0,1.0,1.0}),
//                new DatasetFieldStatistics(4.0,new double[]{1.0,1.0,1.0}))));
//        Pair<Text,List<DatasetFieldStatistics>> pair2 = new Pair<Text,List<DatasetFieldStatistics>>(
//                new Text("key2"), new ArrayList<DatasetFieldStatistics>(Arrays.asList(
//                new DatasetFieldStatistics(1.0,new double[]{2.0,2.0,2.0}),
//                new DatasetFieldStatistics(1.0,new double[]{-2.0,-2.0,-2.0}),
//                new DatasetFieldStatistics(1.0,new double[]{2.0,2.0,2.0}),
//                new DatasetFieldStatistics(1.0,new double[]{-2.0,-2.0,-2.0}))));
//
//        Pair<Text,DatasetFieldStatistics> outPair1 = new Pair<Text,DatasetFieldStatistics>(
//                new Text("key1"), new DatasetFieldStatistics(
//                ((1.0 + 2.0 + 3.0 + 4.0)/(double)4), new double[] {1.0, 1.0, 1.0}));
//        Pair<Text,DatasetFieldStatistics> outPair2 = new Pair<Text,DatasetFieldStatistics>(
//                new Text("key2"), new DatasetFieldStatistics(
//                ((1.0 + 1.0 + 1.0 + 1.0)/(double)4), new double[] {0, 0, 0}));
//
//        reduceDriver1.withInput(pair1);
//        reduceDriver1.withOutput(outPair1);
//        reduceDriver1.runTest();
//
//        reduceDriver2.withInput(pair2);
//        reduceDriver2.withOutput(outPair2);
//        reduceDriver2.runTest();
//    }
//
//
//    private static Schema loadAvroSchemaFromFile(final File schemaFile) throws IOException {
//        FileInputStream fis = new FileInputStream(schemaFile);
//        Schema schema = (new Schema.Parser()).parse(fis);
//        fis.close();
//        return schema;
//    }
}
