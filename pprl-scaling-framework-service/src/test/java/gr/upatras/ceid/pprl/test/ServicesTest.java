package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import gr.upatras.ceid.pprl.service.blocking.BlockingService;
import gr.upatras.ceid.pprl.service.datasets.DatasetsService;
import gr.upatras.ceid.pprl.service.encoding.EncodingService;
import gr.upatras.ceid.pprl.service.matching.MatchingService;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractMapReduceTests;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.assertTrue;

@ContextConfiguration(locations = "classpath:services-test-context.xml", loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "service_test", configName ="hadoopConfiguration")
public class ServicesTest extends AbstractMapReduceTests {
//    private static final Logger LOG = LoggerFactory.getLogger(ServicesTest.class);
//
//    private static final Path dataRepo = new Path("data");
//    private static final String[] dataNames = {
//            "person_small",
//            "voters_a",
//            "voters_b"
//    };
//    private static final String[][] selectedFieldNames = new String[][]{
//            {"name", "surname"},
//            {"surname", "name", "address", "city"},
//            {"surname", "name", "address", "city"}
//    };
//    private static final String[][] includedFieldNames = new String[][]{
//            {"id", "location"},
//            {"id"},
//            {"id"}
//    };
//    private static final String[] uidFieldNames = new String[]{
//            "id",
//            "id",
//            "id"
//    };
//
//    private static final double[][] initialMUP = new double[][]{
//            {0.9, 0.1, 0.1},
//            {0.9, 0.1, 0.1},
//            {0.9, 0.1, 0.1},
//    };
//
//    private static final int ENCODING_N = 1024;
//    private static final int ENCODING_K = 10;
//    private static final int ENCODING_Q = 2;
//
//    private static final int HLSH_BLOCKING_L = 36;
//    private static final int HLSH_BLOCKING_K = 5;
//    private static final short HLSH_BLOCKING_C = 5;
//
//    private static final int HAMMING_THRESHOLD = 100;
//
//
//    private static final Path[] datasetsBase;
//    private static final Path[] datasetsAvro;
//    private static final Path[] datasetsSchema;
//    private static final Path[] datasetsStats;
//    static {
//        datasetsBase = new Path[dataNames.length];
//
//        for (int i = 0; i < dataNames.length; i++) {
//            datasetsBase[i] = new Path(dataRepo, dataNames[i]);
//        }
//        datasetsAvro = new Path[datasetsBase.length];
//        datasetsSchema = new Path[datasetsBase.length];
//        datasetsStats = new Path[datasetsBase.length];
//        for (int i = 0; i < dataNames.length; i++) {
//            datasetsAvro[i] = new Path(datasetsBase[i], "avro");
//            datasetsSchema[i] =
//                    new Path(new Path(datasetsBase[i], "schema"), dataNames[i] + ".avsc");
//            datasetsStats[i] = new Path(datasetsBase[i], "stats_report.properties");
//        }
//    }
//
//    @Autowired
//    private DatasetsService ds;
//
//    @Autowired
//    private EncodingService es;
//
//    @Autowired
//    private MatchingService ms;
//
//    @Autowired
//    private BlockingService bs;
//
//    @Test
//    public void test0() throws IOException {
//        final FileSystem hdfs = getFileSystem();
//        final Path basePPRLpath = new Path(hdfs.getHomeDirectory(), "pprl");
//        boolean baseExists = hdfs.exists(basePPRLpath);
//        assertTrue(baseExists);
//        LOG.info("HDFS PPRL SITE : {}", new Path(hdfs.getHomeDirectory(), "pprl"));
//    }
//
//     @Test
//     public void test1() throws Exception {
//         // import dblp
//         final Path xmlPath = new Path("data/dblp/xml/dblp.xml");
//         final Path uploadedPath = ds.importDblpXmlDataset(xmlPath,"dblp",0);
//         final FileSystem hdfs = getFileSystem();
//         boolean uploadExists =  hdfs.exists(uploadedPath);
//         assertTrue(uploadExists);
//         LOG.info("Uploaded to path : {} ",uploadedPath);
//         final Path uploadedSchemaPath = new Path(uploadedPath,"schema");
//         final Schema uploadedSchema = ds.loadSchema(
//                 ds.retrieveSchemaPath(uploadedSchemaPath)
//         );
//         LOG.info("Uploaded paths : {}", Arrays.toString(ds.retrieveDirectories("dblp")));
//         LOG.info("Uploaded Schema : {} ",uploadedSchema.toString(true));
//
//         // download dblp avro
//         final Path downloadedPath = ds.downloadFiles("dblp","dl_dblp",new Path("data"));
//         LOG.info("Downloaded to path : {} ",downloadedPath);
//     }
//
//     @Test
//     public void test2() throws Exception {
//         for(int i = 0 ; i < dataNames.length ; i++) {
//             // upload dataset
//             final Path[] avroPaths = new Path[]{datasetsAvro[i]};
//             final Path schemaPath = datasetsSchema[i];
//             final Path uploadedPath = ds.uploadFiles(avroPaths, schemaPath, dataNames[i]);
//             final FileSystem hdfs = getFileSystem();
//             boolean uploadExists = hdfs.exists(uploadedPath);
//             assertTrue(uploadExists);
//             LOG.info("Uploaded to path : {} ", uploadedPath);
//             final Path uploadedSchemaPath = new Path(uploadedPath, "schema");
//             final Schema uploadedSchema = ds.loadSchema(
//                     ds.retrieveSchemaPath(uploadedSchemaPath)
//             );
//             LOG.info("Uploaded paths : {}", Arrays.toString(ds.retrieveDirectories(dataNames[i])));
//             LOG.info("Uploaded Schema : {} ", uploadedSchema.toString(true));
//
//             // sort by selected fields (name/surname for all datasets)
//             LOG.info("Sorting and adding UID.");
//             ds.sortDataset(dataNames[i], "sorted_" + dataNames[i], 4, selectedFieldNames[i][0],selectedFieldNames[i][1]);
//             ds.addUIDToDataset("sorted_" + dataNames[i],"uid");
//
//             final Path downloadedPath = ds.downloadFiles("sorted_" + dataNames[i],"dl_sorted_" + dataNames[i],new Path("data"));
//             LOG.info("Downloaded to path : {} ",downloadedPath);
//         }
//     }
//
//     @Test
//     public void test3() throws Exception {
//         // calc stats
//         for(int i = 0 ; i < dataNames.length ; i++) {
//             if(i== 1) continue;
//             final Path[] paths = ds.retrieveDirectories("sorted_" + dataNames[i]);
//             final Path inputPath = paths[1];
//             final Path schemaPath = ds.retrieveSchemaPath(paths[2]);
//             final Schema schema = ds.loadSchema(schemaPath);
//
//             // run avg q gram count
//             final Path propertiesPath1 = ds.countAvgQgrams(
//                     inputPath, schemaPath, paths[0], "stats_report_1", selectedFieldNames[i]);
//             final DatasetStatistics statistics = ds.loadStats(propertiesPath1);
//
//             // get vector frequencies
//             final long recordCount = statistics.getRecordCount();
//             final int reducerCount = 4;
//             final SimilarityVectorFrequencies frequencies = ms.vectorFrequencies(
//                     inputPath, schemaPath, "uid"
//                     , recordCount, reducerCount,
//                     paths[0], "stats_report_2",
//                     selectedFieldNames[i]);
//
//             // run em estimation
//             ExpectationMaximization estimator = ms.newEMInstance(selectedFieldNames[i],
//                     initialMUP[i][0], initialMUP[i][1], initialMUP[i][2]);
//             estimator.runAlgorithm(frequencies);
//
//             // fill rest stats
//             statistics.setEmPairsCount(estimator.getPairCount());
//             statistics.setEmAlgorithmIterations(estimator.getIteration());
//             statistics.setP(estimator.getP());
//             DatasetStatistics.calculateStatsUsingEstimates(
//                     statistics, selectedFieldNames[i],
//                     estimator.getM(), estimator.getU());
//
//             LOG.info("Final stats report saved at : {}",ds.saveStats("stats_report",paths[0],statistics));
//         }
//     }
//
//
//     @Test
//     public void test4() throws Exception {
//         // Simple encoding schemes not stats involved
//         for (int i = 0; i < dataNames.length; i++) {
//             if (i == 1) continue;
//             final Path[] paths = ds.retrieveDirectories(dataNames[i]);
//             final Path inputPath = paths[1];
//             final Path schemaPath = ds.retrieveSchemaPath(paths[2]);
//             final Schema schema = ds.loadSchema(schemaPath);
//             {
//                 final String scheme = "CLK";
//                 final int fieldCount = selectedFieldNames[i].length;
//                 final int N = ENCODING_N;
//                 final int fbfN = 0;
//                 final double[] AQC = null;
//                 final double[] W = null;
//
//                 final BloomFilterEncoding encoding =
//                         BloomFilterEncodingUtil.instanceFactory(
//                                 scheme, fieldCount, N, fbfN, ENCODING_K, ENCODING_Q, AQC, W);
//                 encoding.makeFromSchema(schema, selectedFieldNames[i], includedFieldNames[i]);
//
//                 final Path[] encodedPaths = ds.createDirectories("clk_" + dataNames[i],
//                         DatasetsService.OTHERS_CAN_READ_PERMISSION);
//                 final Path eBasePath = encodedPaths[0];
//                 final Path eSchemaBasePath = encodedPaths[2];
//                 final Path outputPath = encodedPaths[1];
//                 final Path outputSchemaPath = new Path(eSchemaBasePath, "clk_" + dataNames[i] + ".avsc");
//
//                 ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());
//
//                 LOG.info("Encoding CLK : ");
//                 es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
//                 LOG.info("Encoding CLK : DONE at : {}", eBasePath);
//                 final Path downloadedPath = ds.downloadFiles(
//                         "clk_" + dataNames[i],
//                         "dl_" + "clk_" + dataNames[i],
//                         new Path("data"));
//                 LOG.info("Downloaded to path : {} ", downloadedPath);
//             }
//
//             {
//                 final String scheme = "FBF";
//                 final int fieldCount = selectedFieldNames[i].length;
//                 final int N = 0;
//                 final int fbfN = ENCODING_N;
//                 final double[] AQC = null;
//                 final double[] W = null;
//
//                 final BloomFilterEncoding encoding =
//                         BloomFilterEncodingUtil.instanceFactory(
//                                 scheme, fieldCount, N, fbfN,ENCODING_K, ENCODING_Q, AQC, W);
//                 encoding.makeFromSchema(schema, selectedFieldNames[i], includedFieldNames[i]);
//
//                 final Path[] encodedPaths = ds.createDirectories(
//                         "fbf_static_" + dataNames[i], DatasetsService.OTHERS_CAN_READ_PERMISSION);
//                 final Path eBasePath = encodedPaths[0];
//                 final Path eSchemaBasePath = encodedPaths[2];
//                 final Path outputPath = encodedPaths[1];
//                 final Path outputSchemaPath = new Path(eSchemaBasePath, "fbf_static_" + dataNames[i] + ".avsc");
//
//                 ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());
//
//                 LOG.info("Encoding FBF/Static : ");
//                 es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
//                 LOG.info("Encoding FBF/Static : DONE at : {}", eBasePath);
//                 final Path downloadedPath = ds.downloadFiles(
//                         "fbf_static_" + dataNames[i],
//                         "dl_" + "fbf_static_" + dataNames[i],
//                         new Path("data"));
//                 LOG.info("Downloaded to path : {} ", downloadedPath);
//             }
//
//             {
//                 final String scheme = "RBF";
//                 final int fieldCount = selectedFieldNames[i].length;
//                 final int N = ENCODING_N;
//                 final int fbfN = 512;
//                 final double[] AQC = null;
//                 final double[] W = null;
//
//                 final BloomFilterEncoding encoding =
//                         BloomFilterEncodingUtil.instanceFactory(
//                                 scheme, fieldCount, N, fbfN, ENCODING_K, ENCODING_Q, AQC, W);
//                 encoding.makeFromSchema(schema, selectedFieldNames[i], includedFieldNames[i]);
//
//                 final Path[] encodedPaths = ds.createDirectories(
//                         "rbf_uniform_fbf_static_" + dataNames[i], DatasetsService.OTHERS_CAN_READ_PERMISSION);
//                 final Path eBasePath = encodedPaths[0];
//                 final Path eSchemaBasePath = encodedPaths[2];
//                 final Path outputPath = encodedPaths[1];
//                 final Path outputSchemaPath = new Path(eSchemaBasePath, "rbf_uniform_fbf_static_" + dataNames[i] + ".avsc");
//
//                 ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());
//
//                 LOG.info("Encoding RBF/Uniform/FBF/Static : ");
//                 es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
//                 LOG.info("Encoding RBF/Uniform/FBF/Static : DONE at : {}", eBasePath);
//                 final Path downloadedPath = ds.downloadFiles(
//                         "rbf_uniform_fbf_static_" + dataNames[i],
//                         "dl_" + "rbf_uniform_fbf_static_" + dataNames[i],
//                         new Path("data"));
//                 LOG.info("Downloaded to path : {} ", downloadedPath);
//             }
//         }
//     }
//
//     @Test
//     public void test5() throws Exception {
//         // stat based encodings
//         for (int i = 0; i < dataNames.length; i++) {
//             if (i == 1) continue;
//             final Path[] paths = ds.retrieveDirectories(dataNames[i]);
//             final Path inputPath = paths[1];
//             final Path schemaPath = ds.retrieveSchemaPath(paths[2]);
//             final Schema schema = ds.loadSchema(schemaPath);
//
//             final DatasetStatistics statistics = ds.loadStats(
//                     new Path(ds.retrieveDirectories("sorted_"+dataNames[i])[0],
//                     "stats_report.properties"));
//             final double[] avgQGrams = new double[selectedFieldNames[i].length];
//             final double[] weights = new double[selectedFieldNames[i].length];
//             for (int j = 0; j < selectedFieldNames[i].length; j++) {
//                 avgQGrams[j] = statistics.getFieldStatistics().get(selectedFieldNames[i][j]).getQgramCount(ENCODING_Q);
//                 weights[j] = statistics.getFieldStatistics().get(selectedFieldNames[i][j]).getNormalizedRange();
//             }
//
//             {
//                 final String scheme = "FBF";
//                 final int fieldCount = selectedFieldNames[i].length;
//                 final int N = 0;
//                 final int fbfN = 0;
//                 final double[] AQC = avgQGrams;
//                 final double[] W = null;
//
//                 final BloomFilterEncoding encoding =
//                         BloomFilterEncodingUtil.instanceFactory(
//                                 scheme, fieldCount, N, fbfN, ENCODING_K, ENCODING_Q, AQC, W);
//                 encoding.makeFromSchema(schema, selectedFieldNames[i], includedFieldNames[i]);
//
//                 final Path[] encodedPaths = ds.createDirectories(
//                         "fbf_dynamic_" + dataNames[i], DatasetsService.OTHERS_CAN_READ_PERMISSION);
//                 final Path eBasePath = encodedPaths[0];
//                 final Path eSchemaBasePath = encodedPaths[2];
//                 final Path outputPath = encodedPaths[1];
//                 final Path outputSchemaPath = new Path(eSchemaBasePath, "fbf_dynamic_" + dataNames[i] + ".avsc");
//
//                 ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());
//
//                 LOG.info("Encoding FBF/Dynamic : ");
//                 es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
//                 LOG.info("Encoding FBF/Dynamic : DONE at : {}", eBasePath);
//                 final Path downloadedPath = ds.downloadFiles(
//                         "fbf_dynamic_" + dataNames[i],
//                         "dl_" + "fbf_dynamic_" + dataNames[i],
//                         new Path("data"));
//                 LOG.info("Downloaded to path : {} ", downloadedPath);
//             }
//
//             {
//                 final String scheme = "RBF";
//                 final int fieldCount = selectedFieldNames[i].length;
//                 final int N = ENCODING_N;
//                 final int fbfN = 0;
//                 final double[] AQC = avgQGrams;
//                 final double[] W = null;
//
//                 final BloomFilterEncoding encoding =
//                         BloomFilterEncodingUtil.instanceFactory(
//                                 scheme, fieldCount, N, fbfN, ENCODING_K, ENCODING_Q, AQC, W);
//                 encoding.makeFromSchema(schema, selectedFieldNames[i], includedFieldNames[i]);
//
//                 final Path[] encodedPaths = ds.createDirectories(
//                         "rbf_uniform_fbf_dynamic_" + dataNames[i], DatasetsService.OTHERS_CAN_READ_PERMISSION);
//                 final Path eBasePath = encodedPaths[0];
//                 final Path eSchemaBasePath = encodedPaths[2];
//                 final Path outputPath = encodedPaths[1];
//                 final Path outputSchemaPath = new Path(eSchemaBasePath, "rbf_uniform_fbf_dynamic_" + dataNames[i] + ".avsc");
//
//                 ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());
//
//                 LOG.info("Encoding RBF/Uniform/FBF/Dynamic : ");
//                 es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
//                 LOG.info("Encoding RBF/Uniform/FBF/Dynamic : DONE at : {}", eBasePath);
//                 final Path downloadedPath = ds.downloadFiles(
//                         "rbf_uniform_fbf_dynamic_" + dataNames[i],
//                         "dl_" + "rbf_uniform_fbf_dynamic_" + dataNames[i]
//                         , new Path("data"));
//                 LOG.info("Downloaded to path : {} ", downloadedPath);
//             }
//             {
//                 final String scheme = "RBF";
//                 final int fieldCount = selectedFieldNames[i].length;
//                 final int N = 0;
//                 final int fbfN = 512;
//                 final double[] AQC = null;
//                 final double[] W = weights;
//
//                 final BloomFilterEncoding encoding =
//                         BloomFilterEncodingUtil.instanceFactory(
//                                 scheme, fieldCount, N, fbfN, ENCODING_K, ENCODING_Q, AQC, W);
//                 encoding.makeFromSchema(schema, selectedFieldNames[i], includedFieldNames[i]);
//
//                 final Path[] encodedPaths = ds.createDirectories(
//                         "rbf_weighted_fbf_static_" + dataNames[i], DatasetsService.OTHERS_CAN_READ_PERMISSION);
//                 final Path eBasePath = encodedPaths[0];
//                 final Path eSchemaBasePath = encodedPaths[2];
//                 final Path outputPath = encodedPaths[1];
//                 final Path outputSchemaPath = new Path(eSchemaBasePath, "rbf_weighted_fbf_static_" + dataNames[i] + ".avsc");
//
//                 ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());
//
//                 LOG.info("Encoding RBF/Weighted/FBF/Static : ");
//                 es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
//                 LOG.info("Encoding RBF/Weighted/FBF/Static : DONE at : {}", eBasePath);
//                 final Path downloadedPath = ds.downloadFiles(
//                         "rbf_weighted_fbf_static_" + dataNames[i],
//                         "dl_" + "rbf_weighted_fbf_static_" + dataNames[i],
//                         new Path("data"));
//                 LOG.info("Downloaded to path : {} ", downloadedPath);
//
//             }
//
//             {
//                 final String scheme = "RBF";
//                 final int fieldCount = selectedFieldNames[i].length;
//                 final int N = 0;
//                 final int fbfN = 0;
//                 final double[] AQC = avgQGrams;
//                 final double[] W = weights;
//
//                 final BloomFilterEncoding encoding =
//                         BloomFilterEncodingUtil.instanceFactory(
//                                 scheme, fieldCount, N, fbfN, ENCODING_K, ENCODING_Q, AQC, W);
//                 encoding.makeFromSchema(schema, selectedFieldNames[i], includedFieldNames[i]);
//
//                 final Path[] encodedPaths = ds.createDirectories(
//                         "rbf_weighted_fbf_dynamic_" + dataNames[i], DatasetsService.OTHERS_CAN_READ_PERMISSION);
//                 final Path eBasePath = encodedPaths[0];
//                 final Path eSchemaBasePath = encodedPaths[2];
//                 final Path outputPath = encodedPaths[1];
//                 final Path outputSchemaPath = new Path(eSchemaBasePath, "rbf_weighted_fbf_dynamic_" + dataNames[i] + ".avsc");
//
//                 ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());
//
//                 LOG.info("Encoding RBF/Weighted/FBF/Dynamic : ");
//                 es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
//                 LOG.info("Encoding RBF/Weighted/FBF/Dynamic : DONE at : {}", eBasePath);
//                 final Path downloadedPath = ds.downloadFiles(
//                         "rbf_weighted_fbf_dynamic_" + dataNames[i],
//                         "dl_" + "rbf_weighted_fbf_dynamic_" + dataNames[i],
//                         new Path("data"));
//                 LOG.info("Downloaded to path : {} ", downloadedPath);
//             }
//         }
//     }
//
//     @Test
//     public void test6() throws Exception {
//         final String[] encNames = {
//                 "clk_",
//                 "fbf_static_",
//                 "fbf_dynamic_",
//                 "rbf_uniform_fbf_static_",
//                 "rbf_uniform_fbf_dynamic_",
//                 "rbf_weighted_fbf_static_",
//                 "rbf_weighted_fbf_dynamic_"
//         };
//         final Path[] alicePaths = ds.retrieveDirectories("voters_a");
//         final Path aliceAvroPath = alicePaths[1];
//         final Path aliceSchemaPath = ds.retrieveSchemaPath(alicePaths[2]);
//         final Schema aliceSchema = ds.loadSchema(aliceSchemaPath);
//         for (String encName : encNames) {
//             final Path[] bobEncodingPaths = ds.retrieveDirectories(encName + "voters_b");
//             final Schema bobEncodingSchema = ds.loadSchema(ds.retrieveSchemaPath(bobEncodingPaths[2]));
//
//             final String schemeName = BloomFilterEncodingUtil.retrieveSchemeName(bobEncodingSchema);
//             final BloomFilterEncoding encoding =
//                     BloomFilterEncodingUtil.newInstance(schemeName);
//             encoding.setupFromSchema(
//                     BloomFilterEncodingUtil.basedOnExistingSchema(
//                             aliceSchema, selectedFieldNames[1], includedFieldNames[1],
//                             bobEncodingSchema, selectedFieldNames[2])
//             );
//
//             final Path[] aliceEncodingPaths =
//                     ds.createDirectories(encName + "voters_a", DatasetsService.OTHERS_CAN_READ_PERMISSION);
//             final Path aliceEncodingAvroPath = aliceEncodingPaths[1];
//             final Path aliceEncodingSchemaPath = new Path(aliceEncodingPaths[2],encName + "voters_a.avsc");
//             ds.saveSchema(aliceEncodingSchemaPath,encoding.getEncodingSchema());
//             LOG.info("Encoding Scheme : {}", schemeName);
//             es.runEncodeDatasetTool(aliceAvroPath, aliceSchemaPath, aliceEncodingAvroPath, aliceEncodingSchemaPath);
//             LOG.info("Encoding Scheme : {}. DONE at : {}.", schemeName, aliceEncodingPaths[0]);
//             final Path downloadedPath = ds.downloadFiles(
//                     encName + "voters_a",
//                     "dl_" + encName + "voters_a",
//                     new Path("data"));
//             LOG.info("Downloaded to path : {} ", downloadedPath);
//         }
//     }
//
//    @Test
//    public void test7() throws Exception {
//        final String[] encNames = {
//                "clk_",
//                "fbf_static_",
//                "fbf_dynamic_",
//                "rbf_uniform_fbf_static_",
//                "rbf_uniform_fbf_dynamic_",
//                "rbf_weighted_fbf_static_",
//                "rbf_weighted_fbf_dynamic_"
//        };
//
//        for (String encName : encNames) {
//            final String aliceName = encName + "voters_a";
//            final String bobName = encName + "voters_b";
//
//            final Path[] alicePaths = ds.retrieveDirectories(aliceName);
//            final Path aliceAvroPath = alicePaths[1];
//            final Path aliceSchemaPath = ds.retrieveSchemaPath(alicePaths[2]);
//            final String aliceUidFieldName = uidFieldNames[1];
//
//            final Path[] bobPaths = ds.retrieveDirectories(bobName);
//            final Path bobAvroPath = bobPaths[1];
//            final Path bobSchemaPath = ds.retrieveSchemaPath(bobPaths[2]);
//            final String bobUidFieldName = uidFieldNames[2];
//
//           final String blockingName1 = String.format("blocking.%s.%s.%s.%s",
//                   "HLSH_MR".toLowerCase(),
//                   (new SimpleDateFormat("yyyy.MM.dd.hh.mm")).format(new Date()),
//                   aliceName,bobName
//           );
//
//           bs.runHammingLSHFPSBlockingV0ToolRuner(
//                   aliceAvroPath, aliceSchemaPath, aliceUidFieldName,
//                   bobAvroPath, bobSchemaPath, bobUidFieldName,
//                   blockingName1,
//                   HLSH_BLOCKING_L, HLSH_BLOCKING_K, HLSH_BLOCKING_C,
//                   HAMMING_THRESHOLD,
//                   4, 4, 4,420
//           );
//
//            final String blockingName2 = String.format("blocking.%s.%s.%s.%s",
//                    "HLSH_FPS_MR".toLowerCase(),
//                    (new SimpleDateFormat("yyyy.MM.dd.hh.mm")).format(new Date()),
//                    aliceName,bobName
//            );
//
//            bs.runHammingLSHFPSBlockingV1ToolRuner(
//                    aliceAvroPath, aliceSchemaPath, aliceUidFieldName,
//                    bobAvroPath, bobSchemaPath, bobUidFieldName,
//                    blockingName2,
//                    HLSH_BLOCKING_L, HLSH_BLOCKING_K, HLSH_BLOCKING_C,
//                    HAMMING_THRESHOLD,
//                    4, 4, 4,420
//            );
//
//
//            final String blockingName3 = String.format("blocking.%s.%s.%s.%s",
//                    "HLSH_FPS_MR_V1".toLowerCase(),
//                    (new SimpleDateFormat("yyyy.MM.dd.hh.mm")).format(new Date()),
//                    aliceName,bobName
//            );
//            bs.runHammingLSHFPSBlockingV2ToolRuner(
//                    aliceAvroPath, aliceSchemaPath, aliceUidFieldName,
//                    bobAvroPath, bobSchemaPath, bobUidFieldName,
//                    blockingName3,
//                    HLSH_BLOCKING_L, HLSH_BLOCKING_K, HLSH_BLOCKING_C,
//                    HAMMING_THRESHOLD,
//                    4, 4,420
//            );
//
//        }
//    }
//
//    @Test
//    public void test8() throws IOException, DatasetException {
//        final FileSystem hdfs = getFileSystem();
//        final Path basePPRLpath = new Path(hdfs.getHomeDirectory(), "pprl");
//        boolean baseExists = hdfs.exists(basePPRLpath);
//        assertTrue(baseExists);
//        LOG.info("HDFS PPRL SITE : {}", basePPRLpath);
//        RemoteIterator<LocatedFileStatus> iterator = hdfs.listFiles(basePPRLpath, true);
//        while(iterator.hasNext()) {
//            LocatedFileStatus lfs = iterator.next();
//            if(lfs.isFile() && !lfs.getPath().getName().startsWith(".")) {
//                LOG.info("File {} with size : {} bytes",lfs.getPath(),lfs.getLen());
//                if(lfs.getPath().toString().contains("blocking") && lfs.getPath().toString().endsWith("stats")) {
//                    final Path dstBase = new Path("data",lfs.getPath().getParent().getName());
//                    hdfs.copyToLocalFile(lfs.getPath(),new Path(dstBase,lfs.getPath().getName()));
//                }
//            }
//        }
//    }
}