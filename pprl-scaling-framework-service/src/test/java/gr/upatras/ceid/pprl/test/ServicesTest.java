package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import gr.upatras.ceid.pprl.service.datasets.DatasetsService;
import gr.upatras.ceid.pprl.service.encoding.EncodingService;
import gr.upatras.ceid.pprl.service.matching.MatchingService;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractMapReduceTests;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

@ContextConfiguration(locations = "classpath:services-test-context.xml", loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster(nodes = 1, id = "service_test")
public class ServicesTest extends AbstractMapReduceTests {
    private static Logger LOG = LoggerFactory.getLogger(ServicesTest.class);

    @Autowired
    private DatasetsService ds;

    @Autowired
    private EncodingService es;

    @Autowired
    private MatchingService ms;

    @Test
    public void test0() throws IOException {
        final FileSystem hdfs = getFileSystem();
        final Path basePPRLpath = new Path(hdfs.getHomeDirectory(), "pprl");
        boolean baseExists = hdfs.exists(basePPRLpath);
        assertTrue(baseExists);
        LOG.info("HDFS PPRL SITE : {}", new Path(hdfs.getHomeDirectory(), "pprl"));
    }

    @Test
    public void test1() throws Exception {
        final Path[] avroPaths = new Path[]{new Path("data/person_small/avro")};
        final Path schemaPath = new Path("data/person_small/schema/person_small.avsc");
        final Path uploadedPath = ds.uploadFiles(avroPaths, schemaPath, "person_small");
        final FileSystem hdfs = getFileSystem();
        boolean uploadExists = hdfs.exists(uploadedPath);
        assertTrue(uploadExists);
        LOG.info("Uploaded to path : {} ", uploadedPath);
        final Path uploadedSchemaPath = new Path(uploadedPath, "schema");
        final Schema uploadedSchema = ds.loadSchema(
                ds.retrieveSchemaPath(uploadedSchemaPath)
        );
        LOG.info("Uploaded paths : {}", Arrays.toString(ds.retrieveDirectories("person_small")));
        LOG.info("Uploaded Schema : {} ", uploadedSchema.toString(true));

        LOG.info("Sorting and adding UID.");
        ds.sortDataset("person_small", "sorted_person_small", 4, "name", "location");
        ds.addUIDToDataset("sorted_person_small","uid");

        final Path downloadedPath = ds.downloadFiles("sorted_person_small","person_small_downloaded",new Path("data"));
        LOG.info("Downloaded to path : {} ",downloadedPath);
    }

    @Test
    public void test2() throws Exception {
        final Path xmlPath = new Path("data/dblp/xml/dblp.xml");
        final Path uploadedPath = ds.importDblpXmlDataset(xmlPath,"dblp");
        final FileSystem hdfs = getFileSystem();
        boolean uploadExists =  hdfs.exists(uploadedPath);
        assertTrue(uploadExists);
        LOG.info("Uploaded to path : {} ",uploadedPath);
        final Path uploadedSchemaPath = new Path(uploadedPath,"schema");
        final Schema uploadedSchema = ds.loadSchema(
                ds.retrieveSchemaPath(uploadedSchemaPath)
        );
        LOG.info("Uploaded paths : {}", Arrays.toString(ds.retrieveDirectories("dblp")));
        LOG.info("Uploaded Schema : {} ",uploadedSchema.toString(true));

        final Path downloadedPath = ds.downloadFiles("dblp","dblp_downloaded",new Path("data"));
        LOG.info("Downloaded to path : {} ",downloadedPath);
    }

    @Test
    public void test3() throws Exception {
        // Encoding for various schemes
        final Path[] paths = ds.retrieveDirectories("sorted_person_small");
        final Path inputPath = paths[1];
        final Path schemaPath = ds.retrieveSchemaPath(paths[2]);
        final Schema schema = ds.loadSchema(schemaPath);
        final Path statsBasePath = new Path(paths[0], "stats");
        final String[] selected = new String[]{"surname", "location"};
        final String[] included = new String[]{"uid", "name", "id"};

        int recordCount = 120;
        int reducerCount = 2;

        // calc avg q grams per field
        final Path propertiesPath1 = ds.countAvgQgrams(
                inputPath, schemaPath, statsBasePath, "stats_report_1", selected);
        final DatasetStatistics statistics = ds.loadStats(propertiesPath1);


        // similarity frequencies
        final SimilarityVectorFrequencies frequencies = ms.vectorFrequencies(
                inputPath, schemaPath, "uid", recordCount, reducerCount, statsBasePath, "stats_report_2", selected);

        // estimate m u
        final double m0 = 0.9;
        final double u0 = 0.1;
        final double p0 = 0.2;
        ExpectationMaximization estimator = ms.newEMInstance(selected, m0, u0, p0);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics, selected,
                estimator.getM(), estimator.getU());

        // present stats
        LOG.info("Stats : " + DatasetStatistics.prettyStats(statistics));

        final int K = 10;
        final int Q = 2;
        {
            final String scheme = "CLK";
            final int fieldCount = selected.length;
            final int N = 1024;
            final int fbfN = 0;
            final double[] AQC = null;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selected, included);

            final Path[] encodedPaths = ds.createDirectories(
                    "clk_person_small", DatasetsService.OTHERS_CAN_READ_PERMISSION);
            final Path eBasePath = encodedPaths[0];
            final Path eSchemaBasePath = encodedPaths[2];
            final Path outputPath = encodedPaths[1];
            final Path outputSchemaPath = new Path(eSchemaBasePath, "clk_person_small.avsc");

            ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());

            LOG.info("Encoding CLK : ");
            es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
            LOG.info("Encoding CLK : DONE at : {}", eBasePath);
            LOG.info(DatasetsUtil.prettyRecords(
                    DatasetsUtil.loadAvroRecordsFromFSPaths(
                            getFileSystem(),
                            encoding.getEncodingSchema(),
                            outputPath), encoding.getEncodingSchema()));
            final Path downloadedPath = ds.downloadFiles("clk_person_small","clk_person_small",new Path("data"));
            LOG.info("Downloaded to path : {} ",downloadedPath);
        }
        {
            final String scheme = "FBF";
            final int fieldCount = selected.length;
            final int N = 0;
            final int fbfN = 512;
            final double[] AQC = null;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selected, included);

            final Path[] encodedPaths = ds.createDirectories(
                    "fbf_static_person_small", DatasetsService.OTHERS_CAN_READ_PERMISSION);
            final Path eBasePath = encodedPaths[0];
            final Path eSchemaBasePath = encodedPaths[2];
            final Path outputPath = encodedPaths[1];
            final Path outputSchemaPath = new Path(eSchemaBasePath, "fbf_static_person_small.avsc");

            ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());

            LOG.info("Encoding FBF/Static : ");
            es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
            LOG.info("Encoding FBF/Static : DONE at : {}", eBasePath);
            LOG.info(DatasetsUtil.prettyRecords(
                    DatasetsUtil.loadAvroRecordsFromFSPaths(
                            getFileSystem(),
                            encoding.getEncodingSchema(),
                            outputPath), encoding.getEncodingSchema()));
            final Path downloadedPath = ds.downloadFiles("fbf_static_person_small","fbf_static_person_small",new Path("data"));
            LOG.info("Downloaded to path : {} ",downloadedPath);
        }

        {
            final String scheme = "RBF";
            final int fieldCount = selected.length;
            final int N = 1024;
            final int fbfN = 512;
            final double[] AQC = null;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selected, included);

            final Path[] encodedPaths = ds.createDirectories(
                    "rbf_u_fbf_static_person_small", DatasetsService.OTHERS_CAN_READ_PERMISSION);
            final Path eBasePath = encodedPaths[0];
            final Path eSchemaBasePath = encodedPaths[2];
            final Path outputPath = encodedPaths[1];
            final Path outputSchemaPath = new Path(eSchemaBasePath, "rbf_u_fbf_static_person_small.avsc");

            ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());

            LOG.info("Encoding RBF/Uniform/FBF/Static : ");
            es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
            LOG.info("Encoding RBF/Uniform/FBF/Static : DONE at : {}", eBasePath);
            LOG.info(DatasetsUtil.prettyRecords(
                    DatasetsUtil.loadAvroRecordsFromFSPaths(
                            getFileSystem(),
                            encoding.getEncodingSchema(),
                            outputPath), encoding.getEncodingSchema()));
            final Path downloadedPath = ds.downloadFiles("rbf_u_fbf_static_person_small","rbf_u_fbf_static_person_small",new Path("data"));
            LOG.info("Downloaded to path : {} ",downloadedPath);
        }

        final double[] avgQGrams = new double[]{
                statistics.getFieldStatistics().get(selected[0]).getQgramCount(Q),
                statistics.getFieldStatistics().get(selected[1]).getQgramCount(Q)
        };
        final double[] weights = new double[] {
                statistics.getFieldStatistics().get(selected[0]).getNormalizedRange(),
                statistics.getFieldStatistics().get(selected[1]).getNormalizedRange()
        };

        {
            final String scheme = "FBF";
            final int fieldCount = selected.length;
            final int N = 0;
            final int fbfN = 0;
            final double[] AQC = avgQGrams;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selected, included);

            final Path[] encodedPaths = ds.createDirectories(
                    "fbf_dynamic_person_small", DatasetsService.OTHERS_CAN_READ_PERMISSION);
            final Path eBasePath = encodedPaths[0];
            final Path eSchemaBasePath = encodedPaths[2];
            final Path outputPath = encodedPaths[1];
            final Path outputSchemaPath = new Path(eSchemaBasePath, "fbf_dynamic_person_small.avsc");

            ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());

            LOG.info("Encoding FBF/Dynamic : ");
            es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
            LOG.info("Encoding FBF/Dynamic : DONE at : {}", eBasePath);
            LOG.info(DatasetsUtil.prettyRecords(
                    DatasetsUtil.loadAvroRecordsFromFSPaths(
                            getFileSystem(),
                            encoding.getEncodingSchema(),
                            outputPath), encoding.getEncodingSchema()));
            final Path downloadedPath = ds.downloadFiles("fbf_dynamic_person_small","fbf_dynamic_person_small",new Path("data"));
            LOG.info("Downloaded to path : {} ",downloadedPath);
        }

        {
            final String scheme = "RBF";
            final int fieldCount = selected.length;
            final int N = 1024;
            final int fbfN = 0;
            final double[] AQC = avgQGrams;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selected, included);

            final Path[] encodedPaths = ds.createDirectories(
                    "rbf_u_fbf_dynamic_person_small", DatasetsService.OTHERS_CAN_READ_PERMISSION);
            final Path eBasePath = encodedPaths[0];
            final Path eSchemaBasePath = encodedPaths[2];
            final Path outputPath = encodedPaths[1];
            final Path outputSchemaPath = new Path(eSchemaBasePath, "rbf_u_fbf_dynamic_person_small.avsc");

            ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());

            LOG.info("Encoding RBF/Uniform/FBF/Dynamic : ");
            es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
            LOG.info("Encoding RBF/Uniform/FBF/Dynamic : DONE at : {}", eBasePath);
            LOG.info(DatasetsUtil.prettyRecords(
                    DatasetsUtil.loadAvroRecordsFromFSPaths(
                            getFileSystem(),
                            encoding.getEncodingSchema(),
                            outputPath), encoding.getEncodingSchema()));
            final Path downloadedPath = ds.downloadFiles("rbf_u_fbf_dynamic_person_small","rbf_u_fbf_dynamic_person_small",new Path("data"));
            LOG.info("Downloaded to path : {} ",downloadedPath);
        }

        {
            final String scheme = "RBF";
            final int fieldCount = selected.length;
            final int N = 0;
            final int fbfN = 512;
            final double[] AQC = null;
            final double[] W = weights;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selected, included);

            final Path[] encodedPaths = ds.createDirectories(
                    "rbf_w_fbf_static_person_small", DatasetsService.OTHERS_CAN_READ_PERMISSION);
            final Path eBasePath = encodedPaths[0];
            final Path eSchemaBasePath = encodedPaths[2];
            final Path outputPath = encodedPaths[1];
            final Path outputSchemaPath = new Path(eSchemaBasePath, "rbf_w_fbf_static_person_small.avsc");

            ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());

            LOG.info("Encoding RBF/Weighted/FBF/Static : ");
            es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
            LOG.info("Encoding RBF/Weighted/FBF/Static : DONE at : {}", eBasePath);
            LOG.info(DatasetsUtil.prettyRecords(
                    DatasetsUtil.loadAvroRecordsFromFSPaths(
                            getFileSystem(),
                            encoding.getEncodingSchema(),
                            outputPath), encoding.getEncodingSchema()));
        }

        {
            final String scheme = "RBF";
            final int fieldCount = selected.length;
            final int N = 0;
            final int fbfN = 0;
            final double[] AQC = avgQGrams;
            final double[] W = weights;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selected, included);

            final Path[] encodedPaths = ds.createDirectories(
                    "rbf_w_fbf_dynamic_person_small", DatasetsService.OTHERS_CAN_READ_PERMISSION);
            final Path eBasePath = encodedPaths[0];
            final Path eSchemaBasePath = encodedPaths[2];
            final Path outputPath = encodedPaths[1];
            final Path outputSchemaPath = new Path(eSchemaBasePath, "rbf_w_fbf_dynamic_person_small.avsc");

            ds.saveSchema(outputSchemaPath, encoding.getEncodingSchema());

            LOG.info("Encoding RBF/Weighted/FBF/Dynamic : ");
            es.runEncodeDatasetTool(inputPath, schemaPath, outputPath, outputSchemaPath);
            LOG.info("Encoding RBF/Weighted/FBF/Dynamic : DONE at : {}", eBasePath);
            LOG.info(DatasetsUtil.prettyRecords(
                    DatasetsUtil.loadAvroRecordsFromFSPaths(
                            getFileSystem(),
                            encoding.getEncodingSchema(),
                            outputPath), encoding.getEncodingSchema()));
            final Path downloadedPath = ds.downloadFiles("rbf_w_fbf_dynamic_person_small","rbf_w_fbf_dynamic_person_small",new Path("data"));
            LOG.info("Downloaded to path : {} ",downloadedPath);
        }
    }
}