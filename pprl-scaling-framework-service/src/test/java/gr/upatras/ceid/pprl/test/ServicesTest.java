package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
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
import org.apache.hadoop.mapreduce.v2.app.speculate.DataStatistics;
import org.junit.Ignore;
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
        boolean baseExists =  hdfs.exists(basePPRLpath);
        assertTrue(baseExists);
        LOG.info("HDFS PPRL SITE : {}", new Path(hdfs.getHomeDirectory(), "pprl"));
    }

    @Test
    public void test1() throws IOException, DatasetException {
        final Path[] avroPaths = new Path[]{new Path("data/person_small_3/avro")};
        final Path schemaPath = new Path("data/person_small_3/schema/person_small_3.avsc");
        final Path uploadedPath = ds.uploadFiles(avroPaths, schemaPath, "person_small");
        final FileSystem hdfs = getFileSystem();
        boolean uploadExists =  hdfs.exists(uploadedPath);
        assertTrue(uploadExists);
        LOG.info("Uploaded to path : {} ",uploadedPath);
        final Path uploadedSchemaPath = new Path(uploadedPath,"schema");
        final Schema uploadedSchema = ds.loadSchema(
                ds.retrieveSchemaPath(uploadedSchemaPath)
        );
        LOG.info("Uploaded paths : {}", Arrays.toString(ds.retrieveDirectories("person_small")));
        LOG.info("Uploaded Schema : {} ",uploadedSchema.toString(true));

    }

    @Test
    public void test2() throws IOException, DatasetException {
        final Path downloadedPath = ds.downloadFiles("person_small","person_small_downloaded",new Path("data"));
        LOG.info("Downloaded to path : {} ",downloadedPath);
    }

    @Test
    public void test3() throws Exception {
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
    }

    @Test
    public void test4() throws Exception {
        final Path[] paths = ds.retrieveDirectories("person_small");
        final Path inputPath = paths[1];
        final Path schemaPath = ds.retrieveSchemaPath(paths[2]);
        final Path statsBasePath = new Path(paths[0],"stats");
        final String[] fieldNames = new String[]{"surname","location"};
        int recordCount = 120;
        int reducerCount = 2;

        // calc avg q grams per field
        final Path propertiesPath1 = ds.countAvgQgrams(
                inputPath, schemaPath, statsBasePath, "stats_report_1", fieldNames);
        final DatasetStatistics statistics = ds.loadStats(propertiesPath1);


        // similarity frequencies
        final SimilarityVectorFrequencies frequencies = ms.vectorFrequencies(
                inputPath, schemaPath, "uid", recordCount, reducerCount, statsBasePath, "stats_report_2", fieldNames);

        // estimate m u
        final double m0 = 0.9;
        final double u0 = 0.1;
        final double p0 = 0.2;
        ExpectationMaximization estimator = ms.newEMInstance(fieldNames,m0,u0,p0);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics,fieldNames,
                estimator.getM(),estimator.getU());

        // present stats
        LOG.info("Stats : " + DatasetStatistics.prettyStats(statistics));

        // Encoding for various schemes
//        final int K = 10;
//        final int Q = 2;
//        final double[] avgQGrams = new double[]{
//                statistics.getFieldStatistics().get(fieldNames[0]).getQgramCount(Q),
//                statistics.getFieldStatistics().get(fieldNames[1]).getQgramCount(Q)
//        };
//        final double[] weights = new double[] {
//                statistics.getFieldStatistics().get(fieldNames[0]).getNormalizedRange(),
//                statistics.getFieldStatistics().get(fieldNames[1]).getNormalizedRange()
//        };
    }
}