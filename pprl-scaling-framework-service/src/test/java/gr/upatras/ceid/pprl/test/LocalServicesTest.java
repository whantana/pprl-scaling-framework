package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import gr.upatras.ceid.pprl.service.blocking.LocalBlockingService;
import gr.upatras.ceid.pprl.service.datasets.DatasetsService;
import gr.upatras.ceid.pprl.service.datasets.LocalDatasetsService;
import gr.upatras.ceid.pprl.service.encoding.LocalEncodingService;
import gr.upatras.ceid.pprl.service.matching.LocalMatchingService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Arrays;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:local-services-test-context.xml")
public class LocalServicesTest {

    private static final Logger LOG = LoggerFactory.getLogger(LocalServicesTest.class);

    private static final Path dataRepo = new Path("data");
    private static final String[] dataNames = {
            "person_small",
            "voters_a",
            "voters_b"
    };
    private static final String[][] selectedFieldNames = new String[][]{
            {"name", "surname"},
            {"surname", "name", "address", "city"},
            {"surname", "name", "address", "city"}
    };
    private static final String[][] includedFieldNames = new String[][]{
            {"id", "location"},
            {"id"},
            {"id"}
    };
    private static final String[] uidFieldNames = new String[]{
            "id",
            "id",
            "id"
    };

    private static final double[][] initialMUP = new double[][]{
            {0.9, 0.1, 0.1},
            {0.9, 0.1, 0.1},
            {0.9, 0.1, 0.1},
    };

    private static final int ENCODING_N = 1024;
    private static final int ENCODING_K = 10;
    private static final int ENCODING_Q = 2;

    private static final int HLSH_BLOCKING_L = 10;
    private static final int HLSH_BLOCKING_K = 5;
    private static final short HLSH_BLOCKING_C = 2;

    private static final String SIMILARITY_METHOD_NAME = "hamming";
    private static final double SIMILARITY_THRESHOLD = 100;


    private static final Path[] datasetsBase;
    private static final Path[] datasetsAvro;
    private static final Path[] datasetsSchema;
    private static final Path[] datasetsStats;
    static {
        datasetsBase = new Path[dataNames.length];

        for (int i = 0; i < dataNames.length; i++) {
            datasetsBase[i] = new Path(dataRepo, dataNames[i]);
        }
        datasetsAvro = new Path[datasetsBase.length];
        datasetsSchema = new Path[datasetsBase.length];
        datasetsStats = new Path[datasetsBase.length];
        for (int i = 0; i < dataNames.length; i++) {
            datasetsAvro[i] = new Path(datasetsBase[i], "avro");
            datasetsSchema[i] =
                    new Path(new Path(datasetsBase[i], "schema"), dataNames[i] + ".avsc");
            datasetsStats[i] = new Path(datasetsBase[i], "stats_report.properties");
        }
    }

    @Autowired
    private LocalDatasetsService ds;

    @Autowired
    private LocalEncodingService es;

    @Autowired
    private LocalMatchingService ms;

    @Autowired
    private LocalBlockingService bs;

    @Test
    public void test00() throws IOException, DatasetException {
        // Schema description.
        for (int i = 0; i < datasetsSchema.length; i++) {
            final Schema schema = ds.loadSchema(datasetsSchema[i]);
            LOG.info("Schema of {}", dataNames[i]);
            LOG.info(schema.toString(true));
        }
    }

    public void test01() throws IOException, DatasetException {
        // Load records
        for (int i = 0; i < dataNames.length; i++) {
            GenericRecord[] records = ds.loadDatasetRecords(new Path[]{datasetsAvro[i]}, datasetsSchema[i]);
            LOG.info("Loaded {} records from {}.", records.length, dataNames[i]);
        }
    }

    @Test
    public void test02() throws IOException, DatasetException {
        // Take sample
        for (int i = 0; i < dataNames.length; i++) {
            final Schema schema = ds.loadSchema(datasetsSchema[i]);
            GenericRecord[] records = ds.sampleDataset(new Path[]{datasetsAvro[i]}, schema, 6);
            LOG.info("Sample {} records from {}.", records.length, dataNames[i]);
        }
    }

    @Test
    public void test03() throws IOException, DatasetException {
        // partial read of multiple files
        Path schemaPath = new Path("data/da_int/schema/da_int.avsc");
        Path[] avroPaths;

        avroPaths = new Path[]{
                new Path("data/da_int/avro/da_int_1.avro"),
                new Path("data/da_int/avro/da_int_2.avro")
        };
        LOG.info("Loading from files [{}] : {} records ",
                Arrays.toString(avroPaths),
                ds.loadDatasetRecords(avroPaths, schemaPath).length);

        avroPaths = new Path[]{
                new Path("data/da_int/avro/da_int_1.avro"),
                new Path("data/da_int/avro/da_int_2.avro"),
                new Path("data/da_int/avro/da_int_3.avro")
        };
        LOG.info("Loading from files [{}] : {} records ",
                Arrays.toString(avroPaths),
                ds.loadDatasetRecords(avroPaths, schemaPath).length);

        avroPaths = new Path[]{
                new Path("data/da_int/avro")
        };
        LOG.info("Loading from files [{}] : {} records ",
                Arrays.toString(avroPaths),
                ds.loadDatasetRecords(avroPaths, schemaPath).length);
    }

    @Test
    public void test04() throws DatasetException, IOException {

        // update schema and records
        final Path avroPath = new Path("data/person_small/avro");
        final Path schemaPath = new Path("data/person_small/schema/person_small.avsc");
        final GenericRecord[] records = ds.loadDatasetRecords(new Path[]{avroPath}, schemaPath);
        final Schema schema = ds.loadSchema(schemaPath);
        final GenericRecord[] sample = ds.sampleDataset(new Path[]{avroPath}, schema, 6);
        LOG.info(DatasetsUtil.prettyRecords(sample, schema));

        // sort by location,name, save as person_small_1
        final Schema updatedSchema1 =
                ds.sortDatasetSchema(schema, false, null, "location", "name");
        final GenericRecord[] updatedRecords1 =
                ds.sortDatasetRecords(records, schema, false, null, "location", "name");
        Path[] paths = ds.createDirectories("person_small_1", dataRepo);
        ds.saveSchema("person_small_1", paths[2], updatedSchema1);
        ds.saveDatasetRecords("person_small_1", updatedRecords1, updatedSchema1, paths[1]);


        // sort by surname add uiid and save as person_small_2
        final Schema updatedSchema2 =
                ds.sortDatasetSchema(schema, true, "uid", "surname");
        final GenericRecord[] updatedRecords2 =
                ds.sortDatasetRecords(records, schema, true, "uid", "surname");
        paths = ds.createDirectories("person_small_2", dataRepo);
        ds.saveSchema("person_small_2", paths[2], updatedSchema2);
        ds.saveDatasetRecords("person_small_2", updatedRecords2, updatedSchema2, paths[1]);

        // sort by name,surname,location, add uiid and save as person_small
        final Schema updatedSchema3 =
                ds.sortDatasetSchema(schema, true, "uid", "name", "surname", "location");
        final GenericRecord[] updatedRecords3 =
                ds.sortDatasetRecords(records, schema, true, "uid", "name", "surname", "location");
        paths = ds.createDirectories("person_small_3", dataRepo);
        ds.saveSchema("person_small_3", paths[2], updatedSchema3);
        ds.saveDatasetRecords("person_small_3", updatedRecords3, updatedSchema3, paths[1]);
        final GenericRecord[] sample3 = ds.sampleDataset(new Path[]{avroPath}, schemaPath, 6);
        LOG.info(DatasetsUtil.prettyRecords(sample3, updatedSchema3));
    }

    @Test
    public void test05() throws IOException, DatasetException {
        // Statistics from person small
        final Path avroPath = new Path("data/person_small_3/avro");
        final Path schemaPath = new Path("data/person_small_3/schema/person_small_3.avsc");
        final String[] fieldNames = new String[]{"name", "surname", "location"};
        final double m0 = 0.9;
        final double u0 = 0.1;
        final double p0 = 0.2;

        final Schema schema = ds.loadSchema(schemaPath);
        final GenericRecord[] records = ds.loadDatasetRecords(new Path[]{avroPath}, schema);

        // setup stats
        final DatasetStatistics statistics = new DatasetStatistics();

        // record count and field names
        statistics.setRecordCount(records.length);
        statistics.setFieldNames(fieldNames);

        // calculate average q-grams field names
        DatasetStatistics.calculateQgramStatistics(records, schema, statistics, fieldNames);

        // estimate m u
        SimilarityVectorFrequencies frequencies = ms.vectorFrequencies(records, fieldNames);
        ExpectationMaximization estimator = ms.newEMInstance(fieldNames, m0, u0, p0);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics, fieldNames,
                estimator.getM(), estimator.getU());

        // save stats report
        final String reportName = "stats_report";
        ds.saveStats(reportName, statistics, new Path("data/person_small_3"));
    }

    @Test
    public void test06() throws DatasetException, BloomFilterEncodingException, IOException {
        // run simple encodings on the datasets
        for (int i = 0; i < dataNames.length; i++) {
            if (i == 1) continue; // skip voters_a
            simpleEncodings(dataNames[i], datasetsAvro[i], datasetsSchema[i],
                    selectedFieldNames[i], includedFieldNames[i]);
        }
    }

    @Test
    public void test07() throws IOException, DatasetException {
        // calculate statistics
        for (int i = 0; i < dataNames.length; i++) {
            if (i == 1) continue; // skip voters_a
            calcStatisticsAndStoreThem(datasetsAvro[i], datasetsSchema[i],
                    datasetsStats[i], initialMUP[i], selectedFieldNames[i]);
        }
    }

    @Test
    public void test08() throws DatasetException, BloomFilterEncodingException, IOException {
        for (int i = 0; i < dataNames.length; i++) {
            if (i == 1) continue; // skip voters_a
            statBasedEncodings(
                    dataNames[i],
                    datasetsAvro[i], datasetsSchema[i],
                    datasetsStats[i],
                    selectedFieldNames[i], includedFieldNames[i]);
        }
    }

    @Test
    public void test09() throws IOException, DatasetException, BloomFilterEncodingException {
        final String[] encNames = {
                "clk_",
                "fbf_static_",
                "fbf_dynamic_",
                "rbf_uniform_fbf_static_",
                "rbf_uniform_fbf_dynamic_",
                "rbf_weighted_fbf_static_",
                "rbf_weighted_fbf_dynamic_"
        };

        final Schema schema = ds.loadSchema(datasetsSchema[1]);
        final GenericRecord[] records = ds.loadDatasetRecords(new Path[]{datasetsAvro[1]}, schema);


        for(String encName : encNames) {
            final String name = encName + "voters_a";

            final Path existingEncodingSchemaPath =
                    new Path(dataRepo,encName + "voters_b/schema/" + encName + "voters_b.avsc");
            final Schema existingEncodingSchema = ds.loadSchema(existingEncodingSchemaPath);
            final String schemeName = BloomFilterEncodingUtil.retrieveSchemeName(existingEncodingSchema);
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.newInstance(schemeName);
            encoding.setupFromSchema(
                    BloomFilterEncodingUtil.basedOnExistingSchema(
                            schema, selectedFieldNames[1], includedFieldNames[1],
                            existingEncodingSchema, selectedFieldNames[2])
            );

            final GenericRecord[] encodedRecords = es.encodeRecords(records, encoding);
            final Path[] dirs = ds.createDirectories(name,dataRepo, DatasetsService.OTHERS_CAN_READ_PERMISSION);
            ds.saveSchema(name,dirs[2],encoding.getEncodingSchema());
            ds.saveDatasetRecords(name,encodedRecords,encoding.getEncodingSchema(),dirs[1]);
        }
    }

    @Test
    public void test10() throws IOException, DatasetException, BloomFilterEncodingException, BlockingException {
        final String[] encNames = {
                "clk_",
                "fbf_static_",
                "fbf_dynamic_",
                "rbf_uniform_fbf_static_",
                "rbf_uniform_fbf_dynamic_",
                "rbf_weighted_fbf_static_",
                "rbf_weighted_fbf_dynamic_"
        };

        for(String encName : encNames) {
            final Path encAAvroPath = new Path(dataRepo,encName + "voters_a/avro");
            final Path encBAvroPath = new Path(dataRepo,encName + "voters_b/avro");
            final Path encASchemaPath = new Path(dataRepo,encName + "voters_a/schema/" + encName + "voters_a.avsc");
            final Path encBSchemaPath = new Path(dataRepo,encName + "voters_b/schema/" + encName + "voters_b.avsc");

            final Schema aliceEncodingSchema = ds.loadSchema(encASchemaPath);
            final GenericRecord[] aliceEncodedRecords = ds.loadDatasetRecords(new Path[]{encAAvroPath},
                    aliceEncodingSchema);
            final String aliceUidFieldName = uidFieldNames[1];
            final Schema bobEncodingSchema = ds.loadSchema(encBSchemaPath);
            final GenericRecord[] bobEncodedRecords = ds.loadDatasetRecords(new Path[]{encBAvroPath},
                    bobEncodingSchema);
            final String bobUidFieldName = uidFieldNames[2];

            HammingLSHBlocking blocking = bs.newHammingLSHBlockingInstance(HLSH_BLOCKING_L,HLSH_BLOCKING_K,
                    aliceEncodingSchema, bobEncodingSchema
            );
            HammingLSHBlocking.HammingLSHBlockingResult result = bs.runFPSonHammingBlocking(blocking,
                    aliceEncodedRecords, aliceUidFieldName,
                    bobEncodedRecords, bobUidFieldName,
                    HLSH_BLOCKING_C, SIMILARITY_METHOD_NAME, SIMILARITY_THRESHOLD);

            final Path blockingOutputPath = new Path(dataRepo,"blocking_" + encName + "voters_a_voters_b.pairs");
            bs.saveResult(result,blockingOutputPath);
        }
    }

    public void calcStatisticsAndStoreThem(final Path avroPath,
                                           final Path schemaPath,
                                           final Path statsPath,
                                           final double[] initialMUP,
                                           final String[] fieldNames) throws IOException, DatasetException {
        final Schema schema = ds.loadSchema(schemaPath);
        final GenericRecord[] records = ds.loadDatasetRecords(new Path[]{avroPath}, schema);

        // setup stats
        final DatasetStatistics statistics = new DatasetStatistics();

        // record count and field names
        statistics.setRecordCount(records.length);
        statistics.setFieldNames(fieldNames);

        // calculate average q-grams field names
        DatasetStatistics.calculateQgramStatistics(records, schema, statistics, fieldNames);

        // estimate m u
        SimilarityVectorFrequencies frequencies = ms.vectorFrequencies(records,fieldNames);
        ExpectationMaximization estimator = ms.newEMInstance(fieldNames,
                initialMUP[0],initialMUP[1],initialMUP[2]);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics,fieldNames,
                estimator.getM(),estimator.getU());

        // save stats report
        ds.saveStats(statsPath,statistics);
    }

    public void simpleEncodings(final String datasetName,
                                final Path avroPath,
                                final Path schemaPath,
                                final String[] selected, final String[] included)
            throws BloomFilterEncodingException, DatasetException, IOException {

        final Schema schema = ds.loadSchema(schemaPath);
        final GenericRecord[] records = ds.loadDatasetRecords(new Path[]{avroPath},schema);

        LOG.info("Encoding CLK : ");
        {
            final String name = "clk_" + datasetName;
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            "CLK", selected.length, ENCODING_N, 0, ENCODING_K, ENCODING_Q, null, null);
            encoding.makeFromSchema(schema, selected, included);
            final GenericRecord[] encodedRecords = es.encodeRecords(records, encoding);
            final Path[] dirs = ds.createDirectories(name,dataRepo, DatasetsService.OTHERS_CAN_READ_PERMISSION);
            ds.saveSchema(name, dirs[2], encoding.getEncodingSchema());
            ds.saveDatasetRecords(name,encodedRecords,encoding.getEncodingSchema(),dirs[1]);
        }

        LOG.info("---\nEncoding Static FBF :");
        {
            final String name = "fbf_static_" + datasetName;
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            "FBF", selected.length, 0, ENCODING_N, ENCODING_K, ENCODING_Q, null, null);
            encoding.makeFromSchema(schema, selected, included);
            final GenericRecord[] encodedRecords = es.encodeRecords(records, encoding);
            final Path[] dirs = ds.createDirectories(name,dataRepo, DatasetsService.OTHERS_CAN_READ_PERMISSION);
            ds.saveSchema(name,dirs[2],encoding.getEncodingSchema());
            ds.saveDatasetRecords(name,encodedRecords,encoding.getEncodingSchema(),dirs[1]);
        }

        LOG.info("---\nEncoding RBF/Uniform/FBF/Static :");
        {
            final String name = "rbf_uniform_fbf_static_" + datasetName;
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            "RBF", selected.length, ENCODING_N, 512, ENCODING_K, ENCODING_Q, null, null);
            encoding.makeFromSchema(schema, selected, included);
            final GenericRecord[] encodedRecords = es.encodeRecords(records, encoding);
            final Path[] dirs = ds.createDirectories(name,dataRepo, DatasetsService.OTHERS_CAN_READ_PERMISSION);
            ds.saveSchema(name,dirs[2],encoding.getEncodingSchema());
            ds.saveDatasetRecords(name, encodedRecords, encoding.getEncodingSchema(), dirs[1]);
        }
    }

    public void statBasedEncodings(final String datasetName,
                                   final Path avroPath,
                                   final Path schemaPath,
                                   final Path statisticsPath,
                                   final String[] selected, final String[] included)
            throws BloomFilterEncodingException, DatasetException, IOException {

        final Schema schema = ds.loadSchema(schemaPath);
        final GenericRecord[] records = ds.loadDatasetRecords(new Path[]{avroPath},schema);
        final DatasetStatistics statistics = ds.loadStats(statisticsPath);

        final double[] avgQGrams = new double[selected.length];
        final double[] weights = new double[selected.length];
        for (int i = 0; i < selected.length; i++) {
            avgQGrams[i] = statistics.getFieldStatistics().get(selected[i]).getQgramCount(ENCODING_Q);
            weights[i] = statistics.getFieldStatistics().get(selected[i]).getNormalizedRange();
        }
        LOG.info("Encoding Dynamic FBF :");
        {
            final String name = "fbf_dynamic_" + datasetName;
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            "FBF", selected.length, 0, 0, ENCODING_K, ENCODING_Q, avgQGrams, null);
            encoding.makeFromSchema(schema, selected, included);
            final GenericRecord[] encodedRecords = es.encodeRecords(records, encoding);
            final Path[] dirs = ds.createDirectories(name,dataRepo, DatasetsService.OTHERS_CAN_READ_PERMISSION);
            ds.saveSchema(name,dirs[2],encoding.getEncodingSchema());
            ds.saveDatasetRecords(name, encodedRecords, encoding.getEncodingSchema(), dirs[1]);

        }
        LOG.info("---\nEncoding RBF/Uniform/FBF/Dynamic :");
        {
            final String name = "rbf_uniform_fbf_dynamic_" + datasetName;
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            "RBF", selected.length, ENCODING_N, 0, ENCODING_K, ENCODING_Q, avgQGrams, weights);
            encoding.makeFromSchema(schema, selected, included);
            final GenericRecord[] encodedRecords = es.encodeRecords(records, encoding);
            final Path[] dirs = ds.createDirectories(name,dataRepo, DatasetsService.OTHERS_CAN_READ_PERMISSION);
            ds.saveSchema(name,dirs[2],encoding.getEncodingSchema());
            ds.saveDatasetRecords(name, encodedRecords, encoding.getEncodingSchema(), dirs[1]);
        }
        LOG.info("---\nEncoding RBF/Weighted/FBF/Static :");
        {
            final String name = "rbf_weighted_fbf_static_" + datasetName;
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            "RBF", selected.length, 0, 512, ENCODING_K, ENCODING_Q, null, weights);
            encoding.makeFromSchema(schema, selected, included);
            final GenericRecord[] encodedRecords = es.encodeRecords(records, encoding);
            final Path[] dirs = ds.createDirectories(name,dataRepo, DatasetsService.OTHERS_CAN_READ_PERMISSION);
            ds.saveSchema(name,dirs[2],encoding.getEncodingSchema());
            ds.saveDatasetRecords(name, encodedRecords, encoding.getEncodingSchema(), dirs[1]);
        }
        LOG.info("---\nEncoding RBF/Weighted/FBF/Dynamic :");
        {
            final String name = "rbf_weighted_fbf_dynamic_" + datasetName;
            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            "RBF", selected.length, 0, 0, ENCODING_K, ENCODING_Q, avgQGrams, weights);
            encoding.makeFromSchema(schema, selected, included);
            final GenericRecord[] encodedRecords = es.encodeRecords(records, encoding);
            final Path[] dirs = ds.createDirectories(name,dataRepo, DatasetsService.OTHERS_CAN_READ_PERMISSION);
            ds.saveSchema(name,dirs[2],encoding.getEncodingSchema());
            ds.saveDatasetRecords(name, encodedRecords, encoding.getEncodingSchema(), dirs[1]);
        }
    }
}
