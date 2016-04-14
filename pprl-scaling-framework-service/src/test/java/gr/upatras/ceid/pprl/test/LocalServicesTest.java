package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import gr.upatras.ceid.pprl.service.datasets.LocalDatasetsService;

import gr.upatras.ceid.pprl.service.encoding.LocalEncodingService;
import gr.upatras.ceid.pprl.service.matching.LocalMatchingService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
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


    private static final Path dataRepo =new Path("data");

    private static final String[] dataNames = {
            "da_int","person_small"
    };

    private static final Path[] datasetsBase = new Path[]{
            new Path(dataRepo,dataNames[0]),
            new Path(dataRepo,dataNames[1])
    };

    private static final Path[] datasetsAvro = new Path[2];
    private static final Path[] datasetsSchema = new Path[2];
    static {
        int i = 0;
        for (Path dataset : datasetsBase) {
            datasetsAvro[i] = new Path(dataset,"avro");
            datasetsSchema[i] = new Path(dataset,"schema");
            i++;
        }
    };


    @Autowired
    private LocalDatasetsService ds;

    @Autowired
    private LocalEncodingService es;

    @Autowired
    private LocalMatchingService ms;

    @Test
    public void test0() throws IOException, DatasetException {
        // Schema description.
        for (int i = 0 ; i < dataNames.length ; i++) {
            final Schema schema = ds.loadSchema(dataNames[i],datasetsSchema[i]);
            LOG.info("Schema of {}",dataNames[i]);
            LOG.info(schema.toString(true));
        }
    }

    public void test1() throws IOException, DatasetException {
        // Load records
        for (int i = 0 ; i < dataNames.length ; i++) {
            GenericRecord[] records = ds.loadDatasetRecords(new Path[]{datasetsAvro[i]},datasetsSchema[i]);
            LOG.info("Loaded {} records from {}.",records.length,dataNames[i]);
        }
    }

    @Test
    public void test2() throws IOException, DatasetException {
        // Take sample
        for (int i = 0 ; i < dataNames.length ; i++) {
            final Schema schema = ds.loadSchema(dataNames[i],datasetsSchema[i]);
            GenericRecord[] records = ds.sampleDataset(new Path[]{datasetsAvro[i]}, schema, 6);
            LOG.info("Sample {} records from {}.", records.length, dataNames[i]);
        }
    }

    @Test
    public void test3() throws IOException, DatasetException {
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
    public void test4() throws IOException, DatasetException {
        // Statistics from person small
        final Path avroPath = new Path("data/person_small/avro");
        final Path schemaPath = new Path("data/person_small/schema/person_small.avsc");
        final String[] fieldNames = new String[]{"name","surname","location"};
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
        SimilarityVectorFrequencies frequencies = ms.vectorFrequencies(records,fieldNames);
        ExpectationMaximization estimator = ms.newEMInstance(fieldNames,m0,u0,p0);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics,fieldNames,
                estimator.getM(),estimator.getU());

        // save stats report
        final String reportName = "stats_report";
        ds.saveStats(reportName, statistics, new Path("data"));
    }

    @Test
    public void test5() throws DatasetException, IOException {

        // update schema and records
        final Path avroPath = new Path("data/person_small/avro");
        final Path schemaPath = new Path("data/person_small/schema/person_small.avsc");
        final GenericRecord[] records = ds.loadDatasetRecords(new Path[]{avroPath},schemaPath);
        final Schema schema = ds.loadSchema(schemaPath);
        final GenericRecord[] sample = ds.sampleDataset(new Path[]{avroPath}, schema, 6);
        LOG.info(DatasetsUtil.prettyRecords(sample,schema));

        // sort by location,name, save as person_small_1
        final Schema updatedSchema1 =
                ds.updateDatasetSchema(schema,true,false,null,"location","name");
        final GenericRecord[] updatedRecords1 =
                ds.updateDatasetRecords(records,schema,true,false,null,"location","name");
        Path[] paths = ds.createDirectories("person_small_1",dataRepo);
        ds.saveSchema("person_small_1",paths[2],updatedSchema1);
        ds.saveDatasetRecords("person_small_1",updatedRecords1,updatedSchema1,paths[1]);


        // sort by surname add uiid and save as person_small_2
        final Schema updatedSchema2 =
                ds.updateDatasetSchema(schema,true,true,"uid","surname");
        final GenericRecord[] updatedRecords2 =
                ds.updateDatasetRecords(records,schema,true,true,"uid","surname");
        paths = ds.createDirectories("person_small_2",dataRepo);
        ds.saveSchema("person_small_2",paths[2],updatedSchema2);
        ds.saveDatasetRecords("person_small_2",updatedRecords2,updatedSchema2,paths[1]);

        // sort by name,surname,location, add uiid and save as person_small
        final Schema updatedSchema3 =
                ds.updateDatasetSchema(schema,true,true,"uid","name","surname","location");
        final GenericRecord[] updatedRecords3 =
                ds.updateDatasetRecords(records,schema,true,true,"uid","name","surname","location");
        paths = ds.createDirectories("person_small_3",dataRepo);
        ds.saveSchema("person_small_3",paths[2],updatedSchema3);
        ds.saveDatasetRecords("person_small_3",updatedRecords3,updatedSchema3,paths[1]);
        final GenericRecord[] sample3 = ds.sampleDataset(new Path[]{avroPath},schemaPath,6);
        LOG.info(DatasetsUtil.prettyRecords(sample3,updatedSchema3));
    }

    @Test
    public void test6() throws IOException, DatasetException {
        // Statistics from person small
        final Path avroPath = new Path("data/person_small_3/avro");
        final Path schemaPath = new Path("data/person_small_3/schema/person_small_3.avsc");
        final String[] fieldNames = new String[]{"name","surname","location"};
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
        SimilarityVectorFrequencies frequencies = ms.vectorFrequencies(records,fieldNames);
        ExpectationMaximization estimator = ms.newEMInstance(fieldNames,m0,u0,p0);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics,fieldNames,
                estimator.getM(),estimator.getU());

        // save stats report
        final String reportName = "stats_report";
        ds.saveStats(reportName, statistics, new Path("data"));
    }

    @Test
    public void test7() throws IOException, DatasetException, BloomFilterEncodingException {
        // encode a dataset - first load it
        final Path avroPath = new Path("data/person_small_3/avro");
        final Path schemaPath = new Path("data/person_small_3/schema/person_small_3.avsc");
        final GenericRecord[] records = ds.loadDatasetRecords(new Path[]{avroPath},schemaPath);
        final Schema schema = ds.loadSchema(schemaPath);
        LOG.info(DatasetsUtil.prettyRecords(records,schema));

        // selected fields for encoding and rest fields to include
        final String[] selectedFieldNames = new String[]{"surname","location"};
        final String[] includedFieldNames = new String[]{"uid","name","id"};

        // statistics
        // setup stats
        final DatasetStatistics statistics = new DatasetStatistics();

        // record count and field names
        statistics.setRecordCount(records.length);
        statistics.setFieldNames(selectedFieldNames);

        // calculate average q-grams field names
        DatasetStatistics.calculateQgramStatistics(records, schema, statistics, selectedFieldNames);

        // estimate m u
        SimilarityVectorFrequencies frequencies = ms.vectorFrequencies(records,selectedFieldNames);
        final double m0 = 0.9;
        final double u0 = 0.1;
        final double p0 = 0.2;
        ExpectationMaximization estimator = ms.newEMInstance(selectedFieldNames,m0,u0,p0);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics,selectedFieldNames,
                estimator.getM(),estimator.getU());

        // Encoding for various schemes
        final int K = 10;
        final int Q = 2;
        final double[] avgQGrams = new double[]{
                statistics.getFieldStatistics().get(selectedFieldNames[0]).getQgramCount(Q),
                statistics.getFieldStatistics().get(selectedFieldNames[1]).getQgramCount(Q)
        };
        final double[] weights = new double[] {
                statistics.getFieldStatistics().get(selectedFieldNames[0]).getNormalizedRange(),
                statistics.getFieldStatistics().get(selectedFieldNames[1]).getNormalizedRange()
        };

        LOG.info("Encoding CLK : ");
        {
            final String scheme = "CLK";
            final int fieldCount = selectedFieldNames.length;
            final int N = 1024;
            final int fbfN = 0;
            final double[] AQC = null;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selectedFieldNames, includedFieldNames);
            LOG.info(DatasetsUtil.prettyRecords(
                    es.encodeRecords(records, encoding), encoding.getEncodingSchema()));
        }
        LOG.info("---\nEncoding Static FBF :");
        {
            final String scheme = "FBF";
            final int fieldCount = selectedFieldNames.length;
            final int N = 0;
            final int fbfN = 512;
            final double[] AQC = null;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selectedFieldNames, includedFieldNames);
            LOG.info(DatasetsUtil.prettyRecords(
                    es.encodeRecords(records, encoding), encoding.getEncodingSchema()));
        }

        LOG.info("---\nEncoding Dynamic FBF :");
        {
            final String scheme = "FBF";
            final int fieldCount = selectedFieldNames.length;
            final int N = 0;
            final int fbfN = 0;
            final double[] AQC = avgQGrams;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selectedFieldNames, includedFieldNames);
            LOG.info(DatasetsUtil.prettyRecords(
                    es.encodeRecords(records, encoding), encoding.getEncodingSchema()));
        }

        LOG.info("---\nEncoding RBF/Uniform/FBF/Static :");
        {
            final String scheme = "RBF";
            final int fieldCount = selectedFieldNames.length;
            final int N = 1024;
            final int fbfN = 512;
            final double[] AQC = null;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selectedFieldNames, includedFieldNames);
            LOG.info(DatasetsUtil.prettyRecords(
                    es.encodeRecords(records, encoding), encoding.getEncodingSchema()));
        }

        LOG.info("---\nEncoding RBF/Uniform/FBF/Dynamic :");
        {
            final String scheme = "RBF";
            final int fieldCount = selectedFieldNames.length;
            final int N = 1024;
            final int fbfN = 0;
            final double[] AQC = avgQGrams;
            final double[] W = null;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selectedFieldNames, includedFieldNames);
            LOG.info(DatasetsUtil.prettyRecords(
                    es.encodeRecords(records, encoding), encoding.getEncodingSchema()));
        }


        LOG.info("---\nEncoding RBF/Weighted/FBF/Static :");
        {
            final String scheme = "RBF";
            final int fieldCount = selectedFieldNames.length;
            final int N = 0;
            final int fbfN = 512;
            final double[] AQC = null;
            final double[] W = weights;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selectedFieldNames, includedFieldNames);
            LOG.info(DatasetsUtil.prettyRecords(
                    es.encodeRecords(records, encoding), encoding.getEncodingSchema()));
        }

        LOG.info("---\nEncoding RBF/Weighted/FBF/Dynamic :");
        {
            final String scheme = "RBF";
            final int fieldCount = selectedFieldNames.length;
            final int N = 0;
            final int fbfN = 0;
            final double[] AQC = avgQGrams;
            final double[] W = weights;

            final BloomFilterEncoding encoding =
                    BloomFilterEncodingUtil.instanceFactory(
                            scheme, fieldCount, N, fbfN, K, Q, AQC, W);
            encoding.makeFromSchema(schema, selectedFieldNames, includedFieldNames);
            LOG.info(DatasetsUtil.prettyRecords(
                    es.encodeRecords(records, encoding), encoding.getEncodingSchema()));
        }

    }

}
