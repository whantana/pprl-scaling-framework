package gr.upatras.ceid.pprl.benchmarks;


import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.encoding.CLKEncoding;
import gr.upatras.ceid.pprl.encoding.FieldBloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.RowBloomFilterEncoding;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class EncodingEvaluationBenchmarkTest {

    private static final Logger LOG = LoggerFactory.getLogger(EncodingEvaluationBenchmarkTest.class);

    private static final String[] TABLE_HEADER = {"id", "name", "surname", "city"};
    private static Schema.Type[] TYPES = {
            Schema.Type.STRING, Schema.Type.STRING,
            Schema.Type.STRING, Schema.Type.STRING
    };
    private static String[] DOCS = {
            "unique id", "First name", "Last name", "City"
    };
    private static final int LIMIT = 1000;

    private final int[] K = new int[]{10, 15, 30, 35 , 45 , 50};
    private final double[] thresholds = {
            0.50, 0.55,
            0.60, 0.65,
            0.70, 0.75,
            0.80, 0.85,
            0.90, 0.95
    };

    final String[] fieldNamesA = new String[]{"name", "surname", "city"};
    final String[] fieldNamesB = new String[]{"name", "surname", "city"};
    final String idFieldName = "id";
    final String fieldName = "surname";
    final String[] included = new String[]{idFieldName};

    final Path statsPath = new Path("data/benchmarks/stats.properties");
    final String BENCH_HEADER = "fs,recall,precision,fscore\n";

    @Test
    public void allTests() throws IOException, DatasetException, BloomFilterEncodingException {
        // setup
        test00();
        test01();

        //test fbf
        testFBF();

        // open
        test02();
        test03();
        test05(); // Fellegi-Sunter


        // encode strings
        test04();
        // encode records
        test06(); // CLK
        test07(); // FBF Static
        test08(); // FBF Dynamic
        test09(); // RBF Uniform/Static
        test10(); // RBF Uniform/Dynamic
        test11(); // RBF Weighted/Static
        test12(); // RBF Weighted/Dynamic
    }

    public void testFBF() throws IOException, DatasetException, BloomFilterEncodingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/recordsA/avro"));

        Properties properties = new Properties();
        FSDataInputStream fsdis = fs.open(statsPath);
        properties.load(fsdis);
        fsdis.close();
        DatasetStatistics stats = new DatasetStatistics();
        stats.fromProperties(properties);

        for(String f : fieldNamesA) {
            DescriptiveStatistics st = new DescriptiveStatistics();
            int s = 512;
            int k = 10;
            int q = 2;
            double g = stats.getFieldStatistics().get(f).getQgramCount(2);
            LOG.info(String.format("Field %s average bigrams count : %d",f,(int)g));
            CLKEncoding encodingA = new CLKEncoding(s, k, q);
            encodingA.makeFromSchema(schemaA, new String[]{f}, new String[]{idFieldName});
            encodingA.initialize();
            final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
            for (int r = 0; r < recordsA.length; r++) {
                encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);
                BloomFilter bf = BloomFilterEncodingUtil.retrieveBloomFilter(encodedRecordsA[r],
                        encodingA.getEncodingFieldName(),
                        encodingA.getBFN());
                st.addValue(bf.countOnes());
            }
            LOG.info(String.format("Field %s Static FBF (s=%d,k=%d,q=%d) : min/avg/max %d/%d/%d : fpp : %.12f ",
                    f,s,k,q,(int)st.getMin(),(int)st.getMean(),(int)st.getMax(),
                    BloomFilter.calcFPP(s,k,(int)g)));

            st.clear();
            CLKEncoding encodingB = new CLKEncoding(FieldBloomFilterEncoding.dynamicsize(g,k), k, q);
            encodingB.makeFromSchema(schemaA, new String[]{f}, new String[]{idFieldName});
            encodingB.initialize();
            final GenericRecord[] encodedRecordsB = new GenericRecord[recordsA.length];
            for (int r = 0; r < recordsA.length; r++) {
                encodedRecordsB[r] = encodingB.encodeRecord(recordsA[r]);
                BloomFilter bf = BloomFilterEncodingUtil.retrieveBloomFilter(encodedRecordsB[r],
                        encodingB.getEncodingFieldName(),
                        encodingB.getBFN());
                st.addValue(bf.countOnes());
            }
            s = encodingB.getBFN();
            LOG.info(String.format("Field %s Dynamic FBF (s=%d,k=%d,q=%d) : min/avg/max %d/%d/%d : fpp : %.12f",
                    f, s, k, q, (int)st.getMin(), (int)st.getMean(), (int)st.getMax(),
                    BloomFilter.calcFPP(s,k,(int)g)));
        }
    }

    /**
     * Create avro datasets from csv
     */
    public void test00() throws IOException, DatasetException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        Schema schemaA = DatasetsUtil.avroSchema("person_a", "People", "pprl.datasets", TABLE_HEADER, TYPES, DOCS);
        Schema schemaB = DatasetsUtil.avroSchema("person_b", "People", "pprl.datasets", TABLE_HEADER, TYPES, DOCS);
        final Path p = DatasetsUtil.csv2avro(fs, schemaA, "dataA", new Path(fs.getWorkingDirectory(), "data/benchmarks"),
                new Path(fs.getWorkingDirectory(), "data/benchmarks/dataA.csv"));
        LOG.info("Saved at path {} ", p);
        final Path p1 = DatasetsUtil.csv2avro(fs, schemaB, "dataB", new Path(fs.getWorkingDirectory(), "data/benchmarks"),
                new Path(fs.getWorkingDirectory(), "data/benchmarks/dataB.csv"));
        LOG.info("Saved at path {} ", p1);

        final Path p2 = DatasetsUtil.csv2avro(fs, schemaA, "recordsA", new Path(fs.getWorkingDirectory(), "data/benchmarks"),
                new Path(fs.getWorkingDirectory(), "data/benchmarks/recordsA.csv"));
        LOG.info("Saved at path {} ", p2);
        final Path p3 = DatasetsUtil.csv2avro(fs, schemaB, "recordsB", new Path(fs.getWorkingDirectory(), "data/benchmarks"),
                new Path(fs.getWorkingDirectory(), "data/benchmarks/recordsB.csv"));
        LOG.info("Saved at path {} ", p3);
    }

    /**
     * Get statistics of datasets
     */
    public void test01() throws IOException, DatasetException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schemaA, new Path("data/benchmarks/recordsA/avro"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsB/schema/recordsB.avsc"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schemaB, new Path("data/benchmarks/recordsB/avro"));


        // setup stats
        final DatasetStatistics statistics = new DatasetStatistics();

        // record count and field names
        statistics.setRecordCount(recordsA.length);
        statistics.setFieldNames(fieldNamesA);

        // calculate average q-grams field names
        DatasetStatistics.calculateQgramStatistics(recordsA, schemaA, statistics, fieldNamesA);

        // estimate m u
        SimilarityVectorFrequencies frequencies =
			SimilarityUtil.vectorFrequencies(recordsA,fieldNamesA);
                // SimilarityUtil.vectorFrequencies(recordsA,recordsB ,fieldNamesA,fieldNamesB);
        // ExpectationMaximization estimator = new ExpectationMaximization(fieldNamesA, 0.9, 0.01,  0.001);
		ExpectationMaximization estimator = new ExpectationMaximization(fieldNamesA, 0.9, 0.01, Double.MIN_VALUE);
        estimator.runAlgorithm(frequencies);

        LOG.info(frequencies.toString());
        LOG.info(estimator.toString());

        // using m,u,p estimates complete the statistics of the dataset
        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics, fieldNamesA,
                estimator.getM(), estimator.getU());

        LOG.info("---");
        LOG.info(DatasetStatistics.prettyStats(statistics));
        LOG.info("---");
        LOG.info("Saving stats to {}.", statsPath);
        FSDataOutputStream fsdos = fs.create(statsPath, true);
        statistics.toProperties().store(fsdos, "Statistics");
        fsdos.close();
    }

    /**
     * Dice of Q-Grams on open data
     */
    public void test02() throws DatasetException, IOException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/dataA/schema/dataA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/dataB/schema/dataB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/dataA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/dataB/avro"));

        final int[] Q = new int[]{2, 3, 4};

        LOG.info("---");
        for (int q : Q) {
            LOG.info("Dice {}-grams", q);
            LOG.info("---");
            final String fileName = String.format("bench_strings_dice_%d.csv", q);
            FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
            fsdos.writeBytes(BENCH_HEADER);
            for (double threshold : thresholds) {
                int TM = 0;
                int FM = 0;
                int TN = 0;
                int FN = 0;
                for (GenericRecord rA : recordsA) {
                    for (GenericRecord rB : recordsB) {
                        String sA = rA.get(fieldName).toString();
                        String sB = rB.get(fieldName).toString();
                        String idA = rA.get(idFieldName).toString();
                        String idB = rB.get(idFieldName).toString();
                        boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                        boolean similar = SimilarityUtil.similarity(
                                (q == 2 ? "dice_bigrams" : (q == 3 ? "dice_trigrams" : "dice_quadgrams"))
                                , sA, sB, threshold);
                        if (similar && trueSimilar) TM++;
                        else if (similar && !trueSimilar) FM++;
                        else if (!similar && trueSimilar) FN++;
                        else TN++;
                    }
                }
                double recall = (double) TM / (double) (TM + FN);
                double precision = (double) TM / (double) (TM + FM);
                double fscore = 2*(recall*precision)/(recall + precision);
                LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                        threshold, recall, precision,fscore, TM, FM, TN, FN));
                if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
                fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                        threshold, recall, precision,fscore));
            }
            fsdos.close();
            LOG.info("---");
        }
    }

    /**
     * Jaro Winkler on open data
     */
    public void test03() throws DatasetException, IOException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/dataA/schema/dataA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/dataB/schema/dataB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/dataA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/dataB/avro"));

        LOG.info("---");
        LOG.info("Jaro-Winkler");
        LOG.info("---");

        FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", "bench_strings_jw.csv"));
        fsdos.writeBytes(BENCH_HEADER);
        for (double threshold : thresholds) {
            int TM = 0;
            int FM = 0;
            int TN = 0;
            int FN = 0;
            for (GenericRecord rA : recordsA) {
                for (GenericRecord rB : recordsB) {
                    String sA = rA.get(fieldName).toString();
                    String sB = rB.get(fieldName).toString();
                    String idA = rA.get(idFieldName).toString();
                    String idB = rB.get(idFieldName).toString();
                    boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                    boolean similar = SimilarityUtil.similarity("jaro_winkler", sA, sB, threshold);
                    if (similar && trueSimilar) TM++;
                    else if (similar && !trueSimilar) FM++;
                    else if (!similar && trueSimilar) FN++;
                    else TN++;
                }
            }
            double recall = (double) TM / (double) (TM + FN);
            double precision = (double) TM / (double) (TM + FM);
            double fscore = 2*(recall*precision)/(recall + precision);
            LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                    threshold, recall, precision,fscore, TM, FM, TN, FN));
            if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
            fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                    threshold, recall, precision,fscore));
        }
        fsdos.close();
        LOG.info("---");
    }

    /**
     * Dice on encoded strings.
     */
    public void test04() throws IOException, DatasetException, BloomFilterEncodingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/dataA/schema/dataA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/dataB/schema/dataB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/dataA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/dataB/avro"));

        final int[] S = new int[]{512, 1024, 2048};
        final int q = 2;
        LOG.info("---");
        for (int s : S) {
            for (int k : K) {
                LOG.info("Bloom-Filters {}", String.format("(%d,%d,%d)", s, q, k));
                LOG.info("---");
                CLKEncoding encodingA = new CLKEncoding(s, k, q);
                encodingA.makeFromSchema(schemaA, new String[]{fieldName}, new String[]{idFieldName});
                encodingA.initialize();
                final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
                for (int r = 0; r < recordsA.length; r++)
                    encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);

                CLKEncoding encodingB = new CLKEncoding(s, k, q);
                encodingB.makeFromSchema(schemaB, new String[]{fieldName}, new String[]{idFieldName});
                encodingB.initialize();
                final GenericRecord[] encodedRecordsB = new GenericRecord[recordsB.length];
                for (int r = 0; r < recordsB.length; r++)
                    encodedRecordsB[r] = encodingB.encodeRecord(recordsB[r]);

                final String fileName = String.format("bench_strings_bf_%d_%d_%d.csv", s, q, k);
                FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
                fsdos.writeBytes(BENCH_HEADER);
                for (double threshold : thresholds) {
                    int TM = 0;
                    int FM = 0;
                    int TN = 0;
                    int FN = 0;
                    for (GenericRecord erA : encodedRecordsA) {
                        for (GenericRecord erB : encodedRecordsB) {
                            final BloomFilter bfA = encodingA.retrieveBloomFilter(erA);
                            final BloomFilter bfB = encodingB.retrieveBloomFilter(erB);
                            String idA = erA.get(idFieldName).toString();
                            String idB = erB.get(idFieldName).toString();
                            boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                            boolean similar = PrivateSimilarityUtil.similarity("dice", bfA, bfB, threshold);
                            if (similar && trueSimilar) TM++;
                            else if (similar && !trueSimilar) FM++;
                            else if (!similar && trueSimilar) FN++;
                            else TN++;
                        }
                    }
                    double recall = (double) TM / (double) (TM + FN);
                    double precision = (double) TM / (double) (TM + FM);
                    double fscore = 2*(recall*precision)/(recall + precision);
                    LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                            threshold, recall, precision,fscore, TM, FM, TN, FN));
                    if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
                    fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                            threshold, recall, precision,fscore));
                }
                LOG.info("---");
                fsdos.close();
            }
        }
    }

    /**
     * Fellegi-Sunter (Jaro Winkler) on open data
     **/
    public void test05() throws DatasetException, IOException, BloomFilterEncodingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsB/schema/recordsB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/recordsA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/recordsB/avro"));

        ExpectationMaximization estimator =
                new ExpectationMaximization(fieldNamesA, 0.9, 0.01, 0.001);
        SimilarityVectorFrequencies  frequencies =
                SimilarityUtil.vectorFrequencies(recordsA, recordsB,
                        fieldNamesA, fieldNamesB);
        estimator.runAlgorithm(frequencies);

        final double[] m = estimator.getM();
        final double[] u = estimator.getU();
        final double p = estimator.getP();

        final DatasetStatistics statistics = new DatasetStatistics();

        // record count and field names
        statistics.setRecordCount(recordsA.length*recordsA.length);
        statistics.setFieldNames(fieldNamesA);

        statistics.setEmPairsCount(estimator.getPairCount());
        statistics.setEmAlgorithmIterations(estimator.getIteration());
        statistics.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                statistics, fieldNamesA,
                m, u);

        LOG.info(frequencies.toString());
        LOG.info(estimator.toString());

        LOG.info("---");
        LOG.info(DatasetStatistics.prettyStats(statistics));
        LOG.info("---");

        LOG.info("---");
        LOG.info("Jaro-Winkler");
        LOG.info("---");

        int TM = 0;
        int FM = 0;
        int TN = 0;
        int FN = 0;

        for (GenericRecord rA : recordsA) {
            for (GenericRecord rB : recordsB) {
                final String idA = rA.get(idFieldName).toString();
                final String idB = rB.get(idFieldName).toString();
                final double score = SimilarityUtil.similarityScore(
                        new GenericRecord[]{rA, rB},
                        new String[][]{fieldNamesA, fieldNamesB},
                        m, u) + Math.log(p / (1 - p));
                boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                boolean similar = score >= 0;
                if (similar && trueSimilar) TM++;
                else if (similar && !trueSimilar) FM++;
                else if (!similar && trueSimilar) FN++;
                else TN++;
            }
        }
        double recall = (double) TM / (double) (TM + FN);
        double precision = (double) TM / (double) (TM + FM);
        double fscore = 2 * (recall * precision) / (recall + precision);
        LOG.info(String.format("%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                recall, precision,fscore, TM, FM, TN, FN));
    }

    /**
     * Dice on CLK.
     */
    public void test06() throws DatasetException, IOException, BloomFilterEncodingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsB/schema/recordsB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/recordsA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/recordsB/avro"));

        int q = 2;
        int k = 10;
        int[] S = new int[]{1024,2048,4096,8192};
        LOG.info("---");
        for (int s : S) {
            LOG.info("CLK {}", String.format("(%d,%d,%d)", s, q, k));
            LOG.info("---");
            CLKEncoding encodingA = new CLKEncoding(s, k, q);
            encodingA.makeFromSchema(schemaA, fieldNamesA, new String[]{idFieldName});
            encodingA.initialize();
            final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
            for (int r = 0; r < recordsA.length; r++)
                encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);

            BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                    BloomFilterEncodingUtil.basedOnExistingSchema(schemaB, fieldNamesB, included,
                            encodingA.getEncodingSchema(),
                            fieldNamesA));
            encodingB.initialize();
            final GenericRecord[] encodedRecordsB = new GenericRecord[recordsB.length];
            for (int r = 0; r < recordsB.length; r++)
                encodedRecordsB[r] = encodingB.encodeRecord(recordsB[r]);

            final String fileName = String.format("bench_records_clk_%d.csv",encodingA.getBFN());
            FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
            fsdos.writeBytes(BENCH_HEADER);
            for (double threshold : thresholds) {
                int TM = 0;
                int FM = 0;
                int TN = 0;
                int FN = 0;
                for (GenericRecord erA : encodedRecordsA) {
                    for (GenericRecord erB : encodedRecordsB) {
                        final BloomFilter bfA = encodingA.retrieveBloomFilter(erA);
                        final BloomFilter bfB = encodingB.retrieveBloomFilter(erB);
                        String idA = erA.get("id").toString();
                        String idB = erB.get("id").toString();
                        boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                        boolean similar = PrivateSimilarityUtil.similarity("dice", bfA, bfB, threshold);
                        if (similar && trueSimilar) TM++;
                        else if (similar && !trueSimilar) FM++;
                        else if (!similar && trueSimilar) FN++;
                        else TN++;
                    }
                }
                double recall = (double) TM / (double) (TM + FN);
                double precision = (double) TM / (double) (TM + FM);
                double fscore = 2*(recall*precision)/(recall + precision);
                LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                        threshold, recall, precision,fscore, TM, FM, TN, FN));
                if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
                fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                        threshold, recall, precision,fscore));
            }
            LOG.info("---");
            fsdos.close();
        }
    }

    /**
     * Dice on FBF Static
     */
    public void test07() throws DatasetException, IOException, BloomFilterEncodingException {
        // FBF Static
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsB/schema/recordsB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/recordsA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/recordsB/avro"));

        int[] S = new int[]{1024,2048,4096,8192};
        int q = 2;
        int k = 10;
        for (int s : S) {
            int sf = s / fieldNamesA.length;
            FieldBloomFilterEncoding encodingA = new FieldBloomFilterEncoding(sf, fieldNamesA.length, k, q);
            encodingA.makeFromSchema(schemaA, fieldNamesA, new String[]{idFieldName});
            encodingA.initialize();
            final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
            for (int r = 0; r < recordsA.length; r++)
                encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);

            BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                    BloomFilterEncodingUtil.basedOnExistingSchema(schemaB, fieldNamesB, included,
                            encodingA.getEncodingSchema(),
                            fieldNamesA));
            encodingB.initialize();
            final GenericRecord[] encodedRecordsB = new GenericRecord[recordsB.length];
            for (int r = 0; r < recordsB.length; r++)
                encodedRecordsB[r] = encodingB.encodeRecord(recordsB[r]);

            LOG.info("FBF Static {}", String.format("(%d,%d,%d)", encodingA.getBFN(), q, k));
            LOG.info("---");
            final String fileName = String.format("bench_records_fbf_static_%d.csv",
                    encodingA.getBFN());
            FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
            fsdos.writeBytes(BENCH_HEADER);
            for (double threshold : thresholds) {
                int TM = 0;
                int FM = 0;
                int TN = 0;
                int FN = 0;
                for (GenericRecord erA : encodedRecordsA) {
                    for (GenericRecord erB : encodedRecordsB) {
                        final BloomFilter bfA = encodingA.retrieveBloomFilter(erA);
                        final BloomFilter bfB = encodingB.retrieveBloomFilter(erB);
                        String idA = erA.get("id").toString();
                        String idB = erB.get("id").toString();
                        boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                        boolean similar = PrivateSimilarityUtil.similarity("dice", bfA, bfB, threshold);
                        if (similar && trueSimilar) TM++;
                        else if (similar && !trueSimilar) FM++;
                        else if (!similar && trueSimilar) FN++;
                        else TN++;
                    }
                }
                double recall = (double) TM / (double) (TM + FN);
                double precision = (double) TM / (double) (TM + FM);
                double fscore = 2*(recall*precision)/(recall + precision);
                LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                        threshold, recall, precision,fscore, TM, FM, TN, FN));
                if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
                fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                        threshold, recall, precision,fscore));
            }
            LOG.info("---");
            fsdos.close();
        }
    }

    /**
     * Dice on RBF Static/Uniform
     */
    public void test08() throws DatasetException, IOException, BloomFilterEncodingException {
        // RBF (Static/Uniform)
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsB/schema/recordsB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/recordsA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/recordsB/avro"));

        final String[] fieldNamesA = new String[]{"name", "surname", "city"};
        final String[] fieldNamesB = new String[]{"name", "surname", "city"};
        final String idFieldName = "id";

        int[] S = new int[]{1024, 2048, 4096, 8192};
        for (int s : S) {
            int q = 2;
            int k = 10;
            final int[] fbfNs = new int[fieldNamesA.length];
            Arrays.fill(fbfNs, (s/fieldNamesA.length) + 100);

            LOG.info("---");
            RowBloomFilterEncoding encodingA = new RowBloomFilterEncoding(fbfNs, s, k, q);
            encodingA.makeFromSchema(schemaA, fieldNamesA, new String[]{idFieldName});
            encodingA.initialize();
            final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
            for (int r = 0; r < recordsA.length; r++)
                encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);

            BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                    BloomFilterEncodingUtil.basedOnExistingSchema(schemaB, fieldNamesB, included,
                            encodingA.getEncodingSchema(),
                            fieldNamesA));
            encodingB.initialize();
            final GenericRecord[] encodedRecordsB = new GenericRecord[recordsB.length];
            for (int r = 0; r < recordsB.length; r++)
                encodedRecordsB[r] = encodingB.encodeRecord(recordsB[r]);

            LOG.info("RBF Static/Uniform {}", String.format("(%d,%d,%d)", encodingA.getBFN(), q, k));
            LOG.info("---");
            final String fileName = String.format("bench_records_rbf_uniform_static_%d.csv",
                    encodingA.getBFN());
            FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
            fsdos.writeBytes(BENCH_HEADER);
            for (double threshold : thresholds) {
                int TM = 0;
                int FM = 0;
                int TN = 0;
                int FN = 0;
                for (GenericRecord erA : encodedRecordsA) {
                    for (GenericRecord erB : encodedRecordsB) {
                        final BloomFilter bfA = encodingA.retrieveBloomFilter(erA);
                        final BloomFilter bfB = encodingB.retrieveBloomFilter(erB);
                        String idA = erA.get("id").toString();
                        String idB = erB.get("id").toString();
                        boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                        boolean similar = PrivateSimilarityUtil.similarity("dice", bfA, bfB, threshold);
                        if (similar && trueSimilar) TM++;
                        else if (similar && !trueSimilar) FM++;
                        else if (!similar && trueSimilar) FN++;
                        else TN++;
                    }
                }
                double recall = (double) TM / (double) (TM + FN);
                double precision = (double) TM / (double) (TM + FM);
                double fscore = 2*(recall*precision)/(recall + precision);
                LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                        threshold, recall, precision,fscore, TM, FM, TN, FN));
                if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
                fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                        threshold, recall, precision,fscore));
            }
            LOG.info("---");
            fsdos.close();
        }
    }

    /**
     * Dice on FBF Dynamic
     */
    public void test09() throws DatasetException, IOException, BloomFilterEncodingException {

        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsB/schema/recordsB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/recordsA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/recordsB/avro"));

        final String[] fieldNamesA = new String[]{"name", "surname", "city"};
        final String[] fieldNamesB = new String[]{"name", "surname", "city"};
        final String idFieldName = "id";

        Properties properties = new Properties();
        FSDataInputStream fsdis = fs.open(statsPath);
        properties.load(fsdis);
        fsdis.close();
        DatasetStatistics stats = new DatasetStatistics();
        stats.fromProperties(properties);

        int[] K = new int[]{10, 15, 30, 50};
        int q = 2;
        LOG.info("---");
        for (int k : K) {
            final double[] avgQgrams = new double[fieldNamesA.length];
            for (int i = 0; i < fieldNamesA.length; i++)
                avgQgrams[i] = stats.getFieldStatistics()
                        .get(fieldNamesA[i])
                        .getQgramCount(q);

            FieldBloomFilterEncoding encodingA = new FieldBloomFilterEncoding(avgQgrams, k, q);
            encodingA.makeFromSchema(schemaA, fieldNamesA, new String[]{idFieldName});
            encodingA.initialize();
            final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
            for (int r = 0; r < recordsA.length; r++)
                encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);

            BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                    BloomFilterEncodingUtil.basedOnExistingSchema(schemaB, fieldNamesB, included,
                            encodingA.getEncodingSchema(),
                            fieldNamesA));
            encodingB.initialize();
            final GenericRecord[] encodedRecordsB = new GenericRecord[recordsB.length];
            for (int r = 0; r < recordsB.length; r++)
                encodedRecordsB[r] = encodingB.encodeRecord(recordsB[r]);

            LOG.info("FBF Dynamic {}", String.format("(%d,%d,%d)", encodingA.getBFN(), q, k));
            LOG.info("---");
            final String fileName = String.format("bench_records_fbf_dynamic_%d.csv",k);
            FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
            fsdos.writeBytes(BENCH_HEADER);
            for (double threshold : thresholds) {
                int TM = 0;
                int FM = 0;
                int TN = 0;
                int FN = 0;
                for (GenericRecord erA : encodedRecordsA) {
                    for (GenericRecord erB : encodedRecordsB) {
                        final BloomFilter bfA = encodingA.retrieveBloomFilter(erA);
                        final BloomFilter bfB = encodingB.retrieveBloomFilter(erB);
                        String idA = erA.get("id").toString();
                        String idB = erB.get("id").toString();
                        boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                        boolean similar = PrivateSimilarityUtil.similarity("dice", bfA, bfB, threshold);
                        if (similar && trueSimilar) TM++;
                        else if (similar && !trueSimilar) FM++;
                        else if (!similar && trueSimilar) FN++;
                        else TN++;
                    }
                }
                double recall = (double) TM / (double) (TM + FN);
                double precision = (double) TM / (double) (TM + FM);
                double fscore = 2*(recall*precision)/(recall + precision);
                LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                        threshold, recall, precision,fscore, TM, FM, TN, FN));
                if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
                fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                        threshold, recall, precision,fscore));
            }
            LOG.info("---");
            fsdos.close();
        }
    }

    /**
     * Dice on RBF Dynamic Uniform
     */
    public void test10() throws DatasetException, IOException, BloomFilterEncodingException {
        // RBF Dynamic/Uniform
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsB/schema/recordsB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/recordsA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/recordsB/avro"));

        Properties properties = new Properties();
        FSDataInputStream fsdis = fs.open(statsPath);
        properties.load(fsdis);
        fsdis.close();
        DatasetStatistics stats = new DatasetStatistics();
        stats.fromProperties(properties);
        int q = 2;
        int[] K = new int[]{10,15,30,50};
        int[] S = new int[]{1024,2048,4096,8192};
        LOG.info("---");
        for(int s : S) {
            for(int k : K) {
                final double[] avgQgrams = new double[fieldNamesA.length];
                for (int i = 0; i < fieldNamesA.length; i++)
                    avgQgrams[i] = stats.getFieldStatistics()
                            .get(fieldNamesA[i])
                            .getQgramCount(q);
                RowBloomFilterEncoding encodingA = new RowBloomFilterEncoding(avgQgrams, s, k, q);
                encodingA.makeFromSchema(schemaA, fieldNamesA, new String[]{idFieldName});
                encodingA.initialize();
                final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
                for (int r = 0; r < recordsA.length; r++)
                    encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);

                BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                        BloomFilterEncodingUtil.basedOnExistingSchema(schemaB, fieldNamesB, included,
                                encodingA.getEncodingSchema(),
                                fieldNamesA));
                encodingB.initialize();
                final GenericRecord[] encodedRecordsB = new GenericRecord[recordsB.length];
                for (int r = 0; r < recordsB.length; r++)
                    encodedRecordsB[r] = encodingB.encodeRecord(recordsB[r]);

                LOG.info("RBF Dynamic/Uniform {}", String.format("(%d,%d,%d)", encodingA.getBFN(), q, k));
                LOG.info("---");
                final String fileName = String.format("bench_records_rbf_uniform_dynamic_%d_%d.csv",
                        encodingA.getBFN(),k);
                FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
                fsdos.writeBytes(BENCH_HEADER);
                for (double threshold : thresholds) {
                    int TM = 0;
                    int FM = 0;
                    int TN = 0;
                    int FN = 0;
                    for (GenericRecord erA : encodedRecordsA) {
                        for (GenericRecord erB : encodedRecordsB) {
                            final BloomFilter bfA = encodingA.retrieveBloomFilter(erA);
                            final BloomFilter bfB = encodingB.retrieveBloomFilter(erB);
                            String idA = erA.get("id").toString();
                            String idB = erB.get("id").toString();
                            boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                            boolean similar = PrivateSimilarityUtil.similarity("dice", bfA, bfB, threshold);
                            if (similar && trueSimilar) TM++;
                            else if (similar && !trueSimilar) FM++;
                            else if (!similar && trueSimilar) FN++;
                            else TN++;
                        }
                    }
                    double recall = (double) TM / (double) (TM + FN);
                    double precision = (double) TM / (double) (TM + FM);
                    double fscore = 2*(recall*precision)/(recall + precision);
                    LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                            threshold, recall, precision,fscore, TM, FM, TN, FN));
                    if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
                    fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                            threshold, recall, precision,fscore));
                }
                LOG.info("---");
                fsdos.close();
            }
        }
    }

    /**
     * Dice on RBF Static/Weighted
     */
    public void test11() throws DatasetException, IOException, BloomFilterEncodingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsB/schema/recordsB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/recordsA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/recordsB/avro"));

        Properties properties = new Properties();
        FSDataInputStream fsdis = fs.open(statsPath);
        properties.load(fsdis);
        fsdis.close();
        DatasetStatistics stats = new DatasetStatistics();
        stats.fromProperties(properties);

        final double[] weights = new double[fieldNamesA.length];
        int i = 0;
        for (String fieldName : fieldNamesA) {
            weights[i] = stats.getFieldStatistics().get(fieldName).getNormalizedRange();
            i++;
        }
        int[] S = new int[]{128, 256, 512, 1024};
        int q = 2;
        int k = 10;
        for (int sf : S) {
            final int[] fbfNs = new int[fieldNamesA.length];
            Arrays.fill(fbfNs, sf);

            LOG.info("---");
            RowBloomFilterEncoding encodingA = new RowBloomFilterEncoding(fbfNs, weights, k, q);
            encodingA.makeFromSchema(schemaA, fieldNamesA, new String[]{idFieldName});
            encodingA.initialize();
            final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
            for (int r = 0; r < recordsA.length; r++)
                encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);

            BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                    BloomFilterEncodingUtil.basedOnExistingSchema(schemaB, fieldNamesB, included,
                            encodingA.getEncodingSchema(),
                            fieldNamesA));
            encodingB.initialize();
            final GenericRecord[] encodedRecordsB = new GenericRecord[recordsB.length];
            for (int r = 0; r < recordsB.length; r++)
                encodedRecordsB[r] = encodingB.encodeRecord(recordsB[r]);

            LOG.info("RBF Static/Weighted {}", String.format("(%d,%d,%d)", encodingA.getBFN(), q, k));
            LOG.info("---");
            final String fileName = String.format("bench_records_rbf_weighted_static_%d.csv",
                    encodingA.getBFN());
            FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
            fsdos.writeBytes(BENCH_HEADER);
            for (double threshold : thresholds) {
                int TM = 0;
                int FM = 0;
                int TN = 0;
                int FN = 0;
                for (GenericRecord erA : encodedRecordsA) {
                    for (GenericRecord erB : encodedRecordsB) {
                        final BloomFilter bfA = encodingA.retrieveBloomFilter(erA);
                        final BloomFilter bfB = encodingB.retrieveBloomFilter(erB);
                        String idA = erA.get("id").toString();
                        String idB = erB.get("id").toString();
                        boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                        boolean similar = PrivateSimilarityUtil.similarity("dice", bfA, bfB, threshold);
                        if (similar && trueSimilar) TM++;
                        else if (similar && !trueSimilar) FM++;
                        else if (!similar && trueSimilar) FN++;
                        else TN++;
                    }
                }
                double recall = (double) TM / (double) (TM + FN);
                double precision = (double) TM / (double) (TM + FM);
                double fscore = 2 * (recall * precision) / (recall + precision);
                LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                        threshold, recall, precision, fscore, TM, FM, TN, FN));
                if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
                fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                        threshold, recall, precision, fscore));
            }
            LOG.info("---");
            fsdos.close();
        }
    }
    /**
     * Dice on RBF Dynamic/Weighted
     */
    public void test12() throws DatasetException, IOException, BloomFilterEncodingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        final Schema schemaA =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsA/schema/recordsA.avsc"));
        final Schema schemaB =
                DatasetsUtil.loadSchemaFromFSPath(fs, new Path("data/benchmarks/recordsB/schema/recordsB.avsc"));
        final GenericRecord[] recordsA =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaA, new Path("data/benchmarks/recordsA/avro"));
        final GenericRecord[] recordsB =
                DatasetsUtil.loadAvroRecordsFromFSPaths(fs, LIMIT, schemaB, new Path("data/benchmarks/recordsB/avro"));

        Properties properties = new Properties();
        FSDataInputStream fsdis = fs.open(statsPath);
        properties.load(fsdis);
        fsdis.close();
        DatasetStatistics stats = new DatasetStatistics();
        stats.fromProperties(properties);

        final double[] weights = new double[fieldNamesA.length];
        int j = 0;
        for (String fieldName : fieldNamesA) {
            weights[j] = stats.getFieldStatistics().get(fieldName).getNormalizedRange();
            j++;
        }

        int q = 2;
        int[] K = new int[]{10, 15, 30, 50};
        LOG.info("---");
        final double[] avgQgrams = new double[fieldNamesA.length];
        for (int i = 0; i < fieldNamesA.length; i++)
            avgQgrams[i] = stats.getFieldStatistics()
                    .get(fieldNamesA[i])
                    .getQgramCount(q);
        for (int k : K) {
            RowBloomFilterEncoding encodingA = new RowBloomFilterEncoding(avgQgrams, weights, k, q);
            encodingA.makeFromSchema(schemaA, fieldNamesA, new String[]{idFieldName});
            encodingA.initialize();
            final GenericRecord[] encodedRecordsA = new GenericRecord[recordsA.length];
            for (int r = 0; r < recordsA.length; r++)
                encodedRecordsA[r] = encodingA.encodeRecord(recordsA[r]);

            BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                    BloomFilterEncodingUtil.basedOnExistingSchema(schemaB, fieldNamesB, included,
                            encodingA.getEncodingSchema(),
                            fieldNamesA));
            encodingB.initialize();
            final GenericRecord[] encodedRecordsB = new GenericRecord[recordsB.length];
            for (int r = 0; r < recordsB.length; r++)
                encodedRecordsB[r] = encodingB.encodeRecord(recordsB[r]);

            LOG.info("RBF Dynamic/weighted {}", String.format("(%d,%d,%d)", encodingA.getBFN(), q, k));
            LOG.info("---");
            final String fileName = String.format("bench_records_rbf_weighted_dynamic_%d.csv",k);
            FSDataOutputStream fsdos = fs.create(new Path("data/benchmarks", fileName));
            fsdos.writeBytes(BENCH_HEADER);
            for (double threshold : thresholds) {
                int TM = 0;
                int FM = 0;
                int TN = 0;
                int FN = 0;
                for (GenericRecord erA : encodedRecordsA) {
                    for (GenericRecord erB : encodedRecordsB) {
                        final BloomFilter bfA = encodingA.retrieveBloomFilter(erA);
                        final BloomFilter bfB = encodingB.retrieveBloomFilter(erB);
                        String idA = erA.get("id").toString();
                        String idB = erB.get("id").toString();
                        boolean trueSimilar = idA.substring(4, 7).equals(idB.substring(4, 7));
                        boolean similar = PrivateSimilarityUtil.similarity("dice", bfA, bfB, threshold);
                        if (similar && trueSimilar) TM++;
                        else if (similar && !trueSimilar) FM++;
                        else if (!similar && trueSimilar) FN++;
                        else TN++;
                    }
                }
                double recall = (double) TM / (double) (TM + FN);
                double precision = (double) TM / (double) (TM + FM);
                double fscore = 2*(recall*precision)/(recall + precision);
                LOG.info(String.format("%.2f,%.2f,%.2f,%.2f \t %d,%d,%d,%d",
                        threshold, recall, precision,fscore, TM, FM, TN, FN));
                if (Double.isNaN(recall) || Double.isNaN(precision)) continue;
                fsdos.writeBytes(String.format("%.5f,%.5f,%.5f,%.5f\n",
                        threshold, recall, precision,fscore));
            }
            LOG.info("---");
            fsdos.close();
        }
    }
}

