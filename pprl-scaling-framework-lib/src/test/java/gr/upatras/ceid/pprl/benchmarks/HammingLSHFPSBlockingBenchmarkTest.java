package gr.upatras.ceid.pprl.benchmarks;

import gr.upatras.ceid.pprl.blocking.BlockingException;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlocking;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlockingResult;
import gr.upatras.ceid.pprl.blocking.HammingLSHBlockingUtil;
import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetStatistics;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.ExpectationMaximization;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityUtil;
import gr.upatras.ceid.pprl.matching.SimilarityVectorFrequencies;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HammingLSHFPSBlockingBenchmarkTest {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHFPSBlockingBenchmarkTest.class);

    private static FileSystem fs;

    static {
        try {
            fs = FileSystem.getLocal(new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static final String[] VOTER_HEADER = {"id", "surname", "name", "address", "city"};
    private static Schema.Type[] VOTER_TYPES = {
            Schema.Type.STRING, Schema.Type.STRING,
            Schema.Type.STRING, Schema.Type.STRING,
            Schema.Type.STRING
    };
    private static String[] VOTER_DOCS = {
            "Voter ID", "Surname", "Name", "Address", "City"
    };
    private static String[] DATASET_NAMES =
            {"voters_a", "voters_b"};


    private static Pattern VOTER_ID_PATTERN = Pattern.compile("[a|b]([0-9]+)_{0,1}[0-9]*");


    private static GenericRecord[][] SAMPLES = {
            new GenericRecord[100000],
            new GenericRecord[10000]
    };

    private static DatasetStatistics STATS = new DatasetStatistics();

    private static String[] SELECTED_FIELDS = {"surname", "name", "address"};
    private static String[] INCLUDED_FIELDS = {"id"};

    private static int ENCODING_K = 15;
    private static int ENCODING_N = 4096;
    private static int ENCODING_FBF_N = 1024;
    private static int ENCODING_Q = 2;

    private static final String[] ENCODING_NAMES = {
            "clk",
            "fbf_s",
            "fbf_d",
            "rbf_us",
            "rbf_ud",
            "rbf_ws",
            "rbf_wd"
    };

    private static final Map<String,DescriptiveStatistics> HAMMING_STATS = new HashMap();
    private static final Map<String,DescriptiveStatistics> JACCARD_STATS = new HashMap();
    private static final Map<String,DescriptiveStatistics> DICE_STATS = new HashMap();
    static {
        for(String name : ENCODING_NAMES) {
            HAMMING_STATS.put(name,new DescriptiveStatistics());
            JACCARD_STATS.put(name,new DescriptiveStatistics());
            DICE_STATS.put(name,new DescriptiveStatistics());
        }
    }

    private static final int HAMMING_LSH_K = 30;
    private static final double[] HAMMING_DELTAS = {0.005, 0.001, 0.0005, 0.0001, 0.00005};

    @Test
    public void test00() throws IOException, DatasetException {
        LOG.info("CSV to avro");
        Schema schemaVotersA = DatasetsUtil.avroSchema(
                "voters_a", "Voters Registration", "pprl.datasets",
                VOTER_HEADER, VOTER_TYPES, VOTER_DOCS);
        final Path pa = DatasetsUtil.csv2avro(fs, schemaVotersA, DATASET_NAMES[0],
                new Path(fs.getWorkingDirectory(), "data/benchmarks"),
                new Path(fs.getWorkingDirectory(), "data/benchmarks/" + DATASET_NAMES[0] + ".csv"));
        LOG.info("Saved at path {} ", pa);

        Schema schemaVotersB = DatasetsUtil.avroSchema(
                "voters_b", "Voters Registration", "pprl.datasets",
                VOTER_HEADER, VOTER_TYPES, VOTER_DOCS);
        final Path pb = DatasetsUtil.csv2avro(fs, schemaVotersB, DATASET_NAMES[1],
                new Path(fs.getWorkingDirectory(), "data/benchmarks"),
                new Path(fs.getWorkingDirectory(), "data/benchmarks/" + DATASET_NAMES[1] + ".csv"));
        LOG.info("Saved at path {} ", pb);
    }

    @Test
    public void test01() throws IOException, DatasetException {
        LOG.info("Sample and Stats");
        final Path[][] paths = new Path[][]{
                DatasetsUtil.retrieveDatasetDirectories(fs, DATASET_NAMES[0], new Path("data/benchmarks")),
                DatasetsUtil.retrieveDatasetDirectories(fs, DATASET_NAMES[1], new Path("data/benchmarks"))
        };
        final Schema[] schemas = new Schema[]{
                DatasetsUtil.loadSchemaFromFSPath(fs, DatasetsUtil.getSchemaPath(fs, paths[0][2])),
                DatasetsUtil.loadSchemaFromFSPath(fs, DatasetsUtil.getSchemaPath(fs, paths[1][2]))
        };
        SAMPLES[0] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, 1000, schemas[0], paths[0][1]);
        SAMPLES[1] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, 100, schemas[1], paths[1][1]);
        STATS.setRecordCount(SAMPLES[1].length);
        STATS.setFieldNames(SELECTED_FIELDS);
        DatasetStatistics.calculateQgramStatistics(SAMPLES[1], schemas[1], STATS, SELECTED_FIELDS);
        SimilarityVectorFrequencies frequencies =
                SimilarityUtil.vectorFrequencies(SAMPLES[0], SAMPLES[1], SELECTED_FIELDS, SELECTED_FIELDS);
        ExpectationMaximization estimator = new ExpectationMaximization(SELECTED_FIELDS, 0.95, 0.1, 0.001);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        STATS.setEmPairsCount(estimator.getPairCount());
        STATS.setEmAlgorithmIterations(estimator.getIteration());
        STATS.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                STATS, SELECTED_FIELDS,
                estimator.getM(), estimator.getU());

        LOG.info(DatasetStatistics.prettyStats(STATS));
    }

    @Test
    public void test02() throws IOException, DatasetException, BloomFilterEncodingException, BlockingException {
        LOG.info("Encoding datasets");
        final int fieldCount = SELECTED_FIELDS.length;
        // load records and schema
        final Path[][] paths = new Path[][]{
                DatasetsUtil.retrieveDatasetDirectories(fs, DATASET_NAMES[0], new Path("data/benchmarks")),
                DatasetsUtil.retrieveDatasetDirectories(fs, DATASET_NAMES[1], new Path("data/benchmarks"))
        };
        final Schema[] schemas = new Schema[]{
                DatasetsUtil.loadSchemaFromFSPath(fs, DatasetsUtil.getSchemaPath(fs, paths[0][2])),
                DatasetsUtil.loadSchemaFromFSPath(fs, DatasetsUtil.getSchemaPath(fs, paths[1][2]))
        };
        SAMPLES[0] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schemas[0], paths[0][1]);
        SAMPLES[1] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schemas[1], paths[1][1]);

        GenericRecord[][] ENC_SAMPLES = {
                new GenericRecord[SAMPLES[0].length],
                new GenericRecord[SAMPLES[1].length]
        };


        // get specific stats
        final double[] avgQGrams = new double[SELECTED_FIELDS.length];
        final double[] weights = new double[SELECTED_FIELDS.length];
        for (int i = 0; i < SELECTED_FIELDS.length; i++) {
            avgQGrams[i] = STATS.getFieldStatistics().get(SELECTED_FIELDS[i]).getQgramCount(ENCODING_Q);
            weights[i] = STATS.getFieldStatistics().get(SELECTED_FIELDS[i]).getNormalizedRange();
        }

        for (String encodingName : ENCODING_NAMES) {
            BloomFilterEncoding[] encodings = new BloomFilterEncoding[2];

            // encoding schema a
            switch (encodingName) {
                case "clk":
                    encodings[0] = BloomFilterEncodingUtil.instanceFactory("CLK",
                            fieldCount, ENCODING_N, ENCODING_FBF_N, ENCODING_K, ENCODING_Q, avgQGrams, weights);
                    break;
                case "fbf_s":
                    encodings[0] = BloomFilterEncodingUtil.instanceFactory("FBF",
                            fieldCount, ENCODING_N, ENCODING_FBF_N, ENCODING_K, ENCODING_Q, avgQGrams, weights);
                    break;
                case "fbf_d":
                    encodings[0] = BloomFilterEncodingUtil.instanceFactory("FBF",
                            fieldCount, ENCODING_N, 0, ENCODING_K, ENCODING_Q, avgQGrams, weights);
                    break;
                case "rbf_us":
                    encodings[0] = BloomFilterEncodingUtil.instanceFactory("RBF",
                            fieldCount, ENCODING_N, ENCODING_FBF_N, ENCODING_K, ENCODING_Q, avgQGrams, weights);
                    break;
                case "rbf_ud":
                    encodings[0] = BloomFilterEncodingUtil.instanceFactory("RBF",
                            fieldCount, ENCODING_N, 0, ENCODING_K, ENCODING_Q, avgQGrams, weights);
                    break;
                case "rbf_ws":
                    encodings[0] = BloomFilterEncodingUtil.instanceFactory("RBF",
                            fieldCount, 0, ENCODING_FBF_N, ENCODING_K, ENCODING_Q, avgQGrams, weights);
                    break;
                case "rbf_wd":
                    encodings[0] = BloomFilterEncodingUtil.instanceFactory("RBF",
                            fieldCount, 0, 0, ENCODING_K, ENCODING_Q, avgQGrams, weights);
                    break;
                default:
                    throw new IOException("Shouldn't be here");
            }
            encodings[0].makeFromSchema(schemas[0], SELECTED_FIELDS, INCLUDED_FIELDS);
            encodings[0].initialize();

            // encoding schema b
            encodings[1] = BloomFilterEncodingUtil.setupNewInstance(
                    BloomFilterEncodingUtil.basedOnExistingSchema(
                            schemas[1], SELECTED_FIELDS, INCLUDED_FIELDS, encodings[0].getEncodingSchema(),
                            SELECTED_FIELDS));
            encodings[1].initialize();


            // encode data
            for (int i = 0; i < 2; i++) {
                int r = 0;
                for (GenericRecord record : SAMPLES[i]) {
                    ENC_SAMPLES[i][r] = encodings[i].encodeRecord(record);
                    r++;
                }
                LOG.info("Encoding : {} , {} ",encodingName,DATASET_NAMES[i] );
                DatasetsUtil.saveSchemaToFSPath(fs, encodings[i].getEncodingSchema(),
                        new Path("data/benchmarks/" + encodingName + "_" + DATASET_NAMES[i] + ".avsc"));
                DatasetsUtil.saveAvroRecordsToFSPath(fs, ENC_SAMPLES[i], encodings[i].getEncodingSchema(),
                        new Path("data/benchmarks"), encodingName + "_" + DATASET_NAMES[i], 1);
            }

        }
    }

    @Test
    public void test03() throws IOException, DatasetException, BloomFilterEncodingException, BlockingException {
        final String BENCHMARK_HEADER = "enc_type,S,threshold,ptheta,pthetaK,delta,K,Lopt,Lc,L,C,bsize,bt,fpst,tt,fpc,mpc,bm\n";
        final StringBuilder BENCHMARK_REPORT_BUILDER = new StringBuilder(BENCHMARK_HEADER);
        String[] ENCODING_NAMES = {
                "clk",
                "fbf_s",
                "fbf_d",
                "rbf_us",
                "rbf_ud",
                "rbf_ws",
                "rbf_wd"
        };
        for (String encodingName : ENCODING_NAMES) {
            final Path[] avroPaths = {
                    new Path("data/benchmarks",encodingName + "_" + DATASET_NAMES[0] + ".avro"),
                    new Path("data/benchmarks",encodingName + "_" + DATASET_NAMES[1] + ".avro")
            };
            final Path[] schemaPaths = {
                    new Path("data/benchmarks",encodingName + "_" + DATASET_NAMES[0] + ".avsc"),
                    new Path("data/benchmarks",encodingName + "_" + DATASET_NAMES[1] + ".avsc")
            };

            final Schema[] schemas = new Schema[]{
                    DatasetsUtil.loadSchemaFromFSPath(fs,schemaPaths[0]),
                    DatasetsUtil.loadSchemaFromFSPath(fs,schemaPaths[1])
            };

            GenericRecord[][] ENC_SAMPLES = new GenericRecord[2][];
            ENC_SAMPLES[0] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schemas[0], avroPaths[0]);
            ENC_SAMPLES[1] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schemas[1], avroPaths[1]);

            BloomFilterEncoding[] encodings = {
                    BloomFilterEncodingUtil.setupNewInstance(schemas[0]),
                    BloomFilterEncodingUtil.setupNewInstance(schemas[1])
            };

            for (int rb = 0; rb < 100; rb++) {
                for (int ra = 0; ra < 1000; ra++) {
                    final GenericRecord encodedRecordA = ENC_SAMPLES[0][ra];
                    final GenericRecord encodedRecordB = ENC_SAMPLES[1][rb];
                    Matcher matcherA = VOTER_ID_PATTERN.matcher(String.valueOf(encodedRecordA.get("id")));
                    Matcher matcherB = VOTER_ID_PATTERN.matcher(String.valueOf(encodedRecordB.get("id")));
                    if (!matcherA.matches() || !matcherB.matches()) throw new IOException("Wrong id format.");
                    double hamming = PrivateSimilarityUtil.hamming(
                            encodings[0].retrieveBloomFilter(encodedRecordA),
                            encodings[1].retrieveBloomFilter(encodedRecordB)
                    );
                    if (matcherA.group(1).equals(matcherB.group(1))) {
                        HAMMING_STATS.get(encodingName).addValue(hamming);
                    }
                }
            }

            LOG.info(String.format("%s : Hamming Distance Stats : MIN=%d AVG=%d MAX=%d Std.Dev=%.2f",
                    encodingName,
                    (int) HAMMING_STATS.get(encodingName).getMin(),
                    (int) HAMMING_STATS.get(encodingName).getMean(),
                    (int) HAMMING_STATS.get(encodingName).getMax(),
                    HAMMING_STATS.get(encodingName).getStandardDeviation()));

            int hammingThrehold = (int) (encodingName.contains("d") ?
                    Math.round((double)encodings[0].getBFN()*0.1d) :
                    HAMMING_STATS.get(encodingName).getMax());
            BENCHMARK_REPORT_BUILDER.append(runBenchmark(encodingName,ENC_SAMPLES, encodings, hammingThrehold));
        }
        LOG.info("\n\n\n\n\n\n"+ BENCHMARK_REPORT_BUILDER.toString());
    }



    private String runBenchmark(final String encodingName,
                                final GenericRecord[][] ENC_SAMPLES,
                                final BloomFilterEncoding[] encodings,
                                final int hammingThreshold) throws BlockingException {
        int S = encodings[0].getBFN();
        final StringBuilder EXPERIMENT_REPORT_BUILDER = new StringBuilder();
        final StringBuilder COMMON_REPORT_BUILDER = new StringBuilder();
        COMMON_REPORT_BUILDER.append(encodingName).append(',');
        COMMON_REPORT_BUILDER.append(S).append(',');

        double ptheta = HammingLSHBlockingUtil.probOfBaseHashMatch(hammingThreshold, S);
        int K = HAMMING_LSH_K;
        double pthetaK = HammingLSHBlockingUtil.probHashMatch(ptheta,K);

        COMMON_REPORT_BUILDER
                .append(hammingThreshold).append(',')
                .append(String.format("%.2f", ptheta)).append(',')
                .append(String.format("%.2f", pthetaK)).append(',');
        final String reportSoFar = COMMON_REPORT_BUILDER.toString();
        for (double delta : HAMMING_DELTAS) {
            final StringBuilder hfpsBuilder = new StringBuilder(reportSoFar);
            final int[] FPS_OPT_PARAMS = HammingLSHBlockingUtil.optimalParameters(hammingThreshold,S,delta,K);
            final short C = (short) FPS_OPT_PARAMS[0];
            final int L = FPS_OPT_PARAMS[1];
            hfpsBuilder
                    .append((delta <= 0.0001) ? String.format("%.5f", delta) : delta).append(',')
                    .append(K).append(',')
                    .append(FPS_OPT_PARAMS[2]).append(',')
                    .append(FPS_OPT_PARAMS[3]).append(',')
                    .append(L).append(',')
                    .append(C).append(',');

            final HammingLSHBlocking blocking = new HammingLSHBlocking(L, K, encodings[0], encodings[1]);
            blocking.runHLSH(ENC_SAMPLES[1], "id");
            blocking.runFPS(ENC_SAMPLES[0], "id", C, hammingThreshold);
            final HammingLSHBlockingResult result = blocking.getResult();
            hfpsBuilder
                    .append(result.getBobBlockingSize()).append(',')
                    .append(result.getBobBlockingTime()).append(',')
                    .append(result.getFpsTime()).append(',')
                    .append(result.getBobBlockingTime() + result.getFpsTime()).append(",")
                    .append(result.getFrequentPairsCount()).append(',')
                    .append(result.getMatchedPairsCount()).append(',')
                    .append(result.getTrullyMatchedCount());

            hfpsBuilder.append('\n');
            EXPERIMENT_REPORT_BUILDER.append(hfpsBuilder.toString());
        }
        return EXPERIMENT_REPORT_BUILDER.toString();
    }
}
