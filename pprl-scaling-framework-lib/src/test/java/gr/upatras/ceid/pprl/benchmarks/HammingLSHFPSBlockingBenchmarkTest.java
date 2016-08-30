
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HammingLSHFPSBlockingBenchmarkTest {

    private static final Logger LOG = LoggerFactory.getLogger(HammingLSHFPSBlockingBenchmarkTest.class);

    private static FileSystem fs;
    static {
        try {
            fs =FileSystem.getLocal(new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    private static final String[] VOTER_HEADER = {"id","surname","name","address","city"};
    private static Schema.Type[] VOTER_TYPES = {
            Schema.Type.STRING,Schema.Type.STRING,
            Schema.Type.STRING,Schema.Type.STRING,
            Schema.Type.STRING
    };
    private static String[] VOTER_DOCS = {
            "Voter ID","Surname","Name","Address","City"
    };
    private static String[][] DATASET_NAMES = {
//            {"voters_a","voters_b"},
            {"big_voters_a","big_voters_b"}
    };
    private static Pattern VOTER_ID_PATTERN = Pattern.compile("[a|b]([0-9]+)_{0,1}[0-9]*");


    private static GenericRecord[][] SAMPLES = new GenericRecord[2][];
    private static DatasetStatistics STATS = new DatasetStatistics();

    private static String[] SELECTED_FIELDS = {"surname","name","address"};
    private static String[] INCLUDED_FIELDS = {"id"};

    private static int ENCODING_K = 15;
    private static int ENCODING_N = 4096;
    private static int ENCODING_FBF_N = 1024;
    private static int ENCODING_Q = 2;
    private final String[] ENCODING_NAMES = {
            "clk",
            "fbf_s","fbf_d",
            "rbf_us","rbf_ud",
            "rbf_ws","rbf_wd"
    };

    private static final String HAMMING_SIMILARITY_METHOD_NAME = "hamming";
    private static final int HAMMING_LSH_K = 30;
    private static final double[] HAMMING_DELTAS = {0.01,0.005,0.001,0.0005,0.0001};

    @Test
    public void test00() throws IOException, DatasetException {
        LOG.info("CSV to avro");
        Schema schemaVotersA = DatasetsUtil.avroSchema(
                "voters_a", "Voters Registration", "pprl.datasets",
                VOTER_HEADER,VOTER_TYPES,VOTER_DOCS);

        Schema schemaVotersB = DatasetsUtil.avroSchema(
                "voters_b", "Voters Registration", "pprl.datasets",
                VOTER_HEADER, VOTER_TYPES, VOTER_DOCS);
        for(String[] name : DATASET_NAMES) {
            for(int i : new int[]{0,1}) {
                final Path p = DatasetsUtil.csv2avro(fs,i==0?schemaVotersA:schemaVotersB,name[i],
                        new Path(fs.getWorkingDirectory(),"data/benchmarks"),
                        new Path(fs.getWorkingDirectory(), "data/benchmarks/" + name[i] +".csv"));
                LOG.info("Saved at path {} ", p);
            }
        }
    }

    @Test
    public void test01() throws IOException, DatasetException {
        LOG.info("Sample and Stats");
        String[] datasetNames = {"big_voters_a","big_voters_b"};
        final Path[][] paths = new Path[][]{
                DatasetsUtil.retrieveDatasetDirectories(fs, datasetNames[0], new Path("data/benchmarks")),
                DatasetsUtil.retrieveDatasetDirectories(fs, datasetNames[1], new Path("data/benchmarks"))
        };
        final Schema[] schemas = new Schema[]{
                DatasetsUtil.loadSchemaFromFSPath(fs,DatasetsUtil.getSchemaPath(fs,paths[0][2])),
                DatasetsUtil.loadSchemaFromFSPath(fs,DatasetsUtil.getSchemaPath(fs,paths[1][2]))
        };
        SAMPLES[0] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs,1000,schemas[0],paths[0][1]);
        SAMPLES[1] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs,100,schemas[1],paths[1][1]);
        STATS.setRecordCount(SAMPLES[0].length);
        STATS.setFieldNames(SELECTED_FIELDS);
        DatasetStatistics.calculateQgramStatistics(SAMPLES[0], schemas[0], STATS, SELECTED_FIELDS);
        SimilarityVectorFrequencies frequencies =
                SimilarityUtil.vectorFrequencies(SAMPLES[0],SAMPLES[1], SELECTED_FIELDS,SELECTED_FIELDS);
        ExpectationMaximization estimator = new ExpectationMaximization (SELECTED_FIELDS,0.95,0.1,0.001);
        estimator.runAlgorithm(frequencies);

        // using m,u,p estimates complete the statistics of the dataset
        STATS.setEmPairsCount(estimator.getPairCount());
        STATS.setEmAlgorithmIterations(estimator.getIteration());
        STATS.setP(estimator.getP());
        DatasetStatistics.calculateStatsUsingEstimates(
                STATS,SELECTED_FIELDS,
                estimator.getM(),estimator.getU());

        LOG.info(DatasetStatistics.prettyStats(STATS));
    }


    @Test
    public void test02() throws IOException, DatasetException, BloomFilterEncodingException, BlockingException {
        final int fieldCount = SELECTED_FIELDS.length;
        final String BENCHMARK_HEADER = "enc_type,S,threshold,DELTA,ptheta,pthteaK,K,Lopt,L,Lc,C,bsize,bt,fpst,tt,fpc,mpc,bm\n";
        final StringBuilder BENCHMARK_REPORT_BUILDER = new StringBuilder(BENCHMARK_HEADER);
		int run = 0;
        for(String[] datasetNames : DATASET_NAMES) {
            for(String encodingName : ENCODING_NAMES) {
                // load records and schema
                final Path[][] paths = new Path[][]{
                        DatasetsUtil.retrieveDatasetDirectories(fs, datasetNames[0], new Path("data/benchmarks")),
                        DatasetsUtil.retrieveDatasetDirectories(fs, datasetNames[1], new Path("data/benchmarks"))
                };
                final Schema[] schemas = new Schema[]{
                        DatasetsUtil.loadSchemaFromFSPath(fs, DatasetsUtil.getSchemaPath(fs, paths[0][2])),
                        DatasetsUtil.loadSchemaFromFSPath(fs, DatasetsUtil.getSchemaPath(fs, paths[1][2]))
                };
                final StringBuilder EXPERIMENT_REPORT_BUILDER = new StringBuilder();
                SAMPLES[0] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schemas[0], paths[0][1]);
                SAMPLES[1] = DatasetsUtil.loadAvroRecordsFromFSPaths(fs, schemas[1], paths[1][1]);

            	// get specific stats
                final double[] avgQGrams = new double[SELECTED_FIELDS.length];
                final double[] weights = new double[SELECTED_FIELDS.length];
                for (int i = 0; i < SELECTED_FIELDS.length; i++) {
                    avgQGrams[i] = STATS.getFieldStatistics().get(SELECTED_FIELDS[i]).getQgramCount(ENCODING_Q);
                    weights[i] = STATS.getFieldStatistics().get(SELECTED_FIELDS[i]).getNormalizedRange();
                }

                // encoding schema a
                BloomFilterEncoding encodingA;
                switch (encodingName) {
                    case "clk":
                        encodingA = BloomFilterEncodingUtil.instanceFactory("CLK",
                                fieldCount, ENCODING_N, ENCODING_FBF_N, ENCODING_K,ENCODING_Q, avgQGrams, weights);
                        break;
                    case "fbf_s":
                        encodingA = BloomFilterEncodingUtil.instanceFactory("FBF",
                                fieldCount, ENCODING_N, ENCODING_FBF_N, ENCODING_K,ENCODING_Q, avgQGrams, weights);

                        break;
                    case "fbf_d":
                        encodingA = BloomFilterEncodingUtil.instanceFactory("FBF",
                                fieldCount, ENCODING_N, 0, ENCODING_K,ENCODING_Q, avgQGrams, weights);
                        break;
                    case "rbf_us":
                        encodingA = BloomFilterEncodingUtil.instanceFactory("RBF",
                                fieldCount, ENCODING_N, ENCODING_FBF_N, ENCODING_K,ENCODING_Q, avgQGrams, weights);
                        break;
                    case "rbf_ud":
                        encodingA = BloomFilterEncodingUtil.instanceFactory("RBF",
                                fieldCount, ENCODING_N, 0, ENCODING_K,ENCODING_Q, avgQGrams, weights);
                        break;
                    case "rbf_ws":
                        encodingA = BloomFilterEncodingUtil.instanceFactory("RBF",
                                fieldCount, 0, ENCODING_FBF_N, ENCODING_K,ENCODING_Q, avgQGrams, weights);
                        break;
                    case "rbf_wd":
                        encodingA = BloomFilterEncodingUtil.instanceFactory("RBF",
                                fieldCount, 0, ENCODING_FBF_N, ENCODING_K,ENCODING_Q, avgQGrams, weights);
                        break;
                    default:
                        throw new IOException("Shouldn't be here");
                }
                encodingA.makeFromSchema(schemas[0], SELECTED_FIELDS, INCLUDED_FIELDS);
                encodingA.initialize();

                // encoding schema b
                BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(
                        BloomFilterEncodingUtil.basedOnExistingSchema(
                                schemas[1], SELECTED_FIELDS, INCLUDED_FIELDS, encodingA.getEncodingSchema(),
                                SELECTED_FIELDS));
                encodingB.initialize();
                int S = encodingB.getBFN();
                EXPERIMENT_REPORT_BUILDER.append(encodingName).append(',');
                EXPERIMENT_REPORT_BUILDER.append(S).append(',');

                // save encoding schema
                DatasetsUtil.saveSchemaToFSPath(fs,encodingA.getEncodingSchema(),
						new Path("data/benchmarks/" + encodingName + "_voters_a.avsc"));
                DatasetsUtil.saveSchemaToFSPath(fs,encodingB.getEncodingSchema(),
						new Path("data/benchmarks/" + encodingName + "_voters_b.avsc"));

                // encode data
                final BloomFilterEncoding[] encodings = {encodingA, encodingB};
                final GenericRecord[][] ENC_SAMPLES = {
                        new GenericRecord[SAMPLES[0].length],
                        new GenericRecord[SAMPLES[1].length]
                };
                for (int i = 0; i < 2; i++) {
                    int r = 0;
                    for (GenericRecord record : SAMPLES[i]) {
                        ENC_SAMPLES[i][r] = encodings[i].encodeRecord(record);
                        r++;
                    }
                }

                // Profile hamming distance
                DescriptiveStatistics trullyMatchedPairHD = new DescriptiveStatistics();
                DescriptiveStatistics trullyNonMatchedPairHD = new DescriptiveStatistics();

                for (int rb = 0; rb < 100; rb++)
                    for (int ra = 0; ra < 1000; ra++) {
                        final GenericRecord encodedRecordA = ENC_SAMPLES[0][ra];
                        final GenericRecord encodedRecordB = ENC_SAMPLES[1][rb];
                        Matcher matcherA = VOTER_ID_PATTERN .matcher(String.valueOf(encodedRecordA.get("id")));
                        Matcher matcherB = VOTER_ID_PATTERN.matcher(String.valueOf(encodedRecordB.get("id")));
                        if(!matcherA.matches() || !matcherB.matches()) throw new IOException("Wrong id format.");
                        double hamming = PrivateSimilarityUtil.hamming(
                                encodingA.retrieveBloomFilter(encodedRecordA),
                                encodingB.retrieveBloomFilter(encodedRecordB)
                        );
                        if(matcherA.group(1).equals(matcherB.group(1)))
                            trullyMatchedPairHD.addValue(hamming);
                        else
                            trullyNonMatchedPairHD.addValue(hamming);
                    }

                // select hamming threshold max of trully matched distances
                final int hammingThrehold = (int) trullyMatchedPairHD.getMax();
                EXPERIMENT_REPORT_BUILDER.append(hammingThrehold).append(',');

                double ptheta = HammingLSHBlockingUtil.probOfBaseHashMatch(hammingThrehold, S);
                int K = (ptheta < 0.9) ? HAMMING_LSH_K - 20 : HAMMING_LSH_K;
                double pthetaK = HammingLSHBlockingUtil.probHashMatch(ptheta,K);

                final String reportSoFar = EXPERIMENT_REPORT_BUILDER.toString();
                for (double delta : HAMMING_DELTAS) {
                    System.gc();
                    final StringBuilder hfpsBuilder = new StringBuilder(reportSoFar);
                    final int[] limits = HammingLSHBlockingUtil.optimalBlockingGroupCountLimits(delta, pthetaK);
                    final int Lopt = limits[0];
                    final int Lc = limits[1];
                    final short C = HammingLSHBlockingUtil.frequentPairLimit(Lopt, pthetaK);
                    final int L = HammingLSHBlockingUtil.optimalBlockingGroupCountIter(delta, pthetaK);
                    hfpsBuilder
                            .append(String.format("%.4f", delta)).append(',')
                            .append(K).append(',')
                            .append(String.format("%.4f", ptheta)).append(',')
                            .append(String.format("%.4f", pthetaK)).append(',')
                            .append(Lopt).append(',')
                            .append(L).append(',')
                            .append(Lc).append(',')
                            .append(C).append(',');

                    final HammingLSHBlocking blocking = new HammingLSHBlocking(L, K, encodingA, encodingB);
					run++;
					LOG.info("---Run {}---",run);
                    blocking.initialize(ENC_SAMPLES[1]);
                    final HammingLSHBlockingResult result =
                            blocking.runFPS(ENC_SAMPLES[0], "id", ENC_SAMPLES[1], "id", C,
                                    HAMMING_SIMILARITY_METHOD_NAME, hammingThrehold);
                    hfpsBuilder
                            .append(result.getBobBlockingSize()).append(',')
                            .append(result.getBobBlockingTime()).append(',')
                            .append(result.getFpsTime()).append(',')
                            .append(result.getBobBlockingTime() + result.getFpsTime()).append(",")
                            .append(result.getFrequentPairsCount()).append(',')
                            .append(result.getMatchedPairsCount()).append(',')
                            .append(result.getTrullyMatchedCount());
                    hfpsBuilder.append('\n');
                    BENCHMARK_REPORT_BUILDER.append(hfpsBuilder.toString());
                }
            }
        }
        LOG.info("\n\n\n\n\n\n");
        LOG.info("\n\n" + BENCHMARK_REPORT_BUILDER.toString());
    }
}
