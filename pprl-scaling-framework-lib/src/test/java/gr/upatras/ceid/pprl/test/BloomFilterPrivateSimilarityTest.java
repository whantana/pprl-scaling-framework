package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetException;
import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncoding;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingException;
import gr.upatras.ceid.pprl.encoding.BloomFilterEncodingUtil;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
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

import static org.junit.Assert.assertEquals;

public class BloomFilterPrivateSimilarityTest {

    private static Logger LOG = LoggerFactory.getLogger(BloomFilterPrivateSimilarityTest.class);

//    final String[] ENCODING_NAMES = {
//            "clk",
//            "static_fbf","dynamic_fbf",
//            "uniform_rbf_static_fbf","uniform_rbf_dynamic_fbf",
//            "weighted_rbf_static_fbf","weighted_rbf_dynamic_fbf"
//    };

    final String[] ENCODING_NAMES = {
            "clk"
    };

    private static int N = 100;


    @Test
    public void test1() {
        byte[] bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 1;
        bytes[1] = 1;
        bytes[2] = 4;
        LOG.info("BF1 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf1 = new BloomFilter(N,bytes);
        LOG.info("BF2 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf2 = new BloomFilter(N,bytes);

        LOG.info("Hamming : {} ", PrivateSimilarityUtil.hamming(bf1, bf2));
        LOG.info("Hamming1 : {} ",PrivateSimilarityUtil.hamming1(bf1, bf2));
        LOG.info("Jaccard : {} ",PrivateSimilarityUtil.jaccard(bf1, bf2));
        LOG.info("Jaccard1 : {} ",PrivateSimilarityUtil.jaccard1(bf1, bf2));
        LOG.info("Dice : {} ",PrivateSimilarityUtil.dice(bf1, bf2));
        LOG.info("Dice1 : {} ",PrivateSimilarityUtil.dice1(bf1, bf2));
    }

    @Test
    public void test2() {
        byte[] bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 1;
        bytes[1] = 1;
        bytes[2] = 4;
        LOG.info("BF1 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf1 = new BloomFilter(N,bytes);

        bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 1;
        bytes[1] = 1;
        bytes[2] = 5;
        LOG.info("BF2 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf2 = new BloomFilter(N,bytes);


        LOG.info("Hamming : {} ", PrivateSimilarityUtil.hamming(bf1, bf2));
        LOG.info("Hamming1 : {} ",PrivateSimilarityUtil.hamming1(bf1, bf2));
        LOG.info("Jaccard : {} ",PrivateSimilarityUtil.jaccard(bf1, bf2));
        LOG.info("Jaccard1 : {} ",PrivateSimilarityUtil.jaccard1(bf1, bf2));
        LOG.info("Dice : {} ",PrivateSimilarityUtil.dice(bf1, bf2));
        LOG.info("Dice1 : {} ",PrivateSimilarityUtil.dice1(bf1, bf2));
    }

    @Test
    public void test3() {
        byte[] bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 1;
        bytes[1] = 1;
        bytes[2] = 4;
        LOG.info("BF1 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf1 = new BloomFilter(N,bytes);

        bytes = new byte[(int) Math.ceil(N/(double)8)];
        bytes[0] = 0;
        bytes[1] = 0;
        bytes[2] = 4;
        LOG.info("BF2 : {}",DatasetsUtil.prettyBinary(bytes));
        final BloomFilter bf2 = new BloomFilter(N,bytes);

        LOG.info("Hamming : {} ", PrivateSimilarityUtil.hamming(bf1, bf2));
        LOG.info("Hamming1 : {} ",PrivateSimilarityUtil.hamming1(bf1, bf2));
        LOG.info("Jaccard : {} ",PrivateSimilarityUtil.jaccard(bf1, bf2));
        LOG.info("Jaccard1 : {} ",PrivateSimilarityUtil.jaccard1(bf1, bf2));
        LOG.info("Dice : {} ",PrivateSimilarityUtil.dice(bf1, bf2));
        LOG.info("Dice1 : {} ",PrivateSimilarityUtil.dice1(bf1, bf2));
    }

    @Test
    public void test4() throws IOException, DatasetException, BloomFilterEncodingException {
        final FileSystem fs = FileSystem.getLocal(new Configuration());
        for (String encName : ENCODING_NAMES) {
            LOG.info("Working with " + encName );
            final Schema schemaVotersA =
                    DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/voters_a/" + encName + ".avsc"));
            final Schema schemaVotersB =
                    DatasetsUtil.loadSchemaFromFSPath(fs,new Path("data/voters_b/" + encName + ".avsc"));
            final GenericRecord[] recordsA =
                    DatasetsUtil.loadAvroRecordsFromFSPaths(
                            fs,100,schemaVotersA,new Path("data/voters_a/" + encName + ".avro"));
            final GenericRecord[] recordsB =
                    DatasetsUtil.loadAvroRecordsFromFSPaths(
                            fs,10,schemaVotersB,new Path("data/voters_b/" + encName + ".avro"));
            final BloomFilterEncoding encodingA = BloomFilterEncodingUtil.setupNewInstance(schemaVotersA);
            final BloomFilterEncoding encodingB = BloomFilterEncodingUtil.setupNewInstance(schemaVotersB);
            compareRecords(encodingA,encodingB,recordsA,recordsB);
        }
    }

    private void compareRecords(final BloomFilterEncoding encodingA, final BloomFilterEncoding encodingB,
                                final GenericRecord[] encodedRecordsA, final GenericRecord[] encodedRecordsB) {
        DescriptiveStatistics distances[][] = new DescriptiveStatistics[2][3];
        distances[0][0] = new DescriptiveStatistics();
        distances[1][0] = new DescriptiveStatistics();
        distances[0][1] = new DescriptiveStatistics();
        distances[1][1] = new DescriptiveStatistics();
        distances[0][2] = new DescriptiveStatistics();
        distances[1][2] = new DescriptiveStatistics();
        for (GenericRecord encodedRecordB : encodedRecordsB) {
            for (GenericRecord encodedRecordA : encodedRecordsA) {
                double hamming = PrivateSimilarityUtil.hamming(
                        encodingA.retrieveBloomFilter(encodedRecordA),
                        encodingB.retrieveBloomFilter(encodedRecordB)
                );
                double hamming1 = PrivateSimilarityUtil.hamming1(
                        encodingA.retrieveBloomFilter(encodedRecordA),
                        encodingB.retrieveBloomFilter(encodedRecordB)
                );
                assertEquals(hamming, hamming1, 0.01);

                double jaccard = PrivateSimilarityUtil.jaccard(
                        encodingA.retrieveBloomFilter(encodedRecordA),
                        encodingB.retrieveBloomFilter(encodedRecordB)
                );
                double jaccard1 = PrivateSimilarityUtil.jaccard1(
                        encodingA.retrieveBloomFilter(encodedRecordA),
                        encodingB.retrieveBloomFilter(encodedRecordB)
                );
                assertEquals(jaccard,jaccard1,0.01);

                double dice = PrivateSimilarityUtil.dice(
                        encodingA.retrieveBloomFilter(encodedRecordA),
                        encodingB.retrieveBloomFilter(encodedRecordB)
                );
                double dice1 = PrivateSimilarityUtil.dice1(
                        encodingA.retrieveBloomFilter(encodedRecordA),
                        encodingB.retrieveBloomFilter(encodedRecordB)
                );
                assertEquals(dice,dice1,0.01);

                boolean hammingMatch = hamming <= 100;
                boolean jaccardMatch = jaccard >= 0.6;
                boolean diceMatch = dice >= 0.6;

                boolean matchesCompletely = hammingMatch && jaccardMatch && diceMatch;
                boolean matchesPartialy = (hammingMatch || jaccardMatch || diceMatch) &&
                        (!hammingMatch || !jaccardMatch || !diceMatch);
                boolean doesNotMatchAtAll = !(hammingMatch || jaccardMatch || diceMatch);

                boolean shouldMatch = String.valueOf(encodedRecordA.get("id")).charAt(1) ==
                        String.valueOf(encodedRecordB.get("id")).charAt(1);
                boolean shouldNotMatch = !shouldMatch;

                distances[shouldMatch ? 0 : 1][0].addValue(hamming);
                distances[shouldMatch ? 0 : 1][1].addValue(jaccard);
                distances[shouldMatch ? 0 : 1][2].addValue(dice);

                final String pairString = String.format("%s,%s",
                        String.valueOf(encodedRecordA.get("id")),
                        String.valueOf(encodedRecordB.get("id")));


                if(matchesPartialy && shouldMatch) {
                    LOG.info("Pair : {} matches partialy {}.",pairString,
                            String.format("hamming = %f , jaccard = %f dice = %f",hamming,jaccard,dice));
                }

                if(matchesPartialy && shouldNotMatch) {
                    LOG.info("Pair : {} matches partialy while it should not {}.",pairString,
                            String.format("hamming = %f , jaccard = %f dice = %f",hamming,jaccard,dice));
                }

                if(matchesCompletely && shouldNotMatch) {
                    LOG.info("Pair : {} matches completely but it shouldn't. {}.",pairString,
                            String.format("hamming = %f , jaccard = %f dice = %f",hamming,jaccard,dice));
                    LOG.info("Encoded Record A : {}",DatasetsUtil.prettyRecord(encodedRecordA,encodingA.getEncodingSchema()));
                    LOG.info("Encoded Record B : {}",DatasetsUtil.prettyRecord(encodedRecordB,encodingB.getEncodingSchema()));
                }

                if(doesNotMatchAtAll && shouldMatch) {
                    LOG.info("Pair : {} does not match at all but it should. {}.",pairString,
                            String.format("hamming = %f , jaccard = %f dice = %f",hamming,jaccard,dice));
                    LOG.info("Encoded Record A : {}",DatasetsUtil.prettyRecord(encodedRecordA,encodingA.getEncodingSchema()));
                    LOG.info("Encoded Record B : {}",DatasetsUtil.prettyRecord(encodedRecordB,encodingB.getEncodingSchema()));

                }
            }
        }
        LOG.info("Matching Hamming stats : ");
        LOG.info(toStatString(distances[0][0]));
        LOG.info("Non-Matching Hamming stats : ");
        LOG.info(toStatString(distances[1][0]));

        LOG.info("Matching Jaccard stats : ");
        LOG.info(toStatString(distances[0][1]));
        LOG.info("Non-Matching Jaccard stats : ");
        LOG.info(toStatString(distances[1][1]));

        LOG.info("Matching Dice stats : ");
        LOG.info(toStatString(distances[0][2]));
        LOG.info("Non-Matching Dice stats : ");
        LOG.info(toStatString(distances[1][2]));
    }

    private static String toStatString(DescriptiveStatistics stats) {
        return String.format("\n" +
                        "Min: %f, Max: %f \n " +
                        "Avg: %f, Q25: %f ,Q50: %f,Q75: %f \n" +
                        "Std.Dev : %f Var : %f",
                stats.getMin(),stats.getMax(),
                stats.getMean(),stats.getPercentile(25),stats.getPercentile(50),stats.getPercentile(75),
                stats.getStandardDeviation(),stats.getVariance()

        );
    }
}