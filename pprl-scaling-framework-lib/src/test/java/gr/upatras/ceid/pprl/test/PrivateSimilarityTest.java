package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.datasets.DatasetsUtil;
import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.matching.PrivateSimilarityUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrivateSimilarityTest {

    private static Logger LOG = LoggerFactory.getLogger(PrivateSimilarityTest.class);

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
        LOG.info("Dice : {} ",PrivateSimilarityUtil.dice(bf1, bf2));
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
        LOG.info("Dice : {} ",PrivateSimilarityUtil.dice(bf1,bf2));
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
        LOG.info("Dice : {} ",PrivateSimilarityUtil.dice(bf1,bf2));
    }

    // TODO do some similarity check on the voters dataset to refine parameters blocking test.
}
