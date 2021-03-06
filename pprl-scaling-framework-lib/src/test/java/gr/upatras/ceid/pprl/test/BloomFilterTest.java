package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.qgram.QGramUtil;
import org.apache.avro.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest {

    private static Logger LOG = LoggerFactory.getLogger(BloomFilterTest.class);

    private static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
            "Suspendisse ultrices, felis ut vestibulum fringilla, massa tellus blandit lacus, ac faucibus ipsum" +
            " ante eu leo. Ut felis risus, lobortis et felis a, ullamcorper aliquet nisi. Phasellus vitae rutrum " +
            "diam. Vivamus ultricies ligula in nulla porta, eleifend blandit urna volutpat. Suspendisse lobortis massa " +
            "tincidunt massa vulputate, quis auctor purus luctus. Donec nec nunc sollicitudin, ultricies nulla eget," +
            " varius urna. Suspendisse quis mattis erat, et posuere nisi. Sed interdum nibh sed tempor placerat. " +
            "Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Donec vel" +
            " iaculis diam. Interdum et malesuada fames ac ante ipsum primis in faucibus. Fusce ut auctor felis. " +
            "Pellentesque hendrerit, enim in vehicula egestas, orci velit venenatis elit, eget fringilla dolor lacus " +
            "a justo. Aenean eros massa, vulputate sed posuere et, pulvinar tincidunt mauris.";

    private static final int N = 1024;
    private static final int K = 2;

    @Test
    public void test1() throws InvalidKeyException, NoSuchAlgorithmException {
        final BloomFilter bloomFilter = new BloomFilter(N,K);
        Set<String> distinctBigrams = new TreeSet<String>();
        String[] bigrams = QGramUtil.generateQGrams(LOREM_IPSUM, Schema.Type.STRING, 2);
        if(bigrams == null) { LOG.error("bigrams is null"); return; }
        for(String bigram : bigrams) {
            bloomFilter.addData(bigram);
            distinctBigrams.add(bigram);
        }
        LOG.info("Bytes length : {}",bloomFilter.getByteArray().length);
        LOG.info("Distinct bigrams : {}", distinctBigrams.size());
        LOG.info("#1 : {}",bloomFilter.getOnesCount());
        LOG.info("#0 : {}",bloomFilter.getZeroesCount());
        assertEquals(bloomFilter.getOnesCount(),bloomFilter.countOnes());
        assertEquals(bloomFilter.getZeroesCount(),bloomFilter.countZeroes());
        LOG.info("#bitstring : {}",bloomFilter.toString());
        LOG.info("#hexstring : {}",bloomFilter.toHexString());
        LOG.info("--New Copy--");
        final BloomFilter bloomFilter1 = new BloomFilter(N,K,bloomFilter.getByteArray());
        LOG.info("Bytes length : {}",bloomFilter1.getByteArray().length);
        LOG.info("#1 : {}",bloomFilter1.getOnesCount());
        LOG.info("#0 : {}",bloomFilter1.getZeroesCount());
        assertEquals(bloomFilter1.getOnesCount(),bloomFilter1.countOnes());
        assertEquals(bloomFilter1.getZeroesCount(),bloomFilter1.countZeroes());
        LOG.info("#bitstring : {}",bloomFilter1.toString());
        LOG.info("#hexstring : {}",bloomFilter1.toHexString());
        LOG.info("clearing bloom filter");
        bloomFilter.clear();
        LOG.info("#1 : {}",bloomFilter1.getOnesCount());
        LOG.info("#0 : {}",bloomFilter1.getZeroesCount());
        assertEquals(bloomFilter1.getOnesCount(),bloomFilter1.countOnes());
        assertEquals(bloomFilter1.getZeroesCount(),bloomFilter1.countZeroes());
    }

    @Test
    public void test2() {
        byte[] bytes = new byte[(int) Math.ceil(N/(double)8)];
        List<Integer> positions = Arrays.asList(0,2,4,5,6,7,8,1023,1,3,512);
        for (int i : positions) setBit(i,bytes);
        boolean isCorrect = true;
        for (int i = 0 ;i <1024 ; i++) {
            boolean isSet = positions.contains(i);
            isCorrect &= isSet ? getBit(i, bytes) : !getBit(i, bytes);
        }
        assertTrue(isCorrect);

        List<Integer> unsetPositions =  Arrays.asList(1,3,5,7,1023);
        for (int i : unsetPositions) unSetBit(i,bytes);
        for (int i = 0 ;i <1024 ; i++) {
            boolean isSet = positions.contains(i) && !unsetPositions.contains(i);
            isCorrect &= isSet ? getBit(i, bytes) : !getBit(i, bytes);
        }
        assertTrue(isCorrect);
    }

    private boolean getBit(int i, byte[] bytes) {
        return ((bytes[i/8] & (1<<(i%8))) != 0);
    }

    private void setBit(int i, byte[] bytes) {
        bytes[i/8] = (byte) (bytes[i/8]  | (1<<(i%8)));
    }

    private void unSetBit(int i, byte[] bytes) {
        bytes[i/8] = (byte) (bytes[i/8]  & ~(1 << (i%8)));
    }
}
