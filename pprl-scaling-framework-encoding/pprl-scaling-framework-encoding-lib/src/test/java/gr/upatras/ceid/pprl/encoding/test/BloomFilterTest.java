package gr.upatras.ceid.pprl.encoding.test;

import gr.upatras.ceid.pprl.encoding.BloomFilter;
import gr.upatras.ceid.pprl.encoding.QGram;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

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

    @Test
    public void test1() {
        final BloomFilter bloomFilter = new BloomFilter(1024,30);
        LOG.info("before FPP={}", bloomFilter.calcFPP());
        final Map<String,Integer> bigramsHashed = new TreeMap<String,Integer>();
        final Map<String,int[]> bigramsPositionsHashed = new TreeMap<String,int[]>();
        String[] bigrams = QGram.generateQGrams(LOREM_IPSUM, 2);
        if(bigrams == null) { LOG.error("bigrams is null"); return; }
        for(String bigram : bigrams) {
            if(bigramsHashed.containsKey(bigram)) bigramsHashed.put(bigram,bigramsHashed.get(bigram) + 1);
            else bigramsHashed.put(bigram,1);
            int[] positions = bloomFilter.addData(bigram.getBytes(Charset.forName("UTF-8")));
            if(!bigramsPositionsHashed.containsKey(bigram))
                bigramsPositionsHashed.put(bigram, positions);
        }
        LOG.info("Distinct bigrams : {}",bigramsHashed.keySet().size());
        LOG.info("Distinct bigrams : {}",bigramsPositionsHashed.keySet().size());
        LOG.info("Added elements : {}",bloomFilter.getAddedElementsCount());
        LOG.info("#1 : {}",bloomFilter.getOnesCount());
        LOG.info("#0 : {}",bloomFilter.getZeroesCount());
        LOG.info("#bitstring : {}",bloomFilter.toString());
        LOG.info("after inserts FPP={}", bloomFilter.calcFPP());
    }
}
