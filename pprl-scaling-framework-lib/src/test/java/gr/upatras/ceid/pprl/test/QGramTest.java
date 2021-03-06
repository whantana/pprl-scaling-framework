package gr.upatras.ceid.pprl.test;


import gr.upatras.ceid.pprl.qgram.QGramUtil;
import org.apache.avro.Schema;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class QGramTest {

    private static Logger LOG = LoggerFactory.getLogger(QGramTest.class);

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
        final String small = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.";
        final DescriptiveStatistics stats = new DescriptiveStatistics();

        stats.clear();
        for (int i = 0; i < 5; i++) {
            long start = System.currentTimeMillis();
            String[] unigrams = QGramUtil.generateQGrams(small, Schema.Type.STRING, 1);
            String[] bigrams = QGramUtil.generateQGrams(small, Schema.Type.STRING, 2);
            String[] trigrams = QGramUtil.generateQGrams(small, Schema.Type.STRING, 3);
            long end = System.currentTimeMillis();
            stats.addValue(end - start);
            if(i==0) {
                LOG.info("Small unigrams count = {}", unigrams.length);
                LOG.info("Small bigrams count = {}", bigrams.length);
                LOG.info("Small trigrams count = {}", trigrams.length);
            }
            assertEquals(unigrams.length, QGramUtil.calcQgramsCount(small, Schema.Type.STRING, 1));
            assertEquals(bigrams.length, QGramUtil.calcQgramsCount(small, Schema.Type.STRING, 2));
            assertEquals(trigrams.length, QGramUtil.calcQgramsCount(small, Schema.Type.STRING, 3));
        }
        LOG.info("Took " + stats.getPercentile(50) + " ms.");

        stats.clear();
        for (int i = 0; i < 5; i++) {
            long start = System.currentTimeMillis();
            String[] unigrams = QGramUtil.generateUniqueQGrams(small, Schema.Type.STRING, 1);
            String[] bigrams = QGramUtil.generateUniqueQGrams(small, Schema.Type.STRING, 2);
            String[] trigrams = QGramUtil.generateUniqueQGrams(small, Schema.Type.STRING, 3);
            long end = System.currentTimeMillis();
            stats.addValue(end - start);
            if(i==0) {
                LOG.info("Unique Small unigrams count = {}", unigrams.length);
                LOG.info("Unique Small bigrams count = {}", bigrams.length);
                LOG.info("Unique Small trigrams count = {}", trigrams.length);
            }
            assertEquals(unigrams.length,QGramUtil.calcUniqueQgramsCount(small, Schema.Type.STRING, 1));
            assertEquals(bigrams.length,QGramUtil.calcUniqueQgramsCount(small, Schema.Type.STRING, 2));
            assertEquals(trigrams.length, QGramUtil.calcUniqueQgramsCount(small, Schema.Type.STRING, 3));

        }
        LOG.info("Took " + stats.getPercentile(50) + " ms.");
    }



    @Test
    public void test2() {
        Map<String,Integer> bigramsHashed = new HashMap<String,Integer>();
        String[] bigrams = QGramUtil.generateQGrams(LOREM_IPSUM ,Schema.Type.STRING, 2);
        if(bigrams == null) {LOG.error("bigrams == null"); return;}
        for(String s : bigrams) {
            if(bigramsHashed.containsKey(s)) bigramsHashed.put(s, bigramsHashed.get(s) + 1);
            else bigramsHashed.put(s,1);
        }
        LOG.info("bigrams.length = {}" ,bigrams.length);
        LOG.info("unique.bigrams.length = {}" , bigramsHashed.keySet().size());
        String minS = null;
        Integer minI = null;
        String maxS = null;
        Integer maxI = null;

        for(Map.Entry<String,Integer> entry : bigramsHashed.entrySet()) {
            if(minS == null && minI == null) {
                minS = entry.getKey(); minI = entry.getValue();
                maxS = entry.getKey(); maxI = entry.getValue();
                continue;
            }
            if(entry.getValue() < minI) { minS = entry.getKey(); minI = entry.getValue(); }
            if(entry.getValue() > maxI) { maxS = entry.getKey(); maxI = entry.getValue(); }
        }
        LOG.info("min.bigram.count = ({},{})",minS,minI);
        LOG.info("max.bigram.count = ({},{})",maxS,maxI);

        String[] uniqueBigrams = QGramUtil.generateUniqueQGrams(LOREM_IPSUM, Schema.Type.STRING, 2);
        int uniqueBigramsCount = QGramUtil.calcUniqueQgramsCount(LOREM_IPSUM, Schema.Type.STRING, 2);

        assertEquals(uniqueBigramsCount,uniqueBigrams.length);
        assertEquals(uniqueBigramsCount,bigramsHashed.size());
    }


    @Test
    public void test3() {
        final int small = 2;
        final int medium = 12512;
        final int big = 1010500220;
        final int negative = -12345;
        final int[] ints = new int[4];
        ints[0] = small;
        ints[1] = medium;
        ints[2] = big;
        ints[3] = negative;
        for(int i : ints) {
            String[] unigrams = QGramUtil.generateQGrams(i, Schema.Type.INT, 1);
            if(unigrams != null) {
                LOG.info("for int={}, unigrams count = {}", i, unigrams.length);
                LOG.info("{}", Arrays.toString(unigrams));
            }
            String[] bigrams = QGramUtil.generateQGrams(i, Schema.Type.INT, 2);
            if(bigrams != null) {
                LOG.info("for int={}, bigrams count = {}", i, bigrams.length);
                LOG.info("{}", Arrays.toString(bigrams));
            }
            String[] trigrams = QGramUtil.generateQGrams(i, Schema.Type.INT, 3);
            if(trigrams != null) {
                LOG.info("for int={}, trigrams count = {}", i, trigrams.length);
                LOG.info("{}", Arrays.toString(trigrams));
            }
        }
    }

    @Test
    public void test4() {
        final double small = 2.154;
        final double  medium = 12512.1234;
        final double  big = 1010500220.515;
        final double  negative = -12345.111111;
        final double [] dbls = new double [4];
        dbls[0] = small;
        dbls[1] = medium;
        dbls[2] = big;
        dbls[3] = negative;
        for(double i : dbls) {
            String[] unigrams = QGramUtil.generateQGrams(i, Schema.Type.DOUBLE, 1);
            if(unigrams != null) {
                LOG.info("for int={}, unigrams count = {}", i, unigrams.length);
                LOG.info("{}", Arrays.toString(unigrams));
            }
            String[] bigrams = QGramUtil.generateQGrams(i, Schema.Type.DOUBLE, 2);
            if(bigrams != null) {
                LOG.info("for int={}, bigrams count = {}", i, bigrams.length);
                LOG.info("{}", Arrays.toString(bigrams));
            }
            String[] trigrams = QGramUtil.generateQGrams(i, Schema.Type.DOUBLE, 3);
            if(trigrams != null) {
                LOG.info("for int={}, trigrams count = {}", i, trigrams.length);
                LOG.info("{}", Arrays.toString(trigrams));
            }
        }
    }
}
