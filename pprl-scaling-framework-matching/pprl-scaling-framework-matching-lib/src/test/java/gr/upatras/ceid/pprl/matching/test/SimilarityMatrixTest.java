package gr.upatras.ceid.pprl.matching.test;

import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
import gr.upatras.ceid.pprl.matching.SimilarityMatrix;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class SimilarityMatrixTest {

    private static Logger LOG = LoggerFactory.getLogger(SimilarityMatrixTest.class);

    @Test
    public void test0() {

        boolean[][] rows = new boolean[][]{
                {false}, // 0
                {true},  // 1
        };
        assertEquals(rows.length , (1 << 1));
        int rowPos = 0;
        for(boolean[] row : rows) {
            int index = SimilarityMatrix.vector2Index(row);
            LOG.info("Row : {}",rowPos);
            LOG.info("vector {} -index-> {}", Arrays.toString(row),index);
            assertEquals(rowPos, index);
            rowPos++;
        }

        rows = new boolean[][]{
                {false,false}, // 0
                {true,false},  // 1
                {false,true},  // 2
                {true,true},   // 3
        };
        assertEquals(rows.length , (1 << 2));
        rowPos = 0;
        for(boolean[] row : rows) {
            int index = SimilarityMatrix.vector2Index(row);
            LOG.info("Row : {}",rowPos);
            LOG.info("vector {} -index-> {}", Arrays.toString(row),index);
            assertEquals(rowPos, index);
            rowPos++;
        }

        rows = new boolean[][]{
                {false,false,false}, // 0
                {true,false,false},  // 1
                {false,true,false},  // 2
                {true,true,false},   // 3
                {false,false,true},  // 4
                {true,false,true},   // 5
                {false,true,true},   // 6
                {true,true,true}     // 7
        };
        assertEquals(rows.length , (1 << 3));
        rowPos = 0;
        for(boolean[] row : rows) {
            int index = SimilarityMatrix.vector2Index(row);
            LOG.info("Row : {}",rowPos);
            LOG.info("vector {} -index-> {}", Arrays.toString(row),index);
            assertEquals(rowPos, index);
            rowPos++;
        }

        rows = new boolean[][]{
                {false,false,false,false}, // 0
                {true,false,false,false},  // 1
                {false,true,false,false},  // 2
                {true,true,false,false},   // 3
                {false,false,true,false},  // 4
                {true,false,true,false},   // 5
                {false,true,true,false},   // 6
                {true,true,true,false},    // 7
                {false,false,false,true},  // 8
                {true,false,false,true},   // 9
                {false,true,false,true},   // 10
                {true,true,false,true},    // 11
                {false,false,true,true},   // 12
                {true,false,true,true},    // 13
                {false,true,true,true},    // 14
                {true,true,true,true}      // 15
        };
        assertEquals(rows.length , (1 << 4));
        rowPos = 0;
        for(boolean[] row : rows) {
            int index = SimilarityMatrix.vector2Index(row);
            LOG.info("Row : {}",rowPos);
            LOG.info("vector {} -index-> {}", Arrays.toString(row),index);
            assertEquals(rowPos, index);
            rowPos++;
        }
    }


    @Test
    public void test1() {
        long start = System.currentTimeMillis();
        NaiveSimilarityMatrix matrix = NaiveSimilarityMatrix.createMatrix(records);
        long end = System.currentTimeMillis();
        long time = end - start;
        LOG.info("{} took {} ms.",matrix,time);
    }

    @Test
    public void test2() {
        long start = System.currentTimeMillis();
        SimilarityMatrix matrix = SimilarityMatrixTest.createSimilarityMatrix(records);
        long end = System.currentTimeMillis();
        long time = end - start;
        LOG.info("{} took {} ms.",matrix,time);
    }

    @Test
    public void test3() {
        for (int fieldCount= 1; fieldCount < 6; fieldCount++) {
            for (int i = 0; i < (1 << fieldCount); i++) {
                LOG.info("{} , {}",i,SimilarityMatrix.index2Vector(i,fieldCount));
            }
            LOG.info("\n\n");
        }
    }

    @Test
    public void test4() {
        for(int f=1 ; f <= 5; f++ ) {
            for(int j=0; j < f ; j++) {
                int[] indexes = SimilarityMatrix.indexesWithJset(j, f);
                LOG.info("j=" + j +", f=" + f + " -indexes(" + indexes.length +")->" + Arrays.toString(indexes));
            }
            LOG.info("\n");
        }
    }

    // TODO Some tests with maybe some benchmarking ?

    public static SimilarityMatrix createSimilarityMatrix(final String[][] records,
                                                          final String similarityMethodName) {
        final int pairCount = CombinatoricsUtil.twoCombinationsCount(records.length);
        final int fieldCount = records[0].length;
        if(Long.compare(pairCount*fieldCount,Integer.MAX_VALUE) > 0)
            throw new UnsupportedOperationException("Cannot create gamma. #N*#F < Integer.MAX");
        final SimilarityMatrix matrix = new SimilarityMatrix(fieldCount);
        final Iterator<int[]> pairIter = CombinatoricsUtil.getPairs(records.length);
        do {
            int pair[] = pairIter.next();
            boolean[] row = new boolean[records[0].length];
            for(int j=0; j < records[0].length; j++) {
                String s1 = records[pair[0]][j];
                String s2 = records[pair[1]][j];
                if(SimilarityMatrix.similarity(similarityMethodName, s1, s2)) row[j] = true;
            }
            matrix.set(row);

        }while(pairIter.hasNext());
        return matrix;
    }

    public static SimilarityMatrix createSimilarityMatrix(final String[][] records) {
        return createSimilarityMatrix(records, SimilarityMatrix.DEFAULT_SIMILARITY_METHOD_NAME);
    }

    public static String[][] records = {
            {"conner","draiden","irving"}, {"connor","dradien","irving"}, {"connor","draiden","irving"},
            {"lucas","paterson","seattle"},
            {"amaya","mitsoulis","chicago"},
            {"amber","white","newark"},
            {"seamus","hirose","newark"},
            {"joshua","coulson","newark"},
            {"michael","reid","newark"},
            {"hayden","ryan","san diego"},
            {"shelby","wilkins","fremont"},
            {"bailey","snell","san antonio"},
            {"stephanie","walch","newark"},
            {"dylan","meale","newark"},
            {"jacob","dixon","newark"},
            {"hayley","bellchambers","newark"},
            {"niamh","clarke","colorado springs"},
            {"vanessa","gunillasson","newark"},
            {"charlotte","kassos","los angeles"},
            {"isabelle","gryen","laredo"},
            {"isabelle","green","laredo"},
            {"claire","clarke","newark"},
            {"helena","reid","newark"},
            {"keely","gimbrere","newark"},
            {"mystique","campbell","newark"},
            {"nacoya","mitton","newark"},
            {"daniel","bishop","newark"},
            {"sara","comlpton","oakland"}, {"sara","compton","oakland"},
            {"sophie","loqelock","newark"}, {"sophie","lovel0ck","newark"}, {"sophie","lovelock","newark"},
            {"jackson","pascoe","newark"},
            {"reuben","webb","newark"},
            {"benjamin","emllin","dallas"},
            {"lewis","armanini","newark"},
            {"mikhaili","wh:te","newark"}, {"mikhaili","hite","newark"}, {"mikhaili","white","newark"},
            {"jaslyn","lamprey","newark"},
            {"james","ryan","newark"},
            {"ella","schembri","hialeah"},
            {"kenneth","brgles","madison"},
            {"lachlan","bishop","newark"},
            {"benjamin","au","newark"},
            {"jasmyn","montuori","newark"},
            {"max","jolly","newark"},
            {"ryleh","campbell","newark"},
            {"kobe","jongebloed","newark"},
            {"caitlin","fang","newark"},
            {"koula","heerev","newark"}, {"koula","hierey","newark"}, {"koula","heerey","newark"},
            {"jasmine","neville","newark"},
            {"jye","mason","newark"},
            {"anneliese","boaz","newark"},
            {"montana","large","newark"},
            {"jaiden","oliveri","newark"},
            {"savannah","bishop","chicago"},
            {"kane","colquhoun","st. petersburg"},
            {"zac","tweedie","newark"},
            {"erin","crook","washington"},
            {"thomas","wraight","newark"},
            {"jayden","wohltmann","newark"},
            {"georgia","wyllie","long beach"},
            {"tyrone","petersen","newark"},
            {"toby","leslie","newark"},
            {"cameron","rudd","newark"},
            {"emma","berrymlan","newark"}, {"emma","qberryman","newark"}, {"emma","berryman","newark"},
            {"jackson","lihou","newark"},
            {"mitchell","estcourt","phoenix"},
            {"alexandra","rankine","newark"},
            {"amber","purdon","newark"},
            {"micheal","colquhoun","newark"}, {"mitchell","colquhoun","newark"},
            {"ruby","howie","oakland"},
            {"tiana","fletcher-jones","newark"},
            {"emiily","jolly","newark"},
            {"caitlin","clarke","newark"},
            {"benjamin","jolhly","newark"}, {"benjamin","jol1y","newark"}, {"benjamin","jolly","newark"},
            {"alisa","mcgregor","newark"},
            {"amber","rudd","newark"},
            {"joshua","szkolik","newark"},
            {"michaela","mason","newark"},
            {"oakleigh","ottens","newark"},
            {"katelyn","saffoury","newark"},
            {"lewis","absenger","newark"},
            {"mitchell","spillings","newark"},
            {"emiily","purtell","dallas"},
            {"lachlan","george","newark"},
            {"chelsea","burford","newark"},
            {"trevor","stubbs","newark"},
            {"tristan","mesit'l","newark"}, {"tristan","mesiti","newark"}, {"tristan","mesiti","newark"},
            {"noah","lanyon","chicago"},
            {"sonja","lodge","newark"},
            {"ruby","degasperi","newark"},
            {"kyle","mcfadden","newark"},
            {"kyah","swoboda","newark"},
            {"tahnee","steed","boston"},
            {"zoe","callahan","newark"},
            {"nicole","permyakoff","newark"},
            {"samuel","hackenberg","newark"},
            {"thomas","mcclay","newark"},
            {"hugo","chandler","newark"},
            {"sophie","web1>","newark"}, {"sophie","webb","newark"},
            {"bronte","beckwith","baton rouge"},
            {"lucy","noack","newark"},
            {"levi","webb","newark"},
            {"madison","monteleone","newark"},
            {"channing","ziln","detroit"}, {"channing","izlm","detroit"}, {"channing","zilm","detroit"},
            {"grace","lock","los angeles"}
    };
    public static String[] fieldNames = {"name","surname","location"};
}
