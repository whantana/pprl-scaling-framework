package gr.upatras.ceid.pprl.test;

import gr.upatras.ceid.pprl.combinatorics.CombinatoricsUtil;
import org.apache.commons.math3.util.Combinations;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class CombinatoricsTest {

    private static Logger LOG = LoggerFactory.getLogger(CombinatoricsTest.class);

    @Test
    public void test0() {
        assertEquals(CombinatoricsUtil.rankTwoCombination(new int[]{0, 1}), 0);
        assertEquals(CombinatoricsUtil.rankCombination(new int[]{0, 1}, 2), 0);

        Combinations CC2 = new Combinations(10,2);
        Combinations CC3 = new Combinations(10,3);

        int i = 0;
        for(int[] cc : CC2) {
            LOG.info("cc(2) : {} -rank-> {}", Arrays.toString(cc), i);
            assertEquals(CombinatoricsUtil.rankTwoCombination(cc), i++);
        }

        int j = 0;
        for(int[] cc : CC3) {
            assertEquals(CombinatoricsUtil.rankCombination(cc, 3), j++);
        }
    }

    @Test
    public void test1() {
        int N = 10;
        int[] elements = {9,5,4,2,0};
        long[][] ranks = new long[elements.length][N-1];
        for(int i=0 ; i < elements.length ; i++) {
            LOG.info("Element = " + elements[i]);
            for (int j = 0; j < N; j++) {
                if(j == elements[i]) continue;
                int[] cc = (j < elements[i]) ? new int[]{j, elements[i]} : new int[]{elements[i], j};
                int k = (j < elements[i]) ? j : (j - 1);
                ranks[i][k] = CombinatoricsUtil.rankTwoCombination(cc);
                LOG.info("Pair = {} with rank {}",Arrays.toString(cc),ranks[i][k]);
            }


            long[][] testingRanks = CombinatoricsUtil.ranksArraysContaining(elements[i], 10);
            LOG.info("testingRanks = " + Arrays.deepToString(testingRanks));

            int j=0;
            int k=0;
            if(testingRanks[0].length > 1) {
                for (long l = testingRanks[0][0] ; l <= testingRanks[0][1] ; l++) assertEquals(l,ranks[i][j++]);
                k = 1;
            }

            assertEquals(j,elements[i]);
            j++;

            for (;k < testingRanks.length ; k++) {
                assertEquals(testingRanks[k][0],ranks[i][(j-1)]);
                j++;
            }
            assertEquals(j,N);

            long[] alsoTestingRanks = CombinatoricsUtil.ranksContaining(elements[i],10);
            LOG.info("alsoTestingRanks = " + Arrays.toString(alsoTestingRanks));
            assertEquals(alsoTestingRanks.length, ranks[i].length);
            for (int l = 0; l < ranks[i].length ; l++) {
                assertEquals(alsoTestingRanks[l],ranks[i][l]);
            }
        }
    }
    @Test
    public void test2() {
        int N = 10;
        int[] elements = {9,5,4,2,0};
        long[][] ranks = new long[elements.length][N-1];
        for(int i=0 ; i < elements.length ; i++) {
            for (int j = 0; j < N; j++) {
                if(j == elements[i]) continue;
                int[] cc = (j < elements[i]) ? new int[]{j, elements[i]} : new int[]{elements[i], j};
                ranks[i][((j < elements[i]) ? j : (j - 1))] = CombinatoricsUtil.rankTwoCombination(cc);
            }
            Iterator<Long> testIterator = CombinatoricsUtil.oldranksOfElementIterator(elements[i], 10);
            LOG.info("Old Iterator. e = {} , n = {}",elements[i],10);
            int j = 0;
            while(testIterator.hasNext()) {
                long testRank = testIterator.next();
                LOG.info(String.format("testRank = %d vs ranks[%d][%d]=%d",testRank,i,j,ranks[i][j]));
                assertEquals(testRank,ranks[i][j]);
                j++;
            }
        }
    }


    @Test
    public void test3() {
        int N = 10;
        int[] elements = {9,5,4,2,0};
        long[][] ranks = new long[elements.length][N-1];
        for(int i=0 ; i < elements.length ; i++) {
            for (int j = 0; j < N; j++) {
                if(j == elements[i]) continue;
                int[] cc = (j < elements[i]) ? new int[]{j, elements[i]} : new int[]{elements[i], j};
                ranks[i][((j < elements[i]) ? j : (j - 1))] = CombinatoricsUtil.rankTwoCombination(cc);
            }
            Iterator<Long> testIterator = CombinatoricsUtil.ranksOfElementIterator(elements[i], 10);
            LOG.info("Iterator. e = {} , n = {}",elements[i],10);
            int j = 0;
            while(testIterator.hasNext()) {
                long testRank = testIterator.next();
                LOG.info(String.format("testRank = %d vs ranks[%d][%d]=%d",testRank,i,j,ranks[i][j]));
                assertEquals(testRank,ranks[i][j]);
                j++;
            }
        }
    }
}
