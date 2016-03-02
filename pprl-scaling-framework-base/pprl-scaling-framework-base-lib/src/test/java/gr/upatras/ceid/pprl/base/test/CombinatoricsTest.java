package gr.upatras.ceid.pprl.base.test;

import gr.upatras.ceid.pprl.base.CombinatoricsUtil;
import org.apache.commons.math3.util.Combinations;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombinatoricsTest {

    private static Logger LOG = LoggerFactory.getLogger(CombinatoricsTest.class);

    @Test
    public void testRank() {
        Assert.assertEquals(CombinatoricsUtil.rankTwoCombination(new int[]{0, 1}), 0);
        Assert.assertEquals(CombinatoricsUtil.rankCombination(new int[]{0, 1}, 2), 0);
        Combinations CC2 = new Combinations(10,2);
        Combinations CC3 = new Combinations(10,3);
        final int limit = 10;
        int i = 0;
        for(int[] cc : CC2) {
            Assert.assertEquals(CombinatoricsUtil.rankTwoCombination(cc), i++);
            if(i >= limit ) break;
        }
        i = 0;
        for(int[] cc : CC3) {
            Assert.assertEquals(CombinatoricsUtil.rankCombination(cc, 3), i++);
            break;
        }
    }

//    @Test
//    public void testElementRank() {
//        int N = 10;
//        int[] elements = {9,5,4,2,0};
//        int[][] ranks = new int[elements.length][N-1];
//        for(int i=0 ; i < elements.length ; i++) {
//            for (int j = 0; j < N; j++) {
//                if(j == elements[i]) continue;
//                int[] cc = (j < elements[i]) ? new int[]{j, elements[i]} : new int[]{elements[i], j};
//                ranks[i][((j < elements[i]) ? j : (j - 1))] = CombinatoricsUtil.rankTwoCombination(cc);
//            }
//
//
//            int[][] testingRanks = CombinatoricsUtil.ranksArraysContaining(elements[i], 10);
//            int j=0;
//            int k=0;
//            if(testingRanks[0].length > 1) {
//                for (int l = testingRanks[0][0] ; l <= testingRanks[0][1] ; l++) assertEquals(l,ranks[i][j++]);
//                k = 1;
//            }
//
//            assertEquals(j,elements[i]);
//            j++;
//
//            for (;k < testingRanks.length ; k++) {
//                assertEquals(testingRanks[k][0],ranks[i][(j-1)]);
//                j++;
//            }
//            assertEquals(j,N);
//
//            int[] alsoTestingRanks = CombinatoricsUtil.ranksContaining(elements[i],10);
//            assertEquals(alsoTestingRanks.length ,ranks[i].length);
//            for (int l = 0; l < ranks[i].length ; l++) {
//                assertEquals(alsoTestingRanks[l],ranks[i][l]);
//            }
//        }
//    }
//    @Test
//    public void testElementRankIterator() {
//        int N = 10;
//        int[] elements = {9,5,4,2,0};
//        int[][] ranks = new int[elements.length][N-1];
//        for(int i=0 ; i < elements.length ; i++) {
//            for (int j = 0; j < N; j++) {
//                if(j == elements[i]) continue;
//                int[] cc = (j < elements[i]) ? new int[]{j, elements[i]} : new int[]{elements[i], j};
//                ranks[i][((j < elements[i]) ? j : (j - 1))] = CombinatoricsUtil.rankTwoCombination(cc);
//            }
//            Iterator<Integer> testIterator = CombinatoricsUtil.ranksOfElementIterator(elements[i],10);
//            int j = 0;
//            while(testIterator.hasNext()) {
//                int testRank = testIterator.next();
//                assertEquals(testRank,ranks[i][j++]);
//            }
//        }
//    }
}
