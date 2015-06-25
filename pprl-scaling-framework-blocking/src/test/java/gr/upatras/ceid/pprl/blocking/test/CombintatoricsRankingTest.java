package gr.upatras.ceid.pprl.blocking.test;

import gr.upatras.ceid.pprl.blocking.CombinatoricsRanking;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.math3.util.Combinations;

import java.util.Iterator;

public class CombintatoricsRankingTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CombintatoricsRankingTest(String testName)
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( CombintatoricsRankingTest.class );
    }

    public void testRank() {
        assertEquals(CombinatoricsRanking.rank2Combination(new int[]{0,1}),0);
        assertEquals(CombinatoricsRanking.rankCombination(new int[]{0, 1}, 2),0);
        Combinations CC2 = new Combinations(10,2);
        Combinations CC3 = new Combinations(10,3);
        final int limit = 10;
        int i = 0;
        for(int[] cc : CC2) {
            assertEquals(CombinatoricsRanking.rank2Combination(cc),i++);
            if(i >= limit ) break;
        }
        i = 0;
        for(int[] cc : CC3) {
            assertEquals(CombinatoricsRanking.rankCombination(cc,3),i++);
            break;
        }
    }

    public void testElementRank() {
        int N = 10;
        int[] elements = {9,5,4,2,0};
        int[][] ranks = new int[elements.length][N-1];
        for(int i=0 ; i < elements.length ; i++) {
            for (int j = 0; j < N; j++) {
                if(j == elements[i]) continue;
                int[] cc = (j < elements[i]) ? new int[]{j, elements[i]} : new int[]{elements[i], j};
                ranks[i][((j < elements[i]) ? j : (j - 1))] = CombinatoricsRanking.rank2Combination(cc);
            }


            int[][] testingRanks = CombinatoricsRanking.ranksArraysContaining(elements[i], 10);
            int j=0;
            int k=0;
            if(testingRanks[0].length > 1) {
                for (int l = testingRanks[0][0] ; l <= testingRanks[0][1] ; l++) assertEquals(l,ranks[i][j++]);
                k = 1;
            }

            assertEquals(j,elements[i]);
            j++;

            for (;k < testingRanks.length ; k++) {
                assertEquals(testingRanks[k][0],ranks[i][(j-1)]);
                j++;
            }
            assertEquals(j,N);

            int[] alsoTestingRanks = CombinatoricsRanking.ranksContaining(elements[i],10);
            assertEquals(alsoTestingRanks.length ,ranks[i].length);
            for (int l = 0; l < ranks[i].length ; l++) {
                assertEquals(alsoTestingRanks[l],ranks[i][l]);
            }
        }
    }

    public void testElementRankIterator() {
        int N = 10;
        int[] elements = {9,5,4,2,0};
        int[][] ranks = new int[elements.length][N-1];
        for(int i=0 ; i < elements.length ; i++) {
            for (int j = 0; j < N; j++) {
                if(j == elements[i]) continue;
                int[] cc = (j < elements[i]) ? new int[]{j, elements[i]} : new int[]{elements[i], j};
                ranks[i][((j < elements[i]) ? j : (j - 1))] = CombinatoricsRanking.rank2Combination(cc);
            }
            Iterator<Integer> testIterator = CombinatoricsRanking.ranksOfElementIterator(elements[i],10);
            int j = 0;
            while(testIterator.hasNext()) {
                int testRank = testIterator.next();
                assertEquals(testRank,ranks[i][j++]);
            }
        }
    }
}
