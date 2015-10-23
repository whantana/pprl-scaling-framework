package gr.upatras.ceid.pprl.blocking.test;

import gr.upatras.ceid.pprl.blocking.CombinatoricsRanking;
import gr.upatras.ceid.pprl.blocking.TwoPassMBT;
import org.junit.Ignore;
import org.junit.Test;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class TwoPassMBTTest {

    @Test
    public void testPrepare() {
        int N = 100;
        int L = 1024;
        TwoPassMBT algorithm = new TwoPassMBT(N,L,10,20); // 100 bitsets with length 1024 bits and MIN=10,MAX=20
        algorithm.prepare(BitSets.randomBitSets(N,L));
        assertTrue(algorithm.getBUCKETS().isEmpty());
    }

    @Test
    public void testTOTAL_BIT_COUNTER() {
        TwoPassMBT algorithm = new TwoPassMBT(10,10); // 10 bitsets with length 10
        BitSet bs1 = new BitSet();
        bs1.set(5);
        BitSet bs2 = new BitSet();
        bs2.set(5);
        bs2.set(6);
        algorithm.updateCounters(bs1);
        assertEquals(algorithm.getTOTAL_BIT_COUNTER()[5],1);
        algorithm.updateCounters(bs2);
        assertEquals(algorithm.getTOTAL_BIT_COUNTER()[5],2);
        assertEquals(algorithm.getTOTAL_BIT_COUNTER()[6],1);

    }

    @Test
    public void testBIT_2_COMBINATIONS_COUNTERS() {
        TwoPassMBT algorithm = new TwoPassMBT(10,10); // 10 bitsets with length 10
        BitSet bs1 = new BitSet();
        bs1.set(5);
        BitSet bs2 = new BitSet();
        bs2.set(5);
        bs2.set(6);
        algorithm.updateCounters(bs1);
        algorithm.updateCounters(bs2);
        int [][] CCS = algorithm.getBIT_2_COMBINATIONS_COUNTERS();
        assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{5,6})][0],0);
        assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{5,6})][1],1);
        assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{5,6})][2],0);
        assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{5,6})][3],1);
        for (int i = 0; i < 5; i++) {
            assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{i,5})][0],0);
            assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{i,5})][1],0);
            assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{i,5})][2],2);
            assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{i,5})][3],0);
        }

        for (int i = 7 ; i < 10; i++) {
            assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{5,i})][0],0);
            assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{5,i})][1],2);
            assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{5,i})][2],0);
            assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{5,i})][3],0);
        }

        assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{0,1})][0],2);
        assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{0,1})][1],0);
        assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{0,1})][2],0);
        assertEquals(CCS[CombinatoricsRanking.rank2Combination(new int[]{0,1})][3],0);
    }

//    @Test
//    public void testDebug() {
//        int N = 100;
//        int L = 10;
//        TwoPassMBT algorithm = new TwoPassMBT(N,L,1,2); // 100 bitsets with length 10 bits and MIN=10,MAX=20
//        algorithm.prepare(BitSets.randomBitSets(N,L));
//        try {
//            algorithm.dumpCounters();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }
//    }
}
