package gr.upatras.ceid.pprl.blocking.test;

import gr.upatras.ceid.pprl.blocking.Bucket;
import gr.upatras.ceid.pprl.blocking.MBT;
import org.junit.Ignore;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class MBTTest {

    public void testChoosingBit() {

    }

    @Test
    public void testPrepare() {
        int N=100;
        MBT algorithm = new MBT(N,1024,10,20); // 100 records of 1024 bits with MIN_BUCKET_SIZE=10 , MAX_BUCKET_SIZE=20
        try{
            algorithm.run();
        } catch (AssertionError er) {
            algorithm.prepare(new BitSet[N]);
            assertTrue(algorithm.getBUCKETS().isEmpty());
        }
    }
    @Test
    public void testRun() {
        int N = 100;
        int L = 1024;
        MBT algorithm = new MBT(N,L,10,20); // 100 records of 1024 bits with MIN_BUCKET_SIZE=10 , MAX_BUCKET_SIZE=20
        algorithm.prepare(BitSets.randomBitSets(N,L));
        algorithm.run();
        List<Bucket> buckets = algorithm.getBUCKETS();
        for(Bucket b : buckets) testBucket(b);
    }

    @Test
    private void testBucket(final Bucket b) {
        assertEquals(b.getSplitBits().size(),b.getSplitBitValues().size());
        BitSet[] bitSets = new BitSet[b.getCount()];
        bitSets = b.getBitSets().toArray(bitSets);
        for (int i = 0; i < b.getSplitBits().size() ; i++) {
            int sb = b.getSplitBits().get(i);
            boolean sbv = b.getSplitBitValues().get(i);
            for (BitSet bs : bitSets) assertEquals(bs.get(sb),sbv);
        }
    }
}
