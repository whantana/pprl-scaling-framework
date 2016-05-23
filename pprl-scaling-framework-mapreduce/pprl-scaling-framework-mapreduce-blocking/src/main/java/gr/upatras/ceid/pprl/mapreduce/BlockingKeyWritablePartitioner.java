package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A partitioner for this key class.
 */
public class BlockingKeyWritablePartitioner extends Partitioner<BlockingKeyWritable,Text> implements Configurable {

    private Configuration conf;
    private RangesInRanges RR;
    private int L;

    public void setConf(Configuration configuration) {
        conf = configuration;
        RR = null;
        L = configuration.getInt(CommonKeys.BLOCKING_GROUP_COUNT,-1);
        if(L <= 0) throw new IllegalStateException("Must set the blocking group count.");
    }

    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(BlockingKeyWritable key, Text value, int numPartitions) {
        assert key.blockingGroupId >= 0 && key.blockingGroupId < L;
        if(L == numPartitions) return key.blockingGroupId;
        if(RR == null) RR = new RangesInRanges(0,L-1,numPartitions);
        return RR.belongsToRange(key.blockingGroupId);
    }

    /**
     * Helper class to partition up the blocking group space.
     */
    private static class RangesInRanges {
        private int max;
        private int min;
        private int rc;
        boolean equalsubs;
        private int point;
        private int range;
        private int[][] ranges;

        public RangesInRanges(int min, int max, int rc) {
            this.max = max;
            this.min = min;
            this.rc = rc;
            range = max - min + 1;
            equalsubs = (range % rc == 0);
            point = (max - min + 1) % rc;
            int length = range / rc;
            ranges = new int[rc][2];
            ranges[0] = new int[]{min,(min+length) + (equalsubs ? 0 : 1)};
            for(int i = 1 ; i < rc ; i++) {
                int start = ranges[i-1][1];
                int end = start + ((i >= point) ? length : (length + 1));
                ranges[i] = new int[]{start,end};
            }
        }

        public int[] getRangeLimits(final int i) {
            assert i >= 0 && i < rc;
            return ranges[i];
        }

        public int belongsToRange(final int n) {
            assert n >= min && n <= max;
            for (int i = 0 ; i < rc ; i++) {
                int[] limits = ranges[i];
                if(limits[0] <= n && n < limits[1]) return i;
            }
            return -1;
        }
    }
}
