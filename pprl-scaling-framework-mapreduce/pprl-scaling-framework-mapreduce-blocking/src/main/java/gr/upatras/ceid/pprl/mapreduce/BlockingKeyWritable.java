package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

/**
 * A Hamming blocking key writable.
 */
public class BlockingKeyWritable implements WritableComparable<BlockingKeyWritable> {

    public int blockingGroupId;
    public BitSet hash;
    public char datasetId;

    /**
     * Constructor.
     */
    public BlockingKeyWritable() {}

    /**
     * Constructor.
     * @param blockingGroupId the group id.
     * @param hash the hash value.
     * @param dataset source dataset
     */
    public BlockingKeyWritable(int blockingGroupId, BitSet hash, char dataset) {
        this.blockingGroupId = blockingGroupId;
        this.hash = hash;
        this.datasetId = dataset;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(blockingGroupId);
        byte[] bytes = hash.toByteArray();
        out.writeInt(bytes.length);
        for (byte aByte : bytes) {
            out.write(aByte);
        }
        out.writeChar(datasetId);
    }

    public void readFields(DataInput in) throws IOException {
        blockingGroupId = in.readInt();
        byte[] bytes= new byte[in.readInt()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = in.readByte();
        }
        hash = BitSet.valueOf(bytes);
        datasetId = in.readChar();
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof BlockingKeyWritable) {
            BlockingKeyWritable that = (BlockingKeyWritable) o;

            if (blockingGroupId != that.blockingGroupId) return false;
            if (datasetId != that.datasetId) return false;

            return !(hash != null ? !hash.equals(that.hash) : that.hash != null);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = blockingGroupId;
        result = 31 * result + (hash != null ? hash.hashCode() : 0);
        result = 31 * result + (int) datasetId;
        return result;
    }

    /**
     * Comparison between two blocking keys.
     *
     * @param o the other blocking key.
     * @return 1
     */
    public int compareTo(BlockingKeyWritable o) {
        if(this.equals(o)) return 0;
        int c0 = (blockingGroupId > o.blockingGroupId) ? 1 :
                (blockingGroupId == o.blockingGroupId) ? 0 : -1;
        if(c0!=0) return c0;
        int c1 = compareHashes(hash,o.hash);
        if(c1!=0) return c1;
        return (datasetId > o.datasetId) ? 1 :
                (datasetId== o.datasetId) ? 0 : -1;
    }

    /**
     * Compare two bitsets (lexicographically)
     *
     * @param b1 BitSet 1.
     * @param b2 BitSet 2.
     * @return -1 if 1 is greater than 2, 0 if equal , 1 if 2 is greater than 1.
     */
    private static int compareHashes(final BitSet b1, final BitSet b2) {
        if (b1.equals(b2)) return 0;
        BitSet xor = (BitSet)b1.clone();
        xor.xor(b2);
        int firstDifferent = xor.length()-1;
        return b2.get(firstDifferent) ? -1 : 1;
    }

    @Override
    public String toString() {
        return toString(10);
    }

    /**
     * String representation of blocking key.
     *
     * @param K starting rightmost bit of hash value.
     * @return string representation of this blocking key.
     */
    public String toString(int K) {
        StringBuilder sb = new StringBuilder();
        for (int i = (K-1); i >= 0; i--)
            sb.append(hash.get(i)?"1":"0");
        return String.format(
                "%5d_%s_%c",blockingGroupId,sb.toString(), datasetId);
    }

    /**
     * A bitset to string.
     *
     * @param b bitset
     * @param K starting rightmost bit
     * @return a string representation of bitset
     */
    public static String bitsetToKString(final BitSet b,int K) {
        StringBuilder sb = new StringBuilder();
        for (int i = (K-1); i >= 0; i--)
            sb.append(b.get(i)?"1":"0");
        return sb.toString();
    }

    public static boolean sameBlockingKey(final BlockingKeyWritable akey,
                                          final BlockingKeyWritable bkey) {
        assert akey.datasetId == 'A' && bkey.datasetId == 'B';
        return akey.blockingGroupId == bkey.blockingGroupId && akey.hash.equals(bkey.hash);
    }
}
