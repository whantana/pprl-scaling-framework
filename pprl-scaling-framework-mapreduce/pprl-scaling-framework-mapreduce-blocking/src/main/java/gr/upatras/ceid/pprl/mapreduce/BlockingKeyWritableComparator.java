package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.WritableComparator;

/**
 * A blocking key comparator for sorting first by group id then by key
 * and then by dataset id.
 */
public class BlockingKeyWritableComparator extends WritableComparator {

    public BlockingKeyWritableComparator() {
        super(BlockingKeyWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

        int bg1 = readInt(b1,s1);
        int bg2 = readInt(b2,s2);
        int c0 = (bg1 < bg2) ? -1 : ((bg1 == bg2) ? 0 : 1);
        if(c0 != 0) return c0;
        int len1 = readInt(b1,s1 + 4);
        int len2 = readInt(b2,s2 + 4);
        byte[] h1 = new byte[len1];
        System.arraycopy(b1,s1 + 8,h1,0,len1);
        byte[] h2 = new byte[len2];
        System.arraycopy(b2,s2 + 8,h2,0,len2);
        int c1 = compareHashes(h1,h2);
        if(c1 != 0) return c1;
        char d1 = (char) readInt(b1, l1 - 4);
        char d2 = (char) readInt(b2,l2-4);
        return (d1 < d2) ? -1 : ((d1 == d2) ? 0 : 1);
    }

    /**
     * Compare hashes (byte arrays).
     * @param b1 byte array 1.
     * @param b2 byte array 2.
     * @return -1 if b1 is greater than b2, 0 if b1 equals b2, 1 if b2 larger than b1
     */
    private static int compareHashes(final byte[] b1, final byte[] b2) {
        assert b1.length ==  b2.length;
        for (int i = b1.length-1; i >= 0 ; i--) {
            byte xor= (byte)(0xff & (b1[i] ^ b2[i]));
            if(xor == 0) continue;
            int j = 128;
            for (; j >= 0 ; j = j >> 1) {
                if((xor & j) != 0) break;
            }
            if((b1[i] & j) !=0 && (b2[i] & j) == 0) return -1;
            if((b1[i] & j) ==0 && (b2[i] & j) != 0) return 0;
        }
        return 0;
    }
}
