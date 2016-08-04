package gr.upatras.ceid.pprl.mapreduce;

import org.apache.hadoop.io.WritableComparator;

import java.util.Arrays;

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

        // first compare blocking key (serialized int)
        int bg1 = readInt(b1,s1);
        int bg2 = readInt(b2,s2);
        int c0 = (bg1 < bg2) ? -1 : ((bg1 == bg2) ? 0 : 1);
        if(c0 != 0) return c0;

        // then compare hashes (serialized bitset)
        int len1 = readInt(b1,s1 + 4);
        int len2 = readInt(b2,s2 + 4);
        byte[] h1 = new byte[len1];
        System.arraycopy(b1,s1 + 8,h1,0,len1);
        byte[] h2 = new byte[len2];
        System.arraycopy(b2,s2 + 8,h2,0,len2);
        int c1 = (-1)*compareHashes(h1,h2);
        if(c1 != 0) return c1;

        // then compare dataset source (serialized char)
        byte b10 = b1[s1 + 8 + len1];
        byte b11 = b1[s1 + 8 + len1 + 1];
        char d1 = (char) (((b10 << 8) & 0xFF) +
                ((b11) & 0xFF));
        byte b20 = b2[s2 + 8 + len2];
        byte b21 = b2[s2 + 8 + len2 + 1];
        char d2 = (char) (((b20 << 8) & 0xFF) +
                ((b21) & 0xFF));
        return (d1 < d2) ? -1 : ((d1 == d2) ? 0 : 1);
    }

    /**
     * Compare hashes (byte arrays).
     *
     * @param h1 byte array 1.
     * @param h2 byte array 2.
     * @return -1 if b1 is greater than b2, 0 if b1 equals b2, 1 if b2 larger than b1
     */
    private static int compareHashes(final byte[] h1, final byte[] h2) {
        byte[] b1;
        byte[] b2;
        if (h1.length !=  h2.length) {
            if(h1.length == 0) return 1;
            if(h2.length == 0) return -1;
            byte[][] bs = resizeSmallerHash(h1,h2);
            b1 = bs[0];
            b2 = bs[1];
        } else {
            if(h1.length == 0) return 0;
            b1 = h1;
            b2 = h2;
        }

        for (int i = b1.length-1; i >= 0 ; i--) {
            byte xor= (byte)(0xff & (b1[i] ^ b2[i]));
            if(xor == 0) continue;
            int j = 128;
            for (; j >= 0 ; j = j >> 1) {
                if((xor & j) != 0) break;
            }
            if((b1[i] & j) !=0 && (b2[i] & j) == 0) return -1;
            if((b1[i] & j) ==0 && (b2[i] & j) != 0) return 1;
        }
        return 0;
    }

    /**
     * Resize smaller hash have same length in the byte arrays.
     *
     * @param h1 byte array 1.
     * @param h2 byte array 2.
     * @return a resized version of the hashes
     */
    private static byte[][] resizeSmallerHash(final byte[] h1, final byte[] h2) {
        boolean h1Smaller = h1.length < h2.length;
        byte[] resized = new byte[h1Smaller ? h2.length : h1.length];
        Arrays.fill(resized,(byte)0);
        System.arraycopy(h1Smaller ? h1 : h2, 0,resized,0,
                h1Smaller ? h1.length : h2.length);
        return new byte[][]{h1Smaller ? resized : h1,h1Smaller ? h2 : resized};
    }


    static {
        // Register comparator
        WritableComparator.define(BlockingKeyWritable.class, new BlockingKeyWritableComparator());
    }
}
