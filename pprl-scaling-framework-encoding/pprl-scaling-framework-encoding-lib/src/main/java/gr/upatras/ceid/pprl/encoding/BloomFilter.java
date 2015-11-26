package gr.upatras.ceid.pprl.encoding;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

public class BloomFilter {

    private static final String SECRET_KEY = "ZIKRETQI";
    private static final Mac hmacmd5;
    private static final Mac hmacsha1;
    static {
        Mac tmp;
        Mac tmp1;
        try {
            tmp = Mac.getInstance("HmacMD5");
            tmp.init(new SecretKeySpec(SECRET_KEY.getBytes(), "HmacMD5"));
            tmp1 = Mac.getInstance("HmacSHA1");
            tmp1.init(new SecretKeySpec(SECRET_KEY.getBytes(), "HmacSHA1"));
        } catch (NoSuchAlgorithmException e) {
            tmp = null;
            tmp1 = null;
        } catch (InvalidKeyException e) {
            tmp = null;
            tmp1 = null;
        }
        hmacmd5 = tmp;
        hmacsha1 = tmp1;
    }

    private BitSet bitset;
    private int N;
    private int K;
    private int addedElementsCount;
    private int onesCount;
    private int zeroesCount;

    public BloomFilter(final int N, final int K) {
        this.N = N;
        this.K = K;
        addedElementsCount = 0;
        onesCount = 0;
        zeroesCount = N;
        bitset = new BitSet(N);
    }


    public double calcFPP(){
        return Math.pow((1 - Math.exp(-K * (double) addedElementsCount / (double) N)), K);
    }


    public static int[] createHashesV1(final byte[] data,int N,int K) {
        byte[] sha1Digest = hmacsha1.doFinal(data);
        byte[] md5Digest = hmacmd5.doFinal(data);
        final BigInteger SHA1 = new BigInteger(sha1Digest);
        final BigInteger MD5 = new BigInteger(md5Digest);
        final int[] hashes = new int[K];
        for (int i = 0; i < K; i++) {
            final BigInteger I = new BigInteger(String.valueOf(i));
            final BigInteger RES =
                    MD5.multiply(I).add(SHA1).mod(new BigInteger(String.valueOf(N)));
            hashes[i] = Math.abs(RES.intValue());

        }
        return hashes;
    }

    public static int[] createHashesV2(final byte[] data,int N,int K) {
        int k = 0;
        byte salt = 0;
        final int[] hashes = new int[K];

        while(k < K) {
            byte[] md5Digest;
            synchronized (hmacmd5) {
                hmacmd5.update(salt);
                salt++;
                md5Digest = hmacmd5.doFinal(data);
            }
            for (int i = 0; i < md5Digest.length/4 && k < K; i++,k++) {
                int h = 0;
                for (int j = (i*4); j < (i*4)+4; j++) {
                    h <<= 8;
                    h |= ((int) md5Digest[j]) & 0xFF;
                }
                hashes[k] = Math.abs(h % N);
            }
        }
        return hashes;
    }

    public int[] addData(final byte[] data) {
        final int[] positions = createHashesV1(data,N,K);

        for(int position : positions) {
            if(!bitset.get(position)) { 
                onesCount++; zeroesCount--;
                setBit(position);
            }
        }
        addedElementsCount++;
        return positions;
    }

    public int getN() {
        return N;
    }


    public int getK() {
        return K;
    }


    public int getAddedElementsCount() {
        return addedElementsCount;
    }


    public int getOnesCount() {
        return onesCount;
    }

    public int getZeroesCount() {
        return zeroesCount;
    }

    public BitSet getBitset() {
        return bitset;
    }

    public byte[] getBytes() {
        return bitset.toByteArray();
    }

    public double calcBitsPerElement() {
        return this.N / (double)addedElementsCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BloomFilter that = (BloomFilter) o;

        if (N != that.N) return false;
        if (K != that.K) return false;
        if (addedElementsCount != that.addedElementsCount) return false;
        if (onesCount != that.onesCount) return false;
        if (zeroesCount != that.zeroesCount) return false;
        return bitset.equals(that.bitset);

    }

    @Override
    public int hashCode() {
        int result = bitset.hashCode();
        result = 31 * result + N;
        result = 31 * result + K;
        result = 31 * result + addedElementsCount;
        result = 31 * result + onesCount;
        result = 31 * result + zeroesCount;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MSB -> ");
        byte[] bytes = getBytes();
        for (int i = bytes.length-1; i >= 0 ; i--) {
            sb.append(String.format("%8s",
                    Integer.toBinaryString(bytes[i] & 0xFF)).replace(' ', '0'));
        }
        sb.append(" <- LSB");
        return sb.toString();
    }

    public String toHexString() {
        return toHexString(getBytes());
    }

    public static String toHexString(final byte[] bytes) {
        StringBuilder sb = new StringBuilder("0x");
        for (int i = bytes.length-1; i >= 0 ; i--)
            sb.append(String.format("%02x", bytes[i] & 0xFF));
        return sb.toString();
    }

    public int[] toInt() {
        int[] s = new int[N];
        for (int i = 0; i < N; i++)
            s[i] = bitset.get(i) ? 1 : 0;
        return s;
    }

    public Set<Integer> toSetOnes() {
        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < N; i++) {
            if (bitset.get(i)) set.add(i);
        }
        return set;
    }

    public Set<Integer> toSetZeroes() {
        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < N; i++) {
            if (!bitset.get(i)) set.add(i);
        }
        return set;
    }

    public boolean getBit(int bit) {
        return bitset.get(bit);
    }

    public void setBit(int bit, boolean value) {
        bitset.set(bit, value);
    }

    public void setBit(int bit) {
        setBit(bit, true);
    }

    public void clear() {
        for (int i = 0; i < N; i++) setBit(i,false);
        onesCount = 0;
        zeroesCount = N;
        addedElementsCount = 0;
    }
}
