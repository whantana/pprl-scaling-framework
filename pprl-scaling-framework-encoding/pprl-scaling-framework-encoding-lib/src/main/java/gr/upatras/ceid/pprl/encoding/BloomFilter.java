package gr.upatras.ceid.pprl.encoding;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
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

    private int N;
    private int K;
    private int addedElementsCount;
    private int onesCount;
    private int zeroesCount;
    private byte[] byteArray;

    public BloomFilter(final int N, final int K) {
        this.N = N;
        this.K = K;
        byteArray = new byte[(int) Math.ceil(N/(double)8)];
        addedElementsCount = 0;
        onesCount = 0;
        zeroesCount = N;
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
            final BigInteger I = new BigInteger(String.valueOf(i+1)); // TODO ask about i and i+1
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
            if(!getBit(position,byteArray)) {
                onesCount++; zeroesCount--;
                setBit(position,byteArray);
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

    public byte[] getByteArray() {
        return byteArray;
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
        return Arrays.equals(byteArray, that.byteArray);

    }

    @Override
    public int hashCode() {
        int result = byteArray.hashCode();
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
        for (int i = byteArray.length-1; i >= 0 ; i--)
            sb.append(Integer.toBinaryString(byteArray[i] & 255 | 256).substring(1));
        sb.append(" <- LSB");
        return sb.toString();
    }

    public String toHexString() {
        return toHexString(byteArray);
    }

    public static String toHexString(final byte[] bytes) {
        StringBuilder sb = new StringBuilder("0x");
        for (int i = bytes.length-1; i >= 0 ; i--)
            sb.append(String.format("%02x", bytes[i] & 0xFF));
        return sb.toString();
    }

    public Set<Integer> toSetOnes() {
        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < N; i++) {
            if (getBit(i, byteArray)) set.add(i);
        }
        return set;
    }

    public Set<Integer> toSetZeroes() {
        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < N; i++) {
            if (!getBit(i, byteArray)) set.add(i);
        }
        return set;
    }

    public int countOnes() {
        int ones = 0;
        for (int i = 0; i < N; i++) {
            if (getBit(i, byteArray)) ones++;
        }
        return ones;
    }

    public int countZeroes() {
        int zeroes = 0;
        for (int i = 0; i < N; i++) {
            if (!getBit(i, byteArray)) zeroes++;
        }
        return zeroes;
    }

    public boolean getBit(int bit) {
        return getBit(bit,byteArray);
    }

    public void setBit(int bit, boolean value) {
        if(value) setBit(bit,byteArray);
        else unSetBit(bit,byteArray);
    }

    public void setBit(int bit) {
        setBit(bit, true);
    }

    public void clearBit(int bit) {
        setBit(bit, false);
    }

    public void clear() {
        for(int i = 0 ; i < byteArray.length ; i++) byteArray[i] = 0;
        addedElementsCount = 0;
        onesCount = 0;
        zeroesCount = N;
    }

    private static boolean getBit(int i, byte[] bytes) {
        return ((bytes[i/8] & (1<<(i%8))) != 0);
    }

    private static void setBit(int i, byte[] bytes) {
        bytes[i/8] = (byte) (bytes[i/8]  | (1<<(i%8)));
    }

    private static void unSetBit(int i, byte[] bytes) {
        bytes[i/8] = (byte) (bytes[i/8]  & ~(1 << (i%8)));
    }
}
