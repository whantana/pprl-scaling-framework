package gr.upatras.ceid.pprl.encoding;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Bloom Filter class.
 */
public class BloomFilter {

    private static final String SECRET_KEY = "ZIKRETQI";

    private final Mac HMAC_MD5;     // MD5 Mac
    private final Mac HMAC_SHA1;    // SHA1 Mac
    private int N;                  // Length of bloom filter (#bits)
    private int K;                  // Number of hash functions
    private int addedElementsCount; // added elements
    private int onesCount;          // one counts
    private int zeroesCount;        // zeroes counts
    private byte[] byteArray;       // the buffer (byte array)

    /**
     * Constructor.
     *
     * @param N length of bloom filter
     * @param K number of hash values needed for each element to be added.
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    public BloomFilter(final int N, final int K)
            throws NoSuchAlgorithmException, InvalidKeyException {
        this.N = N;
        this.K = K;
        byteArray = new byte[(int) Math.ceil(N/(double)8)];
        onesCount = 0;
        zeroesCount = N;
        addedElementsCount = 0;
        HMAC_MD5 = Mac.getInstance("HmacMD5");
        HMAC_MD5.init(new SecretKeySpec(SECRET_KEY.getBytes(), "HmacMD5"));
        HMAC_SHA1 = Mac.getInstance("HmacSHA1");
        HMAC_SHA1.init(new SecretKeySpec(SECRET_KEY.getBytes(), "HmacSHA1"));
    }

    /**
     * Constructor of instance based on byte array.
     *
     * @param N length of bloom filter
     * @param K number of hash values needed for each element to be added.
     * @param byteArray the bloom filter byte array.
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    public BloomFilter(final int N, final int K, final byte[] byteArray)
            throws NoSuchAlgorithmException, InvalidKeyException {
        assert Math.ceil(N/(double)8) == byteArray.length;
        this.N = N;
        this.K = K;
        this.byteArray = new byte[(int) Math.ceil(N/(double)8)];
        System.arraycopy(byteArray,0,this.byteArray,0,byteArray.length);
        onesCount = countOnes();
        zeroesCount = countZeroes();
        addedElementsCount = (int) Math.ceil(onesCount/(double)K); // an estimate
        HMAC_MD5 = Mac.getInstance("HmacMD5");
        HMAC_MD5.init(new SecretKeySpec(SECRET_KEY.getBytes(), "HmacMD5"));
        HMAC_SHA1 = Mac.getInstance("HmacSHA1");
        HMAC_SHA1.init(new SecretKeySpec(SECRET_KEY.getBytes(), "HmacSHA1"));
    }

    /**
     * Calculates and returns False-Positive probability (BF claims that contains the element
     * , while it hasnt been added).
     *
     * @return False-Positive probability.
     */
    public double calcFPP(){
        return Math.pow((1 - Math.exp(-K * (double) addedElementsCount / (double) N)), K);
    }

    /**
     * Returns the MD5 Mac.
     *
     * @return the MD5 Mac.
     */
    public Mac getHmacMD5() {
        return HMAC_MD5;
    }

    /**
     * Returns the SHA1 Mac.
     *
     * @return the SHA1 Mac.
     */
    public Mac getHmacSHA1() {
        return HMAC_SHA1;
    }

    /**
     * Create and return K hash values an an array of integers. Utilizes the
     * Kirsch, Mitzenmacher "Less Hashing, Same Performance" for getting K
     * hash keys.
     *
     * @param data data byte array.
     * @param N length of bloom fitler.
     * @param K number of hash values needed for each element to be added.
     * @param HMAC_MD5 the MD5 Mac.
     * @param HMAC_SHA1 the SHA1 Mac.
     * @return an array of K integers all in range of [0,...,N).
     */
    public static int[] createHashesV1(final byte[] data, final int N, final int K,
                                       final Mac HMAC_MD5, final Mac HMAC_SHA1) {
        byte[] sha1Digest = HMAC_SHA1.doFinal(data);
        byte[] md5Digest = HMAC_MD5.doFinal(data);
        final BigInteger SHA1 = new BigInteger(sha1Digest);
        final BigInteger MD5 = new BigInteger(md5Digest);
        final BigInteger BIGN = new BigInteger(String.valueOf(N));
        final int[] hashes = new int[K];
        for (int i = 0; i < K; i++) {
            final BigInteger I = new BigInteger(String.valueOf(i+1));
            final BigInteger RES = MD5.multiply(I).add(SHA1).mod(BIGN);
            hashes[i] = Math.abs(RES.intValue());
        }
        return hashes;
    }

    /**
     * Create and return K hash values an an array of integers. Utilizes
     * only the MD5 Mac function and from its 16-bytes retrieves its
     * containing 4 consecutive integers that their modulo-n provides their respected hash values.
     * Then a salt is applied to the mac and the whole process repeats until we reach
     * the K hash value limit.
     *
     * @param data data byte array.
     * @param N length of bloom fitler.
     * @param K number of hash values needed for each element to be added.
     * @param HMAC_MD5 the MD5 Mac.
     * @return an array of K integers all in range of [0,...,N).
     */
    public static int[] createHashesV2(final byte[] data,int N,int K, final Mac HMAC_MD5) {
        int k = 0;
        byte salt = 0;
        final int[] hashes = new int[K];

        while(k < K) {
            byte[] md5Digest;
            HMAC_MD5.update(salt);
            salt++;
            md5Digest = HMAC_MD5.doFinal(data);
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

    /**
     * Add data to the bloom filter.
     *
     * @param data data byte array.
     * @return an array of K integers all in range of [0,...,N).
     */
    public int[] addData(final byte[] data) {
        final int[] positions = createHashesV1(data,N,K,getHmacMD5(),getHmacSHA1());
        //final int[] positions = createHashesV2(data,N,K,getHmacMD5()); //
        setPositions(positions);
        return positions;
    }

    /**
     * Set positions on the bloom-fitler.
     *
     * @param positions an array of K integers all in range of [0,...,N).
     */
    public void setPositions(final int[] positions) {
        for(int position : positions) {
            if(!getBit(position)) {
                onesCount++; zeroesCount--;
                setBit(position);
            }
        }
        addedElementsCount++;
    }

    /**
     * Returns length of bloom filter.
     *
     * @return length of bloom filter.
     */
    public int getN() {
        return N;
    }

    /**
     * Returns the number of hash values.
     *
     * @return the number of hash values.
     */
    public int getK() {
        return K;
    }

    /**
     * Returns the added elemenet count (good estimate).
     *
     * @return the added elemenet count.
     */
    public int getAddedElementsCount() {
        return addedElementsCount;
    }

    /**
     * Returns the number of bits set in the bloom filter.
     *
     * @return the number of bits set in the bloom filter.
     */
    public int getOnesCount() {
        return onesCount;
    }

    /**
     * Returns the number of bits NOT set in the bloom filter.
     *
     * @return the number of bits NOT set in the bloom filter.
     */
    public int getZeroesCount() {
        return zeroesCount;
    }

    /**
     * Returns the underlying byte array.
     *
     * @return the underlying byte array.
     */
    public byte[] getByteArray() {
        return byteArray;
    }

    /**
     * Calculate and return bits per element (good estimate).
     *
     * @return bits per element.
     */
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

    /**
     * Return the Hex representation of this bloom filter in a string.
     *
     * @return the Hex representation of this bloom filter.
     */
    public String toHexString() {
        return toHexString(byteArray);
    }

    /**
     * Return the Hex representation of a byte array in a string.
     *
     * @param bytes input byte array.
     * @return the Hex representation of a byte array.
     */
    public static String toHexString(final byte[] bytes) {
        StringBuilder sb = new StringBuilder("0x");
        for (int i = bytes.length-1; i >= 0 ; i--)
            sb.append(String.format("%02x", bytes[i] & 0xFF));
        return sb.toString();
    }

    /**
     * Returns a set of the bit positions (integers) that are set.
     *
     * @return a set of the bit positions (integers) that are set.
     */
    public Set<Integer> toSetOnes() {
        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < N; i++) {
            if (getBit(i, byteArray)) set.add(i);
        }
        return set;
    }

    /**
     * Returns a set of the bit positions (integers) that are NOT set.
     *
     * @return a set of the bit positions (integers) that are NOT set.
     */
    public Set<Integer> toSetZeroes() {
        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < N; i++) {
            if (!getBit(i, byteArray)) set.add(i);
        }
        return set;
    }

    /**
     * Count and return the number of bits of this bloom filter that are set.
     *
     * @return the number of bits of this bloom filter that are set.
     */
    public int countOnes() {
        int ones = 0;
        for (int i = 0; i < N; i++) {
            if (getBit(i, byteArray)) ones++;
        }
        return ones;
    }

    /**
     * Count and return the number of bits of this bloom filter that are set.
     *
     * @return the number of bits of this bloom filter that are set.
     */
    public int countZeroes() {
        int zeroes = 0;
        for (int i = 0; i < N; i++) {
            if (!getBit(i, byteArray)) zeroes++;
        }
        return zeroes;
    }

    /**
     * Returns true if a bit is set, false otherwise.
     *
     * @param bit position of bit in this bloom filter.
     * @return true if bit is set, false otherwise.
     */
    public boolean getBit(int bit) {
        return getBit(bit,byteArray);
    }

    /**
     * Set a bit to 1 if value is true, Set a bit to 0 if value is false.
     *
     * @param bit position of bit in this bloom filter.
     * @param value true for 1, false for 0.
     */
    public void setBit(int bit, boolean value) {
        if(value) setBit(bit,byteArray);
        else unSetBit(bit,byteArray);
    }

    /**
     * Set a bit to 1.
     *
     * @param bit position of bit in this bloom filter.
     */
    public void setBit(int bit) {
        setBit(bit, true);
    }

    /**
     * Set a bit to 0.
     *
     * @param bit position of bit in this bloom filter.
     */
    public void clearBit(int bit) {
        setBit(bit, false);
    }

    /**
     * Clear this bloom filter. All its bits are set to 0.
     */
    public void clear() {
        for(int i = 0 ; i < byteArray.length ; i++) byteArray[i] = 0;
        addedElementsCount = 0;
        onesCount = 0;
        zeroesCount = N;
    }

    /**
     * Returns true if a bit is set in the bytes array, false otherwise.
     *
     * @param i position of bit in this bloom filter(bytes array).
     * @param bytes a byte array.
     * @return true if ith-bit is set, false otherwise.
     */
    private static boolean getBit(int i, byte[] bytes) {
        return ((bytes[i/8] & (1<<(i%8))) != 0);
    }

    /**
     * Set the i-th bit of the byte array to 1.
     *
     * @param i position of bit in this bloom filter(bytes array).
     * @param bytes a byte array.
     */
    private static void setBit(int i, byte[] bytes) {
        bytes[i/8] = (byte) (bytes[i/8]  | (1<<(i%8)));
    }

    /**
     * Set the i-th bit of the byte array to 0.
     *
     * @param i position of bit in this bloom filter(bytes array).
     * @param bytes a byte array.
     */
    private static void unSetBit(int i, byte[] bytes) {
        bytes[i/8] = (byte) (bytes[i/8]  & ~(1 << (i%8)));
    }
}
